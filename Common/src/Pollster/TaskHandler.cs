// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-2025. All rights reserved.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY, without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Base;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Meter;
using ArmoniK.Core.Common.Pollster.TaskProcessingChecker;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Stream.Worker;
using ArmoniK.Core.Common.Utils;

using Grpc.Core;

using Microsoft.Extensions.Logging;

using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

namespace ArmoniK.Core.Common.Pollster;

public sealed class TaskHandler : IAsyncDisposable
{
  private readonly Activity?                             activity_;
  private readonly ActivityContext                       activityContext_;
  private readonly ActivitySource                        activitySource_;
  private readonly IAgentHandler                         agentHandler_;
  private readonly DataPrefetcher                        dataPrefetcher_;
  private readonly TimeSpan                              delayBeforeAcquisition_;
  private readonly CancellationTokenSource               earlyCts_;
  private readonly string                                folder_;
  private readonly FunctionExecutionMetrics<TaskHandler> functionExecutionMetrics_;
  private readonly CancellationTokenSource               lateCts_;
  private readonly ILogger                               logger_;
  private readonly IQueueMessageHandler                  messageHandler_;
  private readonly string                                ownerPodId_;
  private readonly string                                ownerPodName_;
  private readonly IResultTable                          resultTable_;
  private readonly ISessionTable                         sessionTable_;
  private readonly ISubmitter                            submitter_;
  private readonly ITaskProcessingChecker                taskProcessingChecker_;
  private readonly ITaskTable                            taskTable_;
  private readonly string                                token_;
  private readonly IWorkerStreamHandler                  workerStreamHandler_;
  private          IAgent?                               agent_;
  private          DateTime?                             fetchedDate_;
  private          Action?                               onDispose_;
  private          Output?                               output_;
  private          SessionData?                          sessionData_;
  private          TaskData?                             taskData_;

  public TaskHandler(ISessionTable                         sessionTable,
                     ITaskTable                            taskTable,
                     IResultTable                          resultTable,
                     ISubmitter                            submitter,
                     DataPrefetcher                        dataPrefetcher,
                     IWorkerStreamHandler                  workerStreamHandler,
                     IQueueMessageHandler                  messageHandler,
                     ITaskProcessingChecker                taskProcessingChecker,
                     string                                ownerPodId,
                     string                                ownerPodName,
                     ActivitySource                        activitySource,
                     IAgentHandler                         agentHandler,
                     ILogger                               logger,
                     Injection.Options.Pollster            pollsterOptions,
                     Action                                onDispose,
                     ExceptionManager                      exceptionManager,
                     FunctionExecutionMetrics<TaskHandler> functionExecutionMetrics)
  {
    sessionTable_             = sessionTable;
    taskTable_                = taskTable;
    resultTable_              = resultTable;
    messageHandler_           = messageHandler;
    taskProcessingChecker_    = taskProcessingChecker;
    submitter_                = submitter;
    dataPrefetcher_           = dataPrefetcher;
    workerStreamHandler_      = workerStreamHandler;
    activitySource_           = activitySource;
    agentHandler_             = agentHandler;
    logger_                   = logger;
    onDispose_                = onDispose;
    functionExecutionMetrics_ = functionExecutionMetrics;
    ownerPodId_               = ownerPodId;
    ownerPodName_             = ownerPodName;
    taskData_                 = null;
    sessionData_              = null;

    activity_ = activitySource.CreateActivity($"{nameof(TaskHandler)}",
                                              ActivityKind.Internal);
    activityContext_ = activity_?.Context ?? new ActivityContext();
    activity_?.Start();
    activity_?.Stop();
    activity_?.SetTagAndBaggage("TaskId",
                                messageHandler_.TaskId);
    activity_?.SetTagAndBaggage("MessageId",
                                messageHandler_.MessageId);
    activity_?.SetTagAndBaggage("OwnerPodId",
                                ownerPodId);
    activity_?.SetTagAndBaggage("OwnerPodName",
                                ownerPodName);

    token_ = Guid.NewGuid()
                 .ToString();
    folder_ = Path.Combine(pollsterOptions.SharedCacheFolder,
                           token_);
    Directory.CreateDirectory(folder_);
    delayBeforeAcquisition_ = pollsterOptions.TimeoutBeforeNextAcquisition + TimeSpan.FromSeconds(2);

    earlyCts_ = CancellationTokenSource.CreateLinkedTokenSource(exceptionManager.EarlyCancellationToken);
    lateCts_  = CancellationTokenSource.CreateLinkedTokenSource(exceptionManager.LateCancellationToken);
  }

  /// <summary>
  ///   Start date for the task handled by the current Task Handler
  /// </summary>
  public DateTime StartedAt { get; } = DateTime.UtcNow;


  /// <inheritdoc />
  public async ValueTask DisposeAsync()
  {
    using var measure = functionExecutionMetrics_.CountAndTime();
    using var activity = activitySource_.StartActivityFromParent(activityContext_,
                                                                 activity_);
    using var _ = logger_.BeginNamedScope("DisposeAsync",
                                          ("taskId", messageHandler_.TaskId),
                                          ("messageHandler", messageHandler_.MessageId),
                                          ("sessionId", taskData_?.SessionId ?? ""));

    await ReleaseTaskHandler()
      .ConfigureAwait(false);

    agent_?.Dispose();
    try
    {
      Directory.Delete(folder_,
                       true);
    }
    catch (DirectoryNotFoundException)
    {
    }

    activity_?.Dispose();
    lateCts_.Dispose();
    earlyCts_.Dispose();
  }

  /// <summary>
  ///   Release the TaskHandler from the Pollster.
  ///   Call onDispose and dispose the messageHandler.
  /// </summary>
  private async ValueTask ReleaseTaskHandler()
  {
    using var measure = functionExecutionMetrics_.CountAndTime();
    var onDispose = Interlocked.Exchange(ref onDispose_,
                                         null);
    onDispose?.Invoke();

    logger_.LogDebug("MessageHandler status is {Status}",
                     messageHandler_.Status);
    await messageHandler_.DisposeIgnoreErrorAsync(logger_)
                         .ConfigureAwait(false);
  }

  /// <summary>
  ///   Refresh task metadata and stop execution if current task should be cancelled
  /// </summary>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  public async Task<bool> StopCancelledTask()
  {
    using var measure = functionExecutionMetrics_.CountAndTime();
    using var activity = activitySource_.StartActivityFromParent(activityContext_,
                                                                 activity_);

    using var _ = logger_.BeginNamedScope("StopCancelledTask",
                                          ("taskId", messageHandler_.TaskId),
                                          ("messageHandler", messageHandler_.MessageId),
                                          ("sessionId", taskData_?.SessionId ?? ""));

    if (taskData_?.Status is not null or TaskStatus.Cancelled or TaskStatus.Cancelling)
    {
      taskData_ = await taskTable_.ReadTaskAsync(messageHandler_.TaskId,
                                                 CancellationToken.None)
                                  .ConfigureAwait(false);
      if (taskData_.Status is TaskStatus.Cancelling)
      {
        logger_.LogWarning("Task has been cancelled, trigger cancellation from exterior.");
        await earlyCts_.CancelAsync()
                       .ConfigureAwait(false);
        await lateCts_.CancelAsync()
                      .ConfigureAwait(false);

        // Upon cancellation, dispose the messageHandler to remove the message from the queue, and call onDispose
        // Calling the TaskHandler dispose is not possible here as the cancellationTokenSource is still in use
        messageHandler_.Status = QueueMessageStatus.Cancelled;
        await ReleaseTaskHandler()
          .ConfigureAwait(false);

        return true;
      }
    }

    return false;
  }

  /// <summary>
  ///   Acquisition of the task in the message given to the constructor
  /// </summary>
  /// <returns>
  ///   Integer representing whether the task has been acquired
  ///   Acquired when return is 0
  /// </returns>
  /// <exception cref="ArgumentException">status of the task is not recognized</exception>
  public async Task<AcquisitionStatus> AcquireTask()
  {
    using var measure = functionExecutionMetrics_.CountAndTime();
    using var activity = activitySource_.StartActivityFromParent(activityContext_,
                                                                 activity_);
    using var _ = logger_.BeginNamedScope("Acquiring task",
                                          ("messageHandler", messageHandler_.MessageId),
                                          ("taskId", messageHandler_.TaskId));

    try
    {
      taskData_ = await taskTable_.ReadTaskAsync(messageHandler_.TaskId,
                                                 CancellationToken.None)
                                  .ConfigureAwait(false);

      activity_?.SetTagAndBaggage("SessionId",
                                  taskData_.SessionId);
      activity?.SetTagAndBaggage("SessionId",
                                 taskData_.SessionId);
      using var sessionScope = logger_.BeginPropertyScope(("sessionId", taskData_.SessionId));
      logger_.LogInformation("Start task acquisition with {status}",
                             taskData_.Status);

      if (earlyCts_.IsCancellationRequested)
      {
        messageHandler_.Status = QueueMessageStatus.Postponed;
        logger_.LogDebug("Task data read but execution cancellation requested");
        return AcquisitionStatus.CancelledAfterFirstRead;
      }

      /*
       * Check preconditions:
       *  - Session is not cancelled
       *  - Task is not cancelled
       *  - Task status is OK
       *  - Dependencies have been checked
       *  - Max number of retries has not been reached
       */

      logger_.LogDebug("Handling the task status ({status})",
                       taskData_.Status);

      sessionData_ = await sessionTable_.GetSessionAsync(taskData_.SessionId,
                                                         CancellationToken.None)
                                        .ConfigureAwait(false);
      switch (taskData_.Status)
      {
        case TaskStatus.Cancelling:
          logger_.LogInformation("Task is being cancelled");
          messageHandler_.Status = QueueMessageStatus.Cancelled;
          taskData_ = taskData_ with
                      {
                        EndDate = DateTime.UtcNow,
                        CreationToEndDuration = DateTime.UtcNow - taskData_.CreationDate,
                      };
          await taskTable_.SetTaskCanceledAsync(taskData_,
                                                CancellationToken.None)
                          .ConfigureAwait(false);
          await ResultLifeCycleHelper.AbortTasksAndResults(taskTable_,
                                                           resultTable_,
                                                           new[]
                                                           {
                                                             messageHandler_.TaskId,
                                                           },
                                                           $"Task {messageHandler_.TaskId} has been cancelled",
                                                           CancellationToken.None)
                                     .ConfigureAwait(false);

          // Propagate cancelled status to TaskHandler
          taskData_ = taskData_ with
                      {
                        Status = TaskStatus.Cancelled,
                      };

          return AcquisitionStatus.TaskIsCancelling;
        case TaskStatus.Completed:
          logger_.LogInformation("Task was already completed");
          messageHandler_.Status = QueueMessageStatus.Processed;
          return AcquisitionStatus.TaskIsProcessed;
        case TaskStatus.Paused:
          logger_.LogInformation("Task was paused; remove it from queue");
          messageHandler_.Status = QueueMessageStatus.Processed;
          return AcquisitionStatus.TaskIsPaused;
        case TaskStatus.Creating:
          logger_.LogInformation("Task is still creating");
          messageHandler_.Status = QueueMessageStatus.Postponed;
          return AcquisitionStatus.TaskIsCreating;
        case TaskStatus.Pending:
          logger_.LogInformation("Task is still missing dependencies; task is postponed");
          messageHandler_.Status = QueueMessageStatus.Postponed;
          return AcquisitionStatus.TaskIsPending;
        case TaskStatus.Submitted:
          break;
        case TaskStatus.Dispatched:
          break;
        case TaskStatus.Error:
          logger_.LogInformation("Task was on error elsewhere ; task should have been resubmitted");
          messageHandler_.Status = QueueMessageStatus.Cancelled;
          await ResultLifeCycleHelper.AbortTasksAndResults(taskTable_,
                                                           resultTable_,
                                                           new[]
                                                           {
                                                             messageHandler_.TaskId,
                                                           },
                                                           $"Task {messageHandler_.TaskId} was on error",
                                                           CancellationToken.None)
                                     .ConfigureAwait(false);
          return AcquisitionStatus.TaskIsError;
        case TaskStatus.Timeout:
          logger_.LogInformation("Task was timeout elsewhere ; taking over here");
          messageHandler_.Status = QueueMessageStatus.Poisonous;
          break;
        case TaskStatus.Cancelled:
          logger_.LogInformation("Task has been cancelled");
          messageHandler_.Status = QueueMessageStatus.Cancelled;
          await ResultLifeCycleHelper.AbortTasksAndResults(taskTable_,
                                                           resultTable_,
                                                           new[]
                                                           {
                                                             messageHandler_.TaskId,
                                                           },
                                                           $"Task {messageHandler_.TaskId} was cancelled",
                                                           CancellationToken.None)
                                     .ConfigureAwait(false);
          return AcquisitionStatus.TaskIsCancelled;
        case TaskStatus.Processing:

          // we check if the task was acquired by this pod

          // if the task is acquired by another pod, we check if the task is running on the other pod
          var taskProcessingElsewhere = await taskProcessingChecker_.Check(taskData_.TaskId,
                                                                            taskData_.OwnerPodId,
                                                                            CancellationToken.None)
                                                                    .ConfigureAwait(false);

          logger_.LogInformation("Task {taskId} already acquired by {OtherOwnerPodId} {otherOwnerPodName},, treating it {processing}",
                                  taskData_.TaskId,
                                  taskData_.OwnerPodId,
                                  taskData_.OwnerPodName,
                                  taskProcessingElsewhere);


          taskData_ = await taskTable_.ReadTaskAsync(messageHandler_.TaskId,
                                                        CancellationToken.None)
                                        .ConfigureAwait(false);
          logger_.LogInformation("Task is not running on the other polling agent, status : {status}",
                                    taskData_.Status);

          logger_.LogInformation("[Temporary] Task {taskId} already acquired by {OtherOwnerPodId} {otherOwnerPodName},, treating status {status}",
                                  taskData_.TaskId,
                                  taskData_.OwnerPodId,
                                  taskData_.OwnerPodName,
                                  taskData_.Status);

            if (taskData_.Status is TaskStatus.Submitted)
            {
              logger_.LogInformation("Task {task} was being processed on another pod, but has been released during acquirement",
                                      taskData_.TaskId);
              messageHandler_.Status = QueueMessageStatus.Postponed;
              // TODO: AcquistionStatus must be tested
              return AcquisitionStatus.TaskSubmittedButPreviouslyProcessing;
            }

            logger_.LogDebug("Resubmitting task {task} on another pod",
                              taskData_.TaskId);
            await submitter_.CompleteTaskAsync(taskData_,
                                                sessionData_,
                                                true,
                                                new Output(OutputStatus.Error,
                                                          "Other pod seems to have crashed, resubmitting task"),
                                                CancellationToken.None)
                            .ConfigureAwait(false);

            // Propagate retried status to TaskHandler
            taskData_ = taskData_ with
                        {
                          Status = taskData_.RetryOfIds.Count < taskData_.Options.MaxRetries
                                      ? TaskStatus.Retried
                                      : TaskStatus.Error,
                        };

            messageHandler_.Status = QueueMessageStatus.Processed;
            return AcquisitionStatus.TaskIsProcessingButSeemsCrashed;

          logger_.LogWarning("Task already in processing on this pod. This scenario should be managed earlier. Message likely duplicated. Removing it from queue");
          messageHandler_.Status = QueueMessageStatus.Processed;
          return AcquisitionStatus.TaskIsProcessingHere;
        case TaskStatus.Retried:
          logger_.LogInformation("Task is in retry ; retry task should be executed");
          messageHandler_.Status = QueueMessageStatus.Poisonous;
          var retryId = taskData_.RetryId();

          TaskData retryData;
          var      taskNotFound = false;
          var      taskExists   = false;
          try
          {
            retryData = await taskTable_.ReadTaskAsync(retryId,
                                                       lateCts_.Token)
                                        .ConfigureAwait(false);
          }
          catch (TaskNotFoundException)
          {
            logger_.LogWarning("Retried task {task} was not found in the database; resubmit it",
                               retryId);
            taskNotFound = true;
            try
            {
              await taskTable_.RetryTask(taskData_,
                                         CancellationToken.None)
                              .ConfigureAwait(false);
            }
            catch (TaskAlreadyExistsException)
            {
              logger_.LogWarning("Retried task {task} already exists; finalize creation if needed",
                                 retryId);
              taskExists = true;
            }

            retryData = await taskTable_.ReadTaskAsync(retryId,
                                                       CancellationToken.None)
                                        .ConfigureAwait(false);
          }

          if (retryData.Status is TaskStatus.Creating or TaskStatus.Pending or TaskStatus.Submitted)
          {
            logger_.LogWarning("Retried task {task} is in {status}; trying to finalize task creation",
                               retryId,
                               retryData.Status);
            await submitter_.FinalizeTaskCreation(new List<TaskCreationRequest>
                                                  {
                                                    new(retryId,
                                                        retryData.PayloadId,
                                                        retryData.Options,
                                                        retryData.ExpectedOutputIds,
                                                        retryData.DataDependencies),
                                                  },
                                                  sessionData_,
                                                  taskData_.TaskId,
                                                  CancellationToken.None)
                            .ConfigureAwait(false);
          }
          else
          {
            logger_.LogInformation("Retried task {task} is in {status}; nothing done",
                                   retryId,
                                   retryData.Status);
          }

          return (taskNotFound, taskExists, retryData.Status) switch
                 {
                   (false, false, TaskStatus.Submitted)                       => AcquisitionStatus.TaskIsRetriedAndRetryIsSubmitted,
                   (false, false, TaskStatus.Creating)                        => AcquisitionStatus.TaskIsRetriedAndRetryIsCreating,
                   (false, false, TaskStatus.Pending)                         => AcquisitionStatus.TaskIsRetriedAndRetryIsPending,
                   (true, false, TaskStatus.Submitted or TaskStatus.Creating) => AcquisitionStatus.TaskIsRetriedAndRetryIsNotFound,
                   _                                                          => AcquisitionStatus.TaskIsRetried,
                 };

        case TaskStatus.Unspecified:
        default:
          logger_.LogCritical("Task was in an unknown state {state}",
                              taskData_.Status);
          throw new ArgumentException(nameof(taskData_));
      }

      if (sessionData_.Status is SessionStatus.Cancelled or SessionStatus.Deleted or SessionStatus.Closed or SessionStatus.Purged &&
          taskData_.Status is not (TaskStatus.Cancelled or TaskStatus.Completed or TaskStatus.Error))
      {
        logger_.LogInformation("Task is being cancelled because its session is {sessionStatus}",
                               sessionData_.Status);

        messageHandler_.Status = QueueMessageStatus.Cancelled;
        taskData_ = taskData_ with
                    {
                      EndDate = DateTime.UtcNow,
                      CreationToEndDuration = DateTime.UtcNow - taskData_.CreationDate,
                    };
        await taskTable_.SetTaskCanceledAsync(taskData_,
                                              CancellationToken.None)
                        .ConfigureAwait(false);

        await ResultLifeCycleHelper.AbortTasksAndResults(taskTable_,
                                                         resultTable_,
                                                         new[]
                                                         {
                                                           messageHandler_.TaskId,
                                                         },
                                                         $"Task {messageHandler_.TaskId} has been cancelled because its session {taskData_.SessionId} is {sessionData_.Status}",
                                                         CancellationToken.None)
                                   .ConfigureAwait(false);

        // Propagate cancelled status to TaskHandler
        taskData_ = taskData_ with
                    {
                      Status = TaskStatus.Cancelled,
                    };

        return AcquisitionStatus.SessionNotExecutable;
      }

      if (sessionData_.Status == SessionStatus.Paused)
      {
        logger_.LogDebug("Session paused; message deleted");
        messageHandler_.Status = QueueMessageStatus.Processed;
        return AcquisitionStatus.SessionPaused;
      }

      if (earlyCts_.IsCancellationRequested)
      {
        messageHandler_.Status = QueueMessageStatus.Postponed;
        logger_.LogDebug("Session running but execution cancellation requested");
        return AcquisitionStatus.CancelledAfterSessionAccess;
      }

      taskData_ = taskData_ with
                  {
                    OwnerPodId = ownerPodId_,
                    OwnerPodName = ownerPodName_,
                    AcquisitionDate = DateTime.UtcNow,
                    ReceptionDate = messageHandler_.ReceptionDateTime,
                  };
      taskData_ = await taskTable_.AcquireTask(taskData_,
                                               CancellationToken.None)
                                  .ConfigureAwait(false);

      if (earlyCts_.IsCancellationRequested)
      {
        logger_.LogDebug("Task acquired but execution cancellation requested");
        await ReleaseAndPostponeTask()
          .ConfigureAwait(false);
        return AcquisitionStatus.CancelledAfterAcquisition;
      }

      // empty OwnerPodId means that the task was not acquired because not ready
      if (taskData_.OwnerPodId == "")
      {
        logger_.LogDebug("Task acquired but not ready (empty owner pod id)");
        messageHandler_.Status = QueueMessageStatus.Postponed;
        return AcquisitionStatus.PodIdEmptyAfterAcquisition;
      }

      // we check if the task was acquired by this pod
      if (taskData_.OwnerPodId != ownerPodId_)
      {
        // if the task is acquired by another pod, we check if the task is running on the other pod
        var taskProcessingElsewhere = await taskProcessingChecker_.Check(taskData_.TaskId,
                                                                         taskData_.OwnerPodId,
                                                                         CancellationToken.None)
                                                                  .ConfigureAwait(false);

        logger_.LogInformation("Task {taskId} already acquired by {OtherOwnerPodId}, treating it {processing}",
                               taskData_.TaskId,
                               taskData_.OwnerPodId,
                               taskProcessingElsewhere);

        // if the task is not running on the other pod, we resubmit the task in the queue
        if (!taskProcessingElsewhere)
        {
          taskData_ = await taskTable_.ReadTaskAsync(messageHandler_.TaskId,
                                                     CancellationToken.None)
                                      .ConfigureAwait(false);
          logger_.LogInformation("Task is not running on the other polling agent, status : {status}",
                                 taskData_.Status);

          if (taskData_.Status is TaskStatus.Submitted)
          {
            logger_.LogInformation("Task {task} was dispatched on another pod who crashed, but has been released during acquirement",
                                   taskData_.TaskId);
            messageHandler_.Status = QueueMessageStatus.Postponed;
            // TODO: AcquistionStatus TaskSubmittedButPreviouslyDispatched must be tested
            return AcquisitionStatus.TaskSubmittedButPreviouslyDispatched;
          }

          if (taskData_.Status is TaskStatus.Dispatched && taskData_.AcquisitionDate + delayBeforeAcquisition_ > DateTime.UtcNow)
          {
            messageHandler_.Status = QueueMessageStatus.Postponed;
            logger_.LogDebug("Wait to exceed acquisition timeout before resubmitting task");
            return AcquisitionStatus.AcquisitionFailedTimeoutNotExceeded;
          }

          if (taskData_.Status is TaskStatus.Processing or TaskStatus.Dispatched or TaskStatus.Processed)
          {
            logger_.LogDebug("Resubmitting task {task} on another pod",
                             taskData_.TaskId);
            await submitter_.CompleteTaskAsync(taskData_,
                                               sessionData_,
                                               true,
                                               new Output(OutputStatus.Error,
                                                          "Other pod seems to have crashed, resubmitting task"),
                                               CancellationToken.None)
                            .ConfigureAwait(false);

            // Propagate retried status to TaskHandler
            taskData_ = taskData_ with
                        {
                          Status = taskData_.RetryOfIds.Count < taskData_.Options.MaxRetries
                                     ? TaskStatus.Retried
                                     : TaskStatus.Error,
                        };
          }


          if (taskData_.Status is TaskStatus.Cancelling)
          {
            messageHandler_.Status = QueueMessageStatus.Cancelled;
            taskData_ = taskData_ with
                        {
                          EndDate = DateTime.UtcNow,
                          CreationToEndDuration = DateTime.UtcNow - taskData_.CreationDate,
                        };
            await taskTable_.SetTaskCanceledAsync(taskData_,
                                                  CancellationToken.None)
                            .ConfigureAwait(false);
            await ResultLifeCycleHelper.AbortTasksAndResults(taskTable_,
                                                             resultTable_,
                                                             new[]
                                                             {
                                                               messageHandler_.TaskId,
                                                             },
                                                             $"Task {messageHandler_.TaskId} has been cancelled while acquired on another pod",
                                                             CancellationToken.None)
                                       .ConfigureAwait(false);
            return AcquisitionStatus.AcquisitionFailedTaskCancelling;
          }
        }

        // if the task is running elsewhere, then the message is duplicated so we remove it from the queue
        // and do not acquire the task
        messageHandler_.Status = QueueMessageStatus.Processed;
        return AcquisitionStatus.AcquisitionFailedMessageDuplicated;
      }

      if (taskData_.OwnerPodId == ownerPodId_ && taskData_.Status != TaskStatus.Dispatched)
      {
        logger_.LogInformation("Task is already managed by this agent; message likely to be duplicated");
        messageHandler_.Status = QueueMessageStatus.Processed;
        return AcquisitionStatus.AcquisitionFailedProcessingHere;
      }

      if (earlyCts_.IsCancellationRequested)
      {
        logger_.LogDebug("Task preconditions ok but execution cancellation requested");
        await ReleaseAndPostponeTask()
          .ConfigureAwait(false);
        return AcquisitionStatus.CancelledAfterPreconditions;
      }

      logger_.LogDebug("Task preconditions are OK");
      return AcquisitionStatus.Acquired;
    }
    catch (TaskNotFoundException e)
    {
      logger_.LogWarning(e,
                         "TaskId coming from message queue was not found, delete message from queue");
      messageHandler_.Status = QueueMessageStatus.Processed;
      return AcquisitionStatus.TaskNotFound;
    }
  }

  /// <summary>
  ///   Get the meta data of the acquired task
  /// </summary>
  /// <returns>
  ///   The metadata of the task
  /// </returns>
  public TaskInfo GetAcquiredTaskInfo()
    => taskData_ is not null
         ? new TaskInfo(taskData_.SessionId,
                        taskData_.TaskId,
                        messageHandler_.MessageId,
                        taskData_.Status)
         : throw new ArmoniKException("TaskData should not be null after successful acquisition");

  /// <summary>
  ///   Release task from the current agent and set message to <see cref="QueueMessageStatus.Postponed" />
  /// </summary>
  /// <param name="paused">If task should be paused</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  /// <exception cref="NullReferenceException">wrong order of execution</exception>
  public async Task ReleaseAndPostponeTask(bool paused = false)
  {
    if (taskData_ is null)
    {
      throw new NullReferenceException();
    }

    await taskTable_.ReleaseTask(taskData_,
                                 paused,
                                 CancellationToken.None)
                    .ConfigureAwait(false);

    // Propagate submitted status to TaskHandler
    taskData_ = taskData_ with
                {
                  Status = paused
                             ? TaskStatus.Paused
                             : TaskStatus.Submitted,
                };

    messageHandler_.Status = paused
                               ? QueueMessageStatus.Processed
                               : QueueMessageStatus.Postponed;
  }

  /// <summary>
  ///   Preprocessing (including the data prefetching) of the acquired task
  /// </summary>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  /// <exception cref="NullReferenceException">wrong order of execution</exception>
  /// <exception cref="ObjectDataNotFoundException">input data are not found</exception>
  public async Task PreProcessing()
  {
    using var measure = functionExecutionMetrics_.CountAndTime();
    if (taskData_ is null)
    {
      throw new NullReferenceException();
    }

    using var activity = activitySource_.StartActivityFromParent(activityContext_,
                                                                 activity_);
    using var _ = logger_.BeginNamedScope("PreProcessing",
                                          ("messageHandler", messageHandler_.MessageId),
                                          ("taskId", messageHandler_.TaskId),
                                          ("sessionId", taskData_.SessionId));
    logger_.LogDebug("Start prefetch data");

    try
    {
      await dataPrefetcher_.PrefetchDataAsync(taskData_,
                                              folder_,
                                              earlyCts_.Token)
                           .ConfigureAwait(false);
      fetchedDate_ = DateTime.UtcNow;
    }
    catch (Exception e)
    {
      await HandleErrorRequeueAsync(e,
                                    taskData_,
                                    earlyCts_.Token)
        .ConfigureAwait(false);
    }
  }

  /// <summary>
  ///   Execution of the acquired task on the worker
  /// </summary>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  /// <exception cref="NullReferenceException">wrong order of execution</exception>
  /// <exception cref="ArmoniKException">worker pipe is not initialized</exception>
  public async Task ExecuteTask()
  {
    using var measure = functionExecutionMetrics_.CountAndTime();
    if (taskData_ is null || sessionData_ is null)
    {
      throw new NullReferenceException();
    }

    using var activity = activitySource_.StartActivityFromParent(activityContext_,
                                                                 activity_);
    using var _ = logger_.BeginNamedScope("TaskExecution",
                                          ("messageHandler", messageHandler_.MessageId),
                                          ("taskId", messageHandler_.TaskId),
                                          ("sessionId", taskData_.SessionId));

    try
    {
      logger_.LogDebug("Create agent server to receive requests from worker");

      activity?.AddEvent(new ActivityEvent("Start Handler"));
      // In theory we could create the server during dependencies checking and activate it only now

      agent_ = await agentHandler_.Start(token_,
                                         logger_,
                                         sessionData_,
                                         taskData_,
                                         folder_,
                                         earlyCts_.Token)
                                  .ConfigureAwait(false);


      activity?.AddEvent(new ActivityEvent("Start status update"));
      logger_.LogInformation("Start executing task");
      taskData_ = taskData_ with
                  {
                    StartDate = DateTime.UtcNow,
                    PodTtl = DateTime.UtcNow,
                    FetchedDate = fetchedDate_,
                  };
      // Status update should not be cancelled
      // Task will be marked as processing then start
      await taskTable_.StartTask(taskData_,
                                 CancellationToken.None)
                      .ConfigureAwait(false);
    }
    catch (TaskPausedException)
    {
      logger_.LogDebug("Task was paused and will be removed from queue");
      await ReleaseAndPostponeTask(true)
        .ConfigureAwait(false);
      throw;
    }
    // If an ArmoniKException is thrown, check if this was because
    // the task has been canceled. Use standard error management otherwise.
    catch (ArmoniKException e)
    {
      if (await StopCancelledTask()
            .ConfigureAwait(false))
      {
        earlyCts_.Token.ThrowIfCancellationRequested();
      }
      else
      {
        await HandleErrorRequeueAsync(e,
                                      taskData_,
                                      earlyCts_.Token)
          .ConfigureAwait(false);
      }
    }
    catch (Exception e)
    {
      await HandleErrorRequeueAsync(e,
                                    taskData_,
                                    earlyCts_.Token)
        .ConfigureAwait(false);
    }

    try
    {
      activity?.AddEvent(new ActivityEvent("Start request"));
      logger_.LogDebug("Send request to worker");
      // ReSharper disable once ExplicitCallerInfoArgument
      using (functionExecutionMetrics_.CountAndTime("RequestExecution"))
      {
        output_ = await workerStreamHandler_.StartTaskProcessing(taskData_,
                                                                 token_,
                                                                 folder_,
                                                                 lateCts_.Token)
                                            .ConfigureAwait(false);
      }

      // at this point worker requests should have ended
      taskData_ = taskData_ with
                  {
                    ProcessedDate = DateTime.UtcNow,
                  };
      activity?.AddEvent(new ActivityEvent("End request"));

      logger_.LogDebug("Stop agent server");
      await agentHandler_.Stop(lateCts_.Token)
                         .ConfigureAwait(false);
      activity?.AddEvent(new ActivityEvent("Stopped Handler"));
    }
    catch (Exception e)
    {
      taskData_ = taskData_ with
                  {
                    ProcessedDate = DateTime.UtcNow,
                  };
      await HandleErrorTaskExecutionAsync(e,
                                          taskData_,
                                          earlyCts_.Token)
        .ConfigureAwait(false);
    }
  }

  /// <summary>
  ///   Post processing of the acquired task
  /// </summary>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  /// <exception cref="NullReferenceException">wrong order of execution</exception>
  public async Task PostProcessing()
  {
    using var measure = functionExecutionMetrics_.CountAndTime();
    using var activity = activitySource_.StartActivityFromParent(activityContext_,
                                                                 activity_);
    if (taskData_ is null)
    {
      throw new NullReferenceException(nameof(taskData_) + " is null.");
    }

    if (sessionData_ is null)
    {
      throw new NullReferenceException(nameof(sessionData_) + " is null.");
    }

    if (agent_ is null)
    {
      throw new NullReferenceException(nameof(agent_) + " is null.");
    }

    if (output_ is null)
    {
      throw new NullReferenceException(nameof(output_) + " is null.");
    }

    using var _ = logger_.BeginNamedScope("PostProcessing",
                                          ("messageHandler", messageHandler_.MessageId),
                                          ("taskId", messageHandler_.TaskId),
                                          ("sessionId", taskData_.SessionId));

    try
    {
      logger_.LogInformation("Process task output is {type}",
                             output_.Status);

      if (output_.Status == OutputStatus.Success)
      {
        logger_.LogDebug("Complete processing of the request");
        await agent_.FinalizeTaskCreation(CancellationToken.None)
                    .ConfigureAwait(false);
      }

      await submitter_.CompleteTaskAsync(taskData_,
                                         sessionData_,
                                         false,
                                         output_,
                                         CancellationToken.None)
                      .ConfigureAwait(false);
      messageHandler_.Status = QueueMessageStatus.Processed;
    }
    catch (Exception e)
    {
      await HandleErrorResubmitAsync(e,
                                     taskData_,
                                     earlyCts_.Token)
        .ConfigureAwait(false);
    }

    logger_.LogDebug("End Task Processing");
  }

  private async Task HandleErrorRequeueAsync(Exception         e,
                                             TaskData          taskData,
                                             CancellationToken cancellationToken)
    => await HandleErrorInternalAsync(e,
                                      taskData,
                                      false,
                                      true,
                                      cancellationToken)
         .ConfigureAwait(false);

  private async Task HandleErrorTaskExecutionAsync(Exception         e,
                                                   TaskData          taskData,
                                                   CancellationToken cancellationToken)
    => await HandleErrorInternalAsync(e,
                                      taskData,
                                      true,
                                      true,
                                      cancellationToken)
         .ConfigureAwait(false);

  private async Task HandleErrorResubmitAsync(Exception         e,
                                              TaskData          taskData,
                                              CancellationToken cancellationToken)
    => await HandleErrorInternalAsync(e,
                                      taskData,
                                      true,
                                      false,
                                      cancellationToken)
         .ConfigureAwait(false);

  private async Task HandleErrorInternalAsync(Exception         e,
                                              TaskData          taskData,
                                              bool              resubmit,
                                              bool              requeueIfUnavailable,
                                              CancellationToken cancellationToken)
  {
    using var measure = functionExecutionMetrics_.CountAndTime();

    if (sessionData_ is null)
    {
      throw new NullReferenceException(nameof(sessionData_) + " is null.");
    }

    if (taskData.Status is TaskStatus.Cancelled or TaskStatus.Cancelling)
    {
      messageHandler_.Status = QueueMessageStatus.Processed;
    }
    else
    {
      var connectionEnded = e is RpcException
                                 {
                                   StatusCode: StatusCode.Unavailable,
                                   InnerException: HttpRequestException
                                                   {
                                                     InnerException: HttpIOException
                                                                     {
                                                                       HttpRequestError: HttpRequestError.ResponseEnded,
                                                                     },
                                                   },
                                 };

      var isWorkerDown = e is RpcException re && IsStatusFatal(re.StatusCode);

      // worker crash during cancellation should be treated as error meaning an explicit retry
      // cancellation and worker down should be treated as implicit retry
      if (!connectionEnded && (cancellationToken.IsCancellationRequested || (requeueIfUnavailable && isWorkerDown)))
      {
        if (cancellationToken.IsCancellationRequested)
        {
          logger_.LogWarning(e,
                             "Cancellation triggered, task cancelled here and re executed elsewhere");
        }
        else
        {
          logger_.LogWarning(e,
                             "Worker not available, task cancelled here and re executed elsewhere");
        }

        await ReleaseAndPostponeTask()
          .ConfigureAwait(false);
      }
      else
      {
        logger_.LogError(e,
                         "Error during task execution: {Decision}",
                         resubmit
                           ? "retrying task"
                           : "cancelling task");

        await submitter_.CompleteTaskAsync(taskData,
                                           sessionData_,
                                           resubmit,
                                           new Output(OutputStatus.Error,
                                                      isWorkerDown
                                                        ? $"Worker associated to scheduling agent {ownerPodName_} is down with error: \n{e.Message}"
                                                        : e.Message),
                                           CancellationToken.None)
                        .ConfigureAwait(false);


        messageHandler_.Status = resubmit
                                   ? QueueMessageStatus.Cancelled
                                   : QueueMessageStatus.Processed;
      }
    }

    // Rethrow enable the recording of the error by the Pollster Main loop
    // Keep the stack trace for the rethrown exception
    ExceptionDispatchInfo.Capture(e)
                         .Throw();
  }

  internal static bool IsStatusFatal(StatusCode statusCode)
    => statusCode is StatusCode.Aborted or StatusCode.Cancelled or StatusCode.Unavailable or StatusCode.Unknown;
}
