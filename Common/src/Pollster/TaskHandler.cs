// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Base.Exceptions;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Meter;
using ArmoniK.Core.Common.Pollster.TaskProcessingChecker;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Stream.Worker;
using ArmoniK.Core.Common.Utils;
using ArmoniK.Utils;

using Grpc.Core;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using Submitter = ArmoniK.Core.Common.Injection.Options.Submitter;
using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

namespace ArmoniK.Core.Common.Pollster;

/// <summary>
///   Handles the lifecycle and execution of a single task in the ArmoniK Pollster, including acquisition, preprocessing,
///   execution, postprocessing, and error handling.
///   Manages task and session metadata, agent communication, health checks, and resource cleanup for robust distributed
///   task processing.
/// </summary>
public sealed class TaskHandler : IAsyncDisposable
{
  private readonly Activity?                             activity_;
  private readonly ActivityContext                       activityContext_;
  private readonly ActivitySource                        activitySource_;
  private readonly IAgentHandler                         agentHandler_;
  private readonly DataPrefetcher                        dataPrefetcher_;
  private readonly TimeSpan                              delayBeforeAcquisition_;
  private readonly CancellationTokenSource               earlyCts_;
  private readonly ExceptionManager                      exceptionManager_;
  private readonly string                                folder_;
  private readonly FunctionExecutionMetrics<TaskHandler> functionExecutionMetrics_;
  private readonly HealthCheckRecord                     healthCheckRecord_;
  private readonly CancellationTokenSource               lateCts_;
  private readonly ILogger                               logger_;
  private readonly TimeSpan                              messageDuplicationDelay_;
  private readonly IQueueMessageHandler                  messageHandler_;
  private readonly IObjectStorage                        objectStorage_;
  private readonly string                                ownerPodId_;
  private readonly string                                ownerPodName_;
  private readonly TimeSpan                              processingCrashedDelay_;
  private readonly IPushQueueStorage                     pushQueueStorage_;
  private readonly IResultTable                          resultTable_;
  private readonly ISessionTable                         sessionTable_;
  private readonly ISubmitter                            submitter_;
  private readonly Submitter                             submitterOptions_;
  private readonly ITaskProcessingChecker                taskProcessingChecker_;
  private readonly ITaskTable                            taskTable_;
  private readonly string                                token_;
  private readonly IWorkerStreamHandler                  workerStreamHandler_;
  private          IAgent?                               agent_;
  private          TimeSpan?                             delayMessage_;
  private          DateTime?                             fetchedDate_;
  private          Action?                               onDispose_;
  private          Output?                               output_;
  private          SessionData?                          sessionData_;
  private          TaskData?                             taskData_;

  /// <summary>
  ///   Initializes a new instance of the <see cref="TaskHandler" /> class.
  /// </summary>
  /// <param name="sessionTable">The session table for managing session metadata.</param>
  /// <param name="taskTable">The task table for managing task metadata.</param>
  /// <param name="resultTable">The result table for managing result metadata.</param>
  /// <param name="pushQueueStorage">The queue storage for pushing tasks to be processed.</param>
  /// <param name="objectStorage">The object storage for managing payloads and results.</param>
  /// <param name="submitter">The submitter service for completing tasks and managing results.</param>
  /// <param name="dataPrefetcher">The data prefetcher for loading task input data.</param>
  /// <param name="workerStreamHandler">The handler for worker stream communication.</param>
  /// <param name="messageHandler">The handler for queue messages associated with the task.</param>
  /// <param name="taskProcessingChecker">The checker for verifying if a task is being processed by an agent.</param>
  /// <param name="ownerPodId">The identifier of the current agent.</param>
  /// <param name="ownerPodName">The name of the current agent.</param>
  /// <param name="activitySource">The activity source for distributed tracing.</param>
  /// <param name="agentHandler">The handler for agent lifecycle management.</param>
  /// <param name="logger">The logger for diagnostic and trace logging.</param>
  /// <param name="pollsterOptions">The pollster options for configuration.</param>
  /// <param name="submitterOptions">The submitter options for configuration.</param>
  /// <param name="onDispose">The action to execute on disposal.</param>
  /// <param name="exceptionManager">The exception manager for handling cancellation and errors.</param>
  /// <param name="functionExecutionMetrics">The metrics collector for function execution.</param>
  /// <param name="healthCheckRecord">The health check record for agent health monitoring.</param>
  public TaskHandler(ISessionTable                         sessionTable,
                     ITaskTable                            taskTable,
                     IResultTable                          resultTable,
                     IPushQueueStorage                     pushQueueStorage,
                     IObjectStorage                        objectStorage,
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
                     Submitter                             submitterOptions,
                     Action                                onDispose,
                     ExceptionManager                      exceptionManager,
                     FunctionExecutionMetrics<TaskHandler> functionExecutionMetrics,
                     HealthCheckRecord                     healthCheckRecord)
  {
    exceptionManager_         = exceptionManager;
    sessionTable_             = sessionTable;
    taskTable_                = taskTable;
    resultTable_              = resultTable;
    pushQueueStorage_         = pushQueueStorage;
    objectStorage_            = objectStorage;
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
    submitterOptions_         = submitterOptions;
    healthCheckRecord_        = healthCheckRecord;
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
    delayBeforeAcquisition_  = pollsterOptions.TimeoutBeforeNextAcquisition + TimeSpan.FromSeconds(2);
    messageDuplicationDelay_ = pollsterOptions.MessageDuplicationDelay;
    processingCrashedDelay_  = pollsterOptions.ProcessingCrashedDelay;

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

    if (delayMessage_ is not null)
    {
      _ = Task.Run(async () =>
                   {
                     using var _ = logger_.BeginNamedScope("DelayedReleaseTaskHandler",
                                                           ("taskId", messageHandler_.TaskId),
                                                           ("messageHandler", messageHandler_.MessageId),
                                                           ("sessionId", taskData_?.SessionId ?? ""));

                     try
                     {
                       do
                       {
                         logger_.LogDebug("MessageHandler will be delayed for {MessageDelay} with status {Status}",
                                          delayMessage_.Value,
                                          messageHandler_.Status);
                         await Task.Delay(delayMessage_.Value,
                                          exceptionManager_.EarlyCancellationToken)
                                   .ConfigureAwait(false);
                       } while (await taskProcessingChecker_.Check(taskData_.TaskId,
                                                                   taskData_.OwnerPodId,
                                                                   exceptionManager_.EarlyCancellationToken)
                                                            .ConfigureAwait(false));

                       logger_.LogDebug("Other pod is not processing anymore {Task}: MessageHandler is released with status {Status}",
                                        taskData_.TaskId,
                                        messageHandler_.Status);
                     }
                     catch (OperationCanceledException)
                     {
                       logger_.LogDebug("Agent stopping: MessageHandler for {Task} is released with status {Status}",
                                        taskData_.TaskId,
                                        messageHandler_.Status);
                     }
                     catch (Exception ex)
                     {
                       logger_.LogError(ex,
                                        "Error while checking other pod for {Task}: MessageHandler is released with status {Status}",
                                        taskData_.TaskId,
                                        messageHandler_.Status);
                     }
                     finally
                     {
                       await messageHandler_.DisposeIgnoreErrorAsync(logger_)
                                            .ConfigureAwait(false);
                     }
                   });
    }
    else
    {
      await messageHandler_.DisposeIgnoreErrorAsync(logger_)
                           .ConfigureAwait(false);
    }
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

      if (sessionData_.Status is SessionStatus.Cancelled or SessionStatus.Deleted or SessionStatus.Closed or SessionStatus.Purged &&
          taskData_.Status is not (TaskStatus.Cancelled or TaskStatus.Completed or TaskStatus.Error))
      {
        logger_.LogInformation("Task is being cancelled because its session is {sessionStatus}",
                               sessionData_.Status);

        messageHandler_.Status = QueueMessageStatus.Cancelled;

        // Propagate cancelled status to TaskHandler
        taskData_ = await taskTable_.EndTaskAsync(taskData_,
                                                  TaskStatus.Cancelled)
                                    .ConfigureAwait(false);

        await ResultLifeCycleHelper.TerminateTasksAndResults(taskTable_,
                                                         resultTable_,
                                                         new[]
                                                         {
                                                           messageHandler_.TaskId,
                                                         },
                                                         reason:
                                                         $"Task {messageHandler_.TaskId} has been cancelled because its session {taskData_.SessionId} is {sessionData_.Status}")
                                   .ConfigureAwait(false);

        return AcquisitionStatus.SessionNotExecutable;
      }

      if (sessionData_.Status == SessionStatus.Paused)
      {
        logger_.LogDebug("Session paused; message deleted");
        messageHandler_.Status = QueueMessageStatus.Processed;
        return AcquisitionStatus.SessionPaused;
      }

      switch (taskData_.Status)
      {
        case TaskStatus.Cancelling:
          logger_.LogInformation("Task is being cancelled");
          messageHandler_.Status = QueueMessageStatus.Cancelled;

          // Propagate cancelled status to TaskHandler
          taskData_ = await taskTable_.EndTaskAsync(taskData_,
                                                    TaskStatus.Cancelled)
                                      .ConfigureAwait(false);

          await ResultLifeCycleHelper.TerminateTasksAndResults(taskTable_,
                                                           resultTable_,
                                                           new[]
                                                           {
                                                             messageHandler_.TaskId,
                                                           },
                                                           reason: $"Task {messageHandler_.TaskId} has been cancelled:\n{taskData_.Output.Error}")
                                     .ConfigureAwait(false);

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
          await ResultLifeCycleHelper.TerminateTasksAndResults(taskTable_,
                                                           resultTable_,
                                                           new[]
                                                           {
                                                             messageHandler_.TaskId,
                                                           },
                                                           reason: $"Task {messageHandler_.TaskId} was on error:\n{taskData_.Output.Error}")
                                     .ConfigureAwait(false);
          return AcquisitionStatus.TaskIsError;
        case TaskStatus.Timeout:
          logger_.LogInformation("Task was timeout elsewhere ; taking over here");
          messageHandler_.Status = QueueMessageStatus.Poisonous;
          break;
        case TaskStatus.Cancelled:
          logger_.LogInformation("Task has been cancelled");
          messageHandler_.Status = QueueMessageStatus.Cancelled;
          await ResultLifeCycleHelper.TerminateTasksAndResults(taskTable_,
                                                           resultTable_,
                                                           new[]
                                                           {
                                                             messageHandler_.TaskId,
                                                           },
                                                           reason: $"Task {messageHandler_.TaskId} was cancelled:\n{taskData_.Output.Error}")
                                     .ConfigureAwait(false);
          return AcquisitionStatus.TaskIsCancelled;
        case TaskStatus.Processing:

          // If OwnerPodId is empty, it means that task was partially started or released
          // so we put the task in error and retry it somewhere else
          if (taskData_.OwnerPodId == "")
          {
            logger_.LogDebug("Resubmitting task {task} on another pod",
                             taskData_.TaskId);
            messageHandler_.Status = QueueMessageStatus.Cancelled;

            await submitter_.CompleteTaskAsync(taskData_,
                                               sessionData_,
                                               true,
                                               new Output(OutputStatus.Error,
                                                          $"Other pod seems to have released task while keeping {taskData_.Status} status, resubmitting task"),
                                               CancellationToken.None)
                            .ConfigureAwait(false);

            // Propagate retried status to TaskHandler
            taskData_ = taskData_ with
                        {
                          Status = taskData_.RetryOfIds.Count < taskData_.Options.MaxRetries
                                     ? TaskStatus.Retried
                                     : TaskStatus.Error,
                        };

            return AcquisitionStatus.TaskIsProcessingPodIdEmpty;
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
                logger_.LogInformation("Task {task} with status: {status} was being processed on another pod, but has been released during acquirement",
                                       taskData_.TaskId,
                                       taskData_.Status);
                messageHandler_.Status = QueueMessageStatus.Postponed;
                // TODO: AcquistionStatus must be tested
                return AcquisitionStatus.TaskSubmittedButPreviouslyProcessing;
              }

              if (taskData_.Status is TaskStatus.Processing or TaskStatus.Dispatched or TaskStatus.Processed)
              {
                var status = await TaskLifeCycleHelper.HandleTaskCrashedWhileProcessing(taskTable_,
                                                                                        resultTable_,
                                                                                        objectStorage_,
                                                                                        pushQueueStorage_,
                                                                                        submitterOptions_,
                                                                                        processingCrashedDelay_,
                                                                                        sessionData_,
                                                                                        taskData_,
                                                                                        logger_,
                                                                                        CancellationToken.None)
                                                      .ConfigureAwait(false);

                // Propagate retried status to TaskHandler
                taskData_ = taskData_ with
                            {
                              Status = status,
                            };

                messageHandler_.Status = QueueMessageStatus.Processed;
                return AcquisitionStatus.TaskIsProcessingButSeemsCrashed;
              }
            }
            // task is processing elsewhere so message is duplicated
            else
            {
              // if the task is running elsewhere, we delayed the release of the message to let time
              // for the other pod to either complete the task, or finish crashing.
              // If the other pod is still processing the task the next time, it will loop on this check
              // until the pod finishes or crashes.
              logger_.LogInformation("Task {taskId} is still processing by {OtherOwnerPodId}, delay the requeue of the message for {MessageDelay}",
                                     taskData_.TaskId,
                                     taskData_.OwnerPodId,
                                     messageDuplicationDelay_);
              delayMessage_          = messageDuplicationDelay_;
              messageHandler_.Status = QueueMessageStatus.Postponed;
              return AcquisitionStatus.TaskIsProcessingElsewhere;
            }
          }

          logger_.LogInformation("Task {taskId} is still processing on this pod, delay the requeue of the message for {MessageDelay}",
                                 taskData_.TaskId,
                                 messageDuplicationDelay_);
          delayMessage_          = messageDuplicationDelay_;
          messageHandler_.Status = QueueMessageStatus.Postponed;
          return AcquisitionStatus.TaskIsProcessingHere;
        case TaskStatus.Retried:
          logger_.LogInformation("Task is in retry ; retry task should be executed");

          await TaskLifeCycleHelper.RetryTaskAsync(taskTable_,
                                                   resultTable_,
                                                   pushQueueStorage_,
                                                   taskData_,
                                                   sessionData_,
                                                   null,
                                                   taskData_.Output.Error,
                                                   logger_,
                                                   lateCts_.Token)
                                   .ConfigureAwait(false);
          messageHandler_.Status = QueueMessageStatus.Poisonous;

          return AcquisitionStatus.TaskIsRetried;

        case TaskStatus.Unspecified:
        default:
          logger_.LogCritical("Task was in an unknown state {state}",
                              taskData_.Status);
          throw new ArgumentException(nameof(taskData_));
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
            logger_.LogInformation("Task {task} with status {status} was dispatched on another pod who crashed, but has been released during acquirement",
                                   taskData_.TaskId,
                                   taskData_.Status);
            messageHandler_.Status = QueueMessageStatus.Postponed;
            // TODO: AcquistionStatus TaskSubmittedButPreviouslyDispatched must be tested
            return AcquisitionStatus.TaskSubmittedButPreviouslyDispatched;
          }

          if (taskData_.Status is TaskStatus.Dispatched && taskData_.AcquisitionDate + delayBeforeAcquisition_ > DateTime.UtcNow)
          {
            delayMessage_          = taskData_.AcquisitionDate + delayBeforeAcquisition_ - DateTime.UtcNow;
            messageHandler_.Status = QueueMessageStatus.Postponed;
            logger_.LogDebug("Wait to exceed acquisition timeout before resubmitting task");
            return AcquisitionStatus.AcquisitionFailedTimeoutNotExceeded;
          }

          if (taskData_.Status is TaskStatus.Dispatched)
          {
            logger_.LogInformation("Task {taskId} is still dispatched on {OtherOwnerPodId}, but other pod seems to have crashed. Release the task",
                                   taskData_.TaskId,
                                   taskData_.OwnerPodId);
            await ReleaseAndPostponeTask()
              .ConfigureAwait(false);
            return AcquisitionStatus.AcquisitionFailedDispatchedCrashed;
          }

          if (taskData_.Status is TaskStatus.Processing or TaskStatus.Processed)
          {
            var status = await TaskLifeCycleHelper.HandleTaskCrashedWhileProcessing(taskTable_,
                                                                                    resultTable_,
                                                                                    objectStorage_,
                                                                                    pushQueueStorage_,
                                                                                    submitterOptions_,
                                                                                    processingCrashedDelay_,
                                                                                    sessionData_,
                                                                                    taskData_,
                                                                                    logger_,
                                                                                    CancellationToken.None)
                                                  .ConfigureAwait(false);

            // Propagate retried status to TaskHandler
            taskData_ = taskData_ with
                        {
                          Status = status,
                        };
            messageHandler_.Status = QueueMessageStatus.Processed;
            return AcquisitionStatus.AcquisitionFailedProcessingCrashed;
          }


          if (taskData_.Status is TaskStatus.Cancelling)
          {
            messageHandler_.Status = QueueMessageStatus.Cancelled;
            taskData_ = await taskTable_.EndTaskAsync(taskData_,
                                                      TaskStatus.Cancelled)
                                        .ConfigureAwait(false);
            await ResultLifeCycleHelper.TerminateTasksAndResults(taskTable_,
                                                             resultTable_,
                                                             new[]
                                                             {
                                                               messageHandler_.TaskId,
                                                             },
                                                             reason:
                                                             $"Task {messageHandler_.TaskId} has been cancelled while acquired on another pod:\n{taskData_.Output.Error}")
                                       .ConfigureAwait(false);
            return AcquisitionStatus.AcquisitionFailedTaskCancelling;
          }
        }

        // if the task is running elsewhere, we delayed the release of the message to let time
        // for the other pod to either complete the task, or finish crashing.
        // If the other pod is still processing the task the next time, it will loop on this check
        // until the pod finishes or crashes.
        logger_.LogInformation("Task {taskId} is still processing by {OtherOwnerPodId}, delay the requeue of the message for {MessageDelay}",
                               taskData_.TaskId,
                               taskData_.OwnerPodId,
                               messageDuplicationDelay_);
        delayMessage_          = messageDuplicationDelay_;
        messageHandler_.Status = QueueMessageStatus.Postponed;
        return AcquisitionStatus.AcquisitionFailedMessageDuplicated;
      }

      if (taskData_.OwnerPodId == ownerPodId_ && taskData_.Status != TaskStatus.Dispatched)
      {
        logger_.LogInformation("Task {taskId} is still processing on this pod, delay the requeue of the message for {MessageDelay}",
                               taskData_.TaskId,
                               messageDuplicationDelay_);
        delayMessage_          = messageDuplicationDelay_;
        messageHandler_.Status = QueueMessageStatus.Postponed;
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
    catch (SessionNotFoundException e)
    {
      messageHandler_.Status = QueueMessageStatus.Processed;

      logger_.LogWarning(e,
                         "SessionId for the task coming from message queue was not found, delete message from queue and abort task");
      await TaskLifeCycleHelper.AbortTaskAsync(taskTable_,
                                               resultTable_,
                                               objectStorage_,
                                               submitterOptions_,
                                               taskData_,
                                               OutputStatus.Error,
                                               "Session was not found",
                                               logger_)
                               .ConfigureAwait(false);

      return AcquisitionStatus.SessionNotFound;
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
    catch (ObjectDataNotFoundException e)
    {
      await HandleErrorRequeueAsync(e,
                                    taskData_,
                                    earlyCts_.Token)
        .ConfigureAwait(false);
    }
    catch (Exception e)
    {
      await HandleErrorResubmitAsync(e,
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
    var checkTask = workerStreamHandler_.Check(HealthCheckTag.Liveness);

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

    var check = await checkTask.ConfigureAwait(false);
    logger_.LogDebug("Checked worker {@Status}",
                     check.Status);
    if (check.Status is HealthStatus.Unhealthy)
    {
      await ReleaseAndPostponeTask()
        .ConfigureAwait(false);

      throw new WorkerDownException($"Worker was down while starting the processing of task: {check.Description}",
                                    check.Exception);
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
      await HandleErrorResubmitAsync(e,
                                     taskData_,
                                     lateCts_.Token)
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
        await agent_.CreateResultsAndSubmitChildTasksAsync(CancellationToken.None)
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
                                      cancellationToken)
         .ConfigureAwait(false);

  private async Task HandleErrorResubmitAsync(Exception         e,
                                              TaskData          taskData,
                                              CancellationToken cancellationToken)
    => await HandleErrorInternalAsync(e,
                                      taskData,
                                      true,
                                      cancellationToken)
         .ConfigureAwait(false);

  private async Task HandleErrorInternalAsync(Exception         e,
                                              TaskData          taskData,
                                              bool              resubmit,
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
    else if (e is OperationCanceledException or RpcException
                                                {
                                                  InnerException: OperationCanceledException,
                                                })
    {
      var report = healthCheckRecord_.LastCheck();

      logger_.LogDebug(e,
                       "Cancellation detected {CancellationRequested}, agent health is {@HealthReport}, application is {ApplicationStatus}",
                       cancellationToken.IsCancellationRequested,
                       report.Entries.Select(kv => new KeyValuePair<string, HealthStatus>(kv.Key,
                                                                                          kv.Value.Status))
                             .ToDictionary(),
                       exceptionManager_.Failed
                         ? "Failed"
                         : "Running");


      if (report.Status is HealthStatus.Healthy && !exceptionManager_.Failed)
      {
        if (cancellationToken.IsCancellationRequested)
        {
          logger_.LogWarning(e,
                             "Cancellation triggered, task cancelled here and re executed elsewhere");
        }
        else
        {
          logger_.LogError(e,
                           "Task cancelled here and re executed elsewhere, but cancellation was not requested");
        }

        await ReleaseAndPostponeTask()
          .ConfigureAwait(false);
      }
      else
      {
        logger_.LogError(e,
                         "Agent is stopping while unhealthy or failed: {@HealthReport}",
                         report.Entries.Select(kv => new KeyValuePair<string, HealthStatus>(kv.Key,
                                                                                            kv.Value.Status))
                               .ToDictionary());

        await submitter_.CompleteTaskAsync(taskData,
                                           sessionData_,
                                           resubmit,
                                           new Output(OutputStatus.Error,
                                                      $"Agent is stopping while unhealthy: {report.Entries}"),
                                           CancellationToken.None)
                        .ConfigureAwait(false);


        messageHandler_.Status = resubmit
                                   ? QueueMessageStatus.Cancelled
                                   : QueueMessageStatus.Processed;
      }
    }
    else
    {
      logger_.LogError(e,
                       "Error during task execution: {Decision}",
                       resubmit
                         ? "retrying task"
                         : "cancelling task");

      var isWorkerDown = e is RpcException re && IsStatusFatal(re.StatusCode);
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

    e.RethrowWithStacktrace();
  }

  internal static bool IsStatusFatal(StatusCode statusCode)
    => statusCode is StatusCode.Aborted or StatusCode.Cancelled or StatusCode.Unavailable or StatusCode.Unknown;
}
