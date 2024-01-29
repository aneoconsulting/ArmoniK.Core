// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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
using System.Diagnostics;
using System.IO;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Base;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Pollster.TaskProcessingChecker;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Stream.Worker;

using Grpc.Core;

using Microsoft.Extensions.Logging;

using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

namespace ArmoniK.Core.Common.Pollster;

public sealed class TaskHandler : IAsyncDisposable
{
  private readonly ActivitySource                activitySource_;
  private readonly IAgentHandler                 agentHandler_;
  private readonly CancellationTokenSource       cancellationTokenSource_;
  private readonly DataPrefetcher                dataPrefetcher_;
  private readonly TimeSpan                      delayBeforeAcquisition_;
  private readonly string                        folder_;
  private readonly ILogger                       logger_;
  private readonly IQueueMessageHandler          messageHandler_;
  private readonly Action                        onDispose_;
  private readonly string                        ownerPodId_;
  private readonly string                        ownerPodName_;
  private readonly CancellationTokenRegistration reg1_;
  private readonly IResultTable                  resultTable_;
  private readonly ISessionTable                 sessionTable_;
  private readonly ISubmitter                    submitter_;
  private readonly ITaskProcessingChecker        taskProcessingChecker_;
  private readonly ITaskTable                    taskTable_;
  private readonly string                        token_;
  private readonly CancellationTokenSource       workerConnectionCts_;
  private readonly IWorkerStreamHandler          workerStreamHandler_;
  private          IAgent?                       agent_;
  private          Output?                       output_;
  private          SessionData?                  sessionData_;
  private          TaskData?                     taskData_;

  public TaskHandler(ISessionTable              sessionTable,
                     ITaskTable                 taskTable,
                     IResultTable               resultTable,
                     ISubmitter                 submitter,
                     DataPrefetcher             dataPrefetcher,
                     IWorkerStreamHandler       workerStreamHandler,
                     IQueueMessageHandler       messageHandler,
                     ITaskProcessingChecker     taskProcessingChecker,
                     string                     ownerPodId,
                     string                     ownerPodName,
                     ActivitySource             activitySource,
                     IAgentHandler              agentHandler,
                     ILogger                    logger,
                     Injection.Options.Pollster pollsterOptions,
                     Action                     onDispose,
                     CancellationTokenSource    cancellationTokenSource)
  {
    sessionTable_          = sessionTable;
    taskTable_             = taskTable;
    resultTable_           = resultTable;
    messageHandler_        = messageHandler;
    taskProcessingChecker_ = taskProcessingChecker;
    submitter_             = submitter;
    dataPrefetcher_        = dataPrefetcher;
    workerStreamHandler_   = workerStreamHandler;
    activitySource_        = activitySource;
    agentHandler_          = agentHandler;
    logger_                = logger;
    onDispose_             = onDispose;
    ownerPodId_            = ownerPodId;
    ownerPodName_          = ownerPodName;
    taskData_              = null;
    sessionData_           = null;
    token_ = Guid.NewGuid()
                 .ToString();
    folder_ = Path.Combine(pollsterOptions.SharedCacheFolder,
                           token_);
    Directory.CreateDirectory(folder_);
    delayBeforeAcquisition_ = pollsterOptions.TimeoutBeforeNextAcquisition + TimeSpan.FromSeconds(2);

    workerConnectionCts_     = new CancellationTokenSource();
    cancellationTokenSource_ = new CancellationTokenSource();

    reg1_ = cancellationTokenSource.Token.Register(() => cancellationTokenSource_.Cancel());

    cancellationTokenSource_.Token.Register(() =>
                                            {
                                              logger_.LogWarning("Cancellation triggered, waiting {waitingTime} before cancelling task",
                                                                 pollsterOptions.GraceDelay);
                                              workerConnectionCts_.CancelAfter(pollsterOptions.GraceDelay);
                                            });
    workerConnectionCts_.Token.Register(() => logger_.LogWarning("Cancellation triggered, start to properly cancel task"));
  }


  /// <inheritdoc />
  public async ValueTask DisposeAsync()
  {
    using var _ = logger_.BeginNamedScope("DisposeAsync",
                                          ("taskId", messageHandler_.TaskId),
                                          ("messageHandler", messageHandler_.MessageId),
                                          ("sessionId", taskData_?.SessionId ?? ""));

    onDispose_.Invoke();
    logger_.LogDebug("MessageHandler status is {status}",
                     messageHandler_.Status);
    await messageHandler_.DisposeAsync()
                         .ConfigureAwait(false);
    reg1_.Unregister();
    await reg1_.DisposeAsync()
               .ConfigureAwait(false);
    cancellationTokenSource_.Dispose();
    workerConnectionCts_.Dispose();
    agent_?.Dispose();
    try
    {
      Directory.Delete(folder_,
                       true);
    }
    catch (DirectoryNotFoundException)
    {
    }
  }

  /// <summary>
  ///   Refresh task metadata and stop execution if current task should be cancelled
  /// </summary>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  public async Task StopCancelledTask()
  {
    if (taskData_?.Status is not null or TaskStatus.Cancelled or TaskStatus.Cancelling)
    {
      taskData_ = await taskTable_.ReadTaskAsync(messageHandler_.TaskId,
                                                 CancellationToken.None)
                                  .ConfigureAwait(false);
      if (taskData_.Status is TaskStatus.Cancelling)
      {
        logger_.LogWarning("Task has been cancelled, trigger cancellation from exterior.");
        cancellationTokenSource_.Cancel();
      }
    }
  }

  /// <summary>
  ///   Acquisition of the task in the message given to the constructor
  /// </summary>
  /// <returns>
  ///   Integer representing whether the task has been acquired
  ///   Acquired when return is 0
  /// </returns>
  /// <exception cref="ArgumentException">status of the task is not recognized</exception>
  public async Task<int> AcquireTask()
  {
    using var activity = activitySource_.StartActivity($"{nameof(AcquireTask)}");
    using var _ = logger_.BeginNamedScope("Acquiring task",
                                          ("messageHandler", messageHandler_.MessageId),
                                          ("taskId", messageHandler_.TaskId));

    try
    {
      taskData_ = await taskTable_.ReadTaskAsync(messageHandler_.TaskId,
                                                 CancellationToken.None)
                                  .ConfigureAwait(false);

      using var sessionScope = logger_.BeginPropertyScope(("sessionId", taskData_.SessionId));
      logger_.LogInformation("Start task acquisition");

      if (cancellationTokenSource_.IsCancellationRequested)
      {
        messageHandler_.Status = QueueMessageStatus.Postponed;
        logger_.LogDebug("Task data read but execution cancellation requested");
        return 1;
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
          await ResultLifeCycleHelper.AbortTaskAndResults(taskTable_,
                                                          resultTable_,
                                                          messageHandler_.TaskId,
                                                          CancellationToken.None)
                                     .ConfigureAwait(false);

          return 2;
        case TaskStatus.Completed:
          logger_.LogInformation("Task was already completed");
          messageHandler_.Status = QueueMessageStatus.Processed;
          return 3;
        case TaskStatus.Creating:
          logger_.LogInformation("Task is still creating");
          messageHandler_.Status = QueueMessageStatus.Postponed;
          return 4;
        case TaskStatus.Submitted:
          break;
        case TaskStatus.Dispatched:
          break;
        case TaskStatus.Error:
          logger_.LogInformation("Task was on error elsewhere ; task should have been resubmitted");
          messageHandler_.Status = QueueMessageStatus.Cancelled;
          await ResultLifeCycleHelper.AbortTaskAndResults(taskTable_,
                                                          resultTable_,
                                                          messageHandler_.TaskId,
                                                          CancellationToken.None)
                                     .ConfigureAwait(false);
          return 5;
        case TaskStatus.Timeout:
          logger_.LogInformation("Task was timeout elsewhere ; taking over here");
          break;
        case TaskStatus.Cancelled:
          logger_.LogInformation("Task has been cancelled");
          messageHandler_.Status = QueueMessageStatus.Cancelled;
          await ResultLifeCycleHelper.AbortTaskAndResults(taskTable_,
                                                          resultTable_,
                                                          messageHandler_.TaskId,
                                                          CancellationToken.None)
                                     .ConfigureAwait(false);
          return 6;
        case TaskStatus.Processing:

          // If OwnerPodId is empty, it means that task was partially started or released
          // so we put the task in error and retry it somewhere else
          if (taskData_.OwnerPodId == "")
          {
            logger_.LogDebug("Resubmitting task {task} on another pod",
                             taskData_.TaskId);

            await submitter_.CompleteTaskAsync(taskData_,
                                               true,
                                               new Output(false,
                                                          $"Other pod seems to have released task while keeping {taskData_.Status} status, resubmitting task"),
                                               CancellationToken.None)
                            .ConfigureAwait(false);
            return 7;
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

              if (taskData_.Status is TaskStatus.Processing or TaskStatus.Dispatched or TaskStatus.Processed)
              {
                logger_.LogDebug("Resubmitting task {task} on another pod",
                                 taskData_.TaskId);
                await submitter_.CompleteTaskAsync(taskData_,
                                                   true,
                                                   new Output(false,
                                                              "Other pod seems to have crashed, resubmitting task"),
                                                   CancellationToken.None)
                                .ConfigureAwait(false);
                messageHandler_.Status = QueueMessageStatus.Processed;
                return 8;
              }
            }
            // task is processing elsewhere so message is duplicated
            else
            {
              messageHandler_.Status = QueueMessageStatus.Processed;
              return 9;
            }
          }

          logger_.LogWarning("Task already in processing on this pod. This scenario should be managed earlier. Message likely duplicated. Removing it from queue");
          messageHandler_.Status = QueueMessageStatus.Processed;
          return 10;
        case TaskStatus.Retried:
          logger_.LogInformation("Task is in retry ; retry task should be executed");
          return 11;
        case TaskStatus.Unspecified:
        default:
          logger_.LogCritical("Task was in an unknown state {state}",
                              taskData_.Status);
          throw new ArgumentException(nameof(taskData_));
      }

      sessionData_ = await sessionTable_.GetSessionAsync(taskData_.SessionId,
                                                         CancellationToken.None)
                                        .ConfigureAwait(false);
      var isSessionCancelled = sessionData_.Status == SessionStatus.Cancelled;

      if (isSessionCancelled && taskData_.Status is not (TaskStatus.Cancelled or TaskStatus.Completed or TaskStatus.Error))
      {
        logger_.LogInformation("Task is being cancelled because its session is cancelled");

        messageHandler_.Status = QueueMessageStatus.Cancelled;
        taskData_ = taskData_ with
                    {
                      EndDate = DateTime.UtcNow,
                      CreationToEndDuration = DateTime.UtcNow - taskData_.CreationDate,
                    };
        await taskTable_.SetTaskCanceledAsync(taskData_,
                                              CancellationToken.None)
                        .ConfigureAwait(false);

        await ResultLifeCycleHelper.AbortTaskAndResults(taskTable_,
                                                        resultTable_,
                                                        messageHandler_.TaskId,
                                                        CancellationToken.None)
                                   .ConfigureAwait(false);

        return 12;
      }

      if (cancellationTokenSource_.IsCancellationRequested)
      {
        messageHandler_.Status = QueueMessageStatus.Postponed;
        logger_.LogDebug("Session running but execution cancellation requested");
        return 13;
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

      if (cancellationTokenSource_.IsCancellationRequested)
      {
        logger_.LogDebug("Task acquired but execution cancellation requested");
        await taskTable_.ReleaseTask(taskData_,
                                     CancellationToken.None)
                        .ConfigureAwait(false);
        messageHandler_.Status = QueueMessageStatus.Postponed;
        return 14;
      }

      // empty OwnerPodId means that the task was not acquired because not ready
      if (taskData_.OwnerPodId == "")
      {
        logger_.LogDebug("Task acquired but not ready (empty owner pod id)");
        messageHandler_.Status = QueueMessageStatus.Postponed;
        return 15;
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

          if (taskData_.Status is TaskStatus.Dispatched && taskData_.AcquisitionDate + delayBeforeAcquisition_ > DateTime.UtcNow)

          {
            messageHandler_.Status = QueueMessageStatus.Postponed;
            logger_.LogDebug("Wait to exceed acquisition timeout before resubmitting task");
            return 16;
          }

          if (taskData_.Status is TaskStatus.Processing or TaskStatus.Dispatched or TaskStatus.Processed)
          {
            logger_.LogDebug("Resubmitting task {task} on another pod",
                             taskData_.TaskId);
            await submitter_.CompleteTaskAsync(taskData_,
                                               true,
                                               new Output(false,
                                                          "Other pod seems to have crashed, resubmitting task"),
                                               CancellationToken.None)
                            .ConfigureAwait(false);
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
            await ResultLifeCycleHelper.AbortTaskAndResults(taskTable_,
                                                            resultTable_,
                                                            messageHandler_.TaskId,
                                                            CancellationToken.None)
                                       .ConfigureAwait(false);
            return 17;
          }
        }

        // if the task is running elsewhere, then the message is duplicated so we remove it from the queue
        // and do not acquire the task
        messageHandler_.Status = QueueMessageStatus.Processed;
        return 18;
      }

      if (taskData_.OwnerPodId == ownerPodId_ && taskData_.Status != TaskStatus.Dispatched)
      {
        logger_.LogInformation("Task is already managed by this agent; message likely to be duplicated");
        messageHandler_.Status = QueueMessageStatus.Processed;
        return 19;
      }

      if (cancellationTokenSource_.IsCancellationRequested)
      {
        logger_.LogDebug("Task preconditions ok but execution cancellation requested");
        await taskTable_.ReleaseTask(taskData_,
                                     CancellationToken.None)
                        .ConfigureAwait(false);
        messageHandler_.Status = QueueMessageStatus.Postponed;
        return 20;
      }

      logger_.LogDebug("Task preconditions are OK");
      return 0;
    }
    catch (TaskNotFoundException e)
    {
      logger_.LogWarning(e,
                         "TaskId coming from message queue was not found, delete message from queue");
      messageHandler_.Status = QueueMessageStatus.Processed;
      return 21;
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
                        messageHandler_.MessageId)
         : throw new ArmoniKException("TaskData should not be null after successful acquisition");

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
    if (taskData_ is null)
    {
      throw new NullReferenceException();
    }

    using var _ = logger_.BeginNamedScope("PreProcessing",
                                          ("messageHandler", messageHandler_.MessageId),
                                          ("taskId", messageHandler_.TaskId),
                                          ("sessionId", taskData_.SessionId));
    logger_.LogDebug("Start prefetch data");

    try
    {
      await dataPrefetcher_.PrefetchDataAsync(taskData_,
                                              folder_,
                                              cancellationTokenSource_.Token)
                           .ConfigureAwait(false);
    }
    catch (Exception e)
    {
      await HandleErrorRequeueAsync(e,
                                    taskData_,
                                    cancellationTokenSource_.Token)
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
    if (taskData_ is null || sessionData_ is null)
    {
      throw new NullReferenceException();
    }

    using var _ = logger_.BeginNamedScope("TaskExecution",
                                          ("messageHandler", messageHandler_.MessageId),
                                          ("taskId", messageHandler_.TaskId),
                                          ("sessionId", taskData_.SessionId));

    try
    {
      logger_.LogDebug("Create agent server to receive requests from worker");

      // In theory we could create the server during dependencies checking and activate it only now
      agent_ = await agentHandler_.Start(token_,
                                         logger_,
                                         sessionData_,
                                         taskData_,
                                         folder_,
                                         cancellationTokenSource_.Token)
                                  .ConfigureAwait(false);

      logger_.LogInformation("Start executing task");
      taskData_ = taskData_ with
                  {
                    StartDate = DateTime.UtcNow,
                    PodTtl = DateTime.UtcNow,
                  };
      // Status update should not be cancelled
      // Task will be marked as processing then start
      await taskTable_.StartTask(taskData_,
                                 CancellationToken.None)
                      .ConfigureAwait(false);
    }
    // this catch may have to be removed
    catch (TaskAlreadyInFinalStateException e)
    {
      messageHandler_.Status = QueueMessageStatus.Processed;
      logger_.LogWarning(e,
                         "Task already in a final state, removing it from the queue");
      throw;
    }
    catch (Exception e)
    {
      await HandleErrorRequeueAsync(e,
                                    taskData_,
                                    cancellationTokenSource_.Token)
        .ConfigureAwait(false);
    }

    try
    {
      // at this point worker requests should have ended
      logger_.LogDebug("Wait for task output");
      output_ = await workerStreamHandler_.StartTaskProcessing(taskData_,
                                                               token_,
                                                               folder_,
                                                               workerConnectionCts_.Token)
                                          .ConfigureAwait(false);

      taskData_ = taskData_ with
                  {
                    ProcessedDate = DateTime.UtcNow,
                  };

      logger_.LogDebug("Stop agent server");
      await agentHandler_.Stop(workerConnectionCts_.Token)
                         .ConfigureAwait(false);
    }
    catch (Exception e)
    {
      taskData_ = taskData_ with
                  {
                    ProcessedDate = DateTime.UtcNow,
                  };
      await HandleErrorTaskExecutionAsync(e,
                                          taskData_,
                                          cancellationTokenSource_.Token)
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
    if (taskData_ is null)
    {
      throw new NullReferenceException(nameof(taskData_) + " is null.");
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
                             output_.Success);

      if (output_.Success)
      {
        logger_.LogDebug("Complete processing of the request");
        await agent_.FinalizeTaskCreation(CancellationToken.None)
                    .ConfigureAwait(false);
      }
      else
      {
        await agent_.CancelChildTasks(CancellationToken.None)
                    .ConfigureAwait(false);
      }

      await submitter_.CompleteTaskAsync(taskData_,
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
                                     cancellationTokenSource_.Token)
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
    if (agent_ is null)
    {
      throw new NullReferenceException(nameof(agent_) + " is null.");
    }

    if (taskData.Status is TaskStatus.Cancelled or TaskStatus.Cancelling)
    {
      messageHandler_.Status = QueueMessageStatus.Processed;
    }
    else
    {
      var isWorkerDown = e is RpcException re && IsStatusFatal(re.StatusCode);

      if (cancellationToken.IsCancellationRequested || (requeueIfUnavailable && isWorkerDown))
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

        await taskTable_.ReleaseTask(taskData,
                                     CancellationToken.None)
                        .ConfigureAwait(false);
        messageHandler_.Status = QueueMessageStatus.Postponed;
      }
      else
      {
        logger_.LogError(e,
                         "Error during task execution: {Decision}",
                         resubmit
                           ? "retrying task"
                           : "cancelling task");

        await submitter_.CompleteTaskAsync(taskData,
                                           resubmit,
                                           new Output(false,
                                                      e.Message),
                                           CancellationToken.None)
                        .ConfigureAwait(false);


        messageHandler_.Status = resubmit
                                   ? QueueMessageStatus.Cancelled
                                   : QueueMessageStatus.Processed;
      }

      await agent_.CancelChildTasks(CancellationToken.None)
                  .ConfigureAwait(false);
    }

    // Rethrow enable the recording of the error by the Pollster Main loop
    // Keep the stack trace for the rethrown exception
    ExceptionDispatchInfo.Capture(e)
                         .Throw();
  }

  internal static bool IsStatusFatal(StatusCode statusCode)
    => statusCode is StatusCode.Aborted or StatusCode.Cancelled or StatusCode.Unavailable or StatusCode.Unknown;
}
