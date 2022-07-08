// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Worker;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Pollster.TaskProcessingChecker;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Stream.Worker;

using Grpc.Core;

using Microsoft.Extensions.Logging;

using Output = ArmoniK.Api.gRPC.V1.Output;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Pollster;

internal class TaskHandler : IAsyncDisposable
{
  private readonly ISessionTable                               sessionTable_;
  private readonly ITaskTable                                  taskTable_;
  private readonly IResultTable                                resultTable_;
  private readonly IQueueMessageHandler                        messageHandler_;
  private readonly ITaskProcessingChecker                      taskProcessingChecker_;
  private readonly ISubmitter                                  submitter_;
  private readonly DataPrefetcher                              dataPrefetcher_;
  private readonly IWorkerStreamHandler                        workerStreamHandler_;
  private readonly IObjectStorageFactory                       objectStorageFactory_;
  private readonly ActivitySource                              activitySource_;
  private readonly ILogger                                     logger_;
  private          TaskData?                                   taskData_;
  private          Queue<ProcessRequest.Types.ComputeRequest>? computeRequestStream_;
  private readonly string                                      ownerPodId_;
  private readonly IAgentHandler                               agentHandler_;
  private readonly string                                      socketPath_;
  private          Agent?                                      agent_;

  public TaskHandler(ISessionTable          sessionTable,
                     ITaskTable             taskTable,
                     IResultTable           resultTable,
                     ISubmitter             submitter,
                     DataPrefetcher         dataPrefetcher,
                     IWorkerStreamHandler   workerStreamHandler,
                     IObjectStorageFactory  objectStorageFactory,
                     IQueueMessageHandler   messageHandler,
                     ITaskProcessingChecker taskProcessingChecker,
                     string                 ownerPodId,
                     ActivitySource         activitySource,
                     IAgentHandler          agentHandler,
                     string                 socketPath,
                     ILogger                logger)
  {
    sessionTable_          = sessionTable;
    taskTable_             = taskTable;
    resultTable_           = resultTable;
    messageHandler_        = messageHandler;
    taskProcessingChecker_ = taskProcessingChecker;
    submitter_             = submitter;
    dataPrefetcher_        = dataPrefetcher;
    workerStreamHandler_   = workerStreamHandler;
    objectStorageFactory_  = objectStorageFactory;
    activitySource_        = activitySource;
    agentHandler_          = agentHandler;
    logger_                = logger;
    socketPath_       = socketPath;
    ownerPodId_            = ownerPodId;
    taskData_              = null;
  }

  /// <summary>
  /// Acquisition of the task in the message given to the constructor
  /// </summary>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// Bool representing whether the task has been acquired
  /// </returns>
  /// <exception cref="ArgumentException">status of the task is not recognized</exception>
  public async Task<bool> AcquireTask(CancellationToken cancellationToken)
  {
    using var activity = activitySource_.StartActivity($"{nameof(AcquireTask)}");
    using var _ = logger_.BeginNamedScope("Acquiring task",
                                          ("taskId", messageHandler_.TaskId));

    logger_.LogInformation("Acquire task");
    try
    {
      taskData_ = await taskTable_.ReadTaskAsync(messageHandler_.TaskId,
                                                 cancellationToken)
                                  .ConfigureAwait(false);

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
        case TaskStatus.Canceling:
          logger_.LogInformation("Task is being cancelled");
          messageHandler_.Status = QueueMessageStatus.Cancelled;
          await taskTable_.SetTaskCanceledAsync(messageHandler_.TaskId,
                                                CancellationToken.None)
                          .ConfigureAwait(false);
          await resultTable_.AbortTaskResults(taskData_.SessionId,
                                              taskData_.TaskId,
                                              CancellationToken.None)
                            .ConfigureAwait(false);
          return false;
        case TaskStatus.Completed:
          logger_.LogInformation("Task was already completed");
          messageHandler_.Status = QueueMessageStatus.Processed;
          return false;
        case TaskStatus.Creating:
          logger_.LogInformation("Task is still creating");
          messageHandler_.Status = QueueMessageStatus.Postponed;
          return false;
        case TaskStatus.Submitted:
          break;
        case TaskStatus.Dispatched:
          break;
        case TaskStatus.Error:
          logger_.LogInformation("Task was on error elsewhere ; task should have been resubmitted");
          messageHandler_.Status = QueueMessageStatus.Cancelled;
          await resultTable_.AbortTaskResults(taskData_.SessionId,
                                              taskData_.TaskId,
                                              CancellationToken.None)
                            .ConfigureAwait(false);
          return false;
        case TaskStatus.Timeout:
          logger_.LogInformation("Task was timeout elsewhere ; taking over here");
          break;
        case TaskStatus.Canceled:
          logger_.LogInformation("Task has been cancelled");
          messageHandler_.Status = QueueMessageStatus.Cancelled;
          await resultTable_.AbortTaskResults(taskData_.SessionId,
                                              taskData_.TaskId,
                                              CancellationToken.None)
                            .ConfigureAwait(false);
          return false;
        case TaskStatus.Processing:
          logger_.LogInformation("Task is processing elsewhere ; taking over here");
          break;
        case TaskStatus.Failed:
          logger_.LogInformation("Task is failed");
          messageHandler_.Status = QueueMessageStatus.Poisonous;
          return false;
        case TaskStatus.Unspecified:
        default:
          logger_.LogCritical("Task was in an unknown state {state}",
                              taskData_.Status);
          throw new ArgumentException(nameof(taskData_));
      }

      if (taskData_.DataDependencies.Any())
      {
        var dependencies = await resultTable_.AreResultsAvailableAsync(taskData_.SessionId,
                                                                       taskData_.DataDependencies,
                                                                       cancellationToken)
                                             .ConfigureAwait(false);

        if (!dependencies.Any())
        {
          logger_.LogDebug("Dependencies are not ready yet.");
          messageHandler_.Status = QueueMessageStatus.Postponed;
          return false;
        }

        if (dependencies.SingleOrDefault(i => i.Status == ResultStatus.Completed,
                                         new ResultStatusCount(ResultStatus.Completed,
                                                               0))
                        .Count != taskData_.DataDependencies.Count)
        {
          logger_.LogDebug("Dependencies are not complete yet. Checking the status of the results");
          messageHandler_.Status = QueueMessageStatus.Postponed;

          if (dependencies.SingleOrDefault(i => i.Status == ResultStatus.Aborted,
                                           new ResultStatusCount(ResultStatus.Aborted,
                                                                 0))
                          .Count == 0)
          {
            logger_.LogDebug("No results aborted. Waiting for the remaining uncompleted results.");
            return false;
          }

          logger_.LogInformation("One of the input data is aborted. Removing task from the queue");

          await submitter_.CompleteTaskAsync(taskData_,
                                             false,
                                             new Output
                                             {
                                               Error = new Output.Types.Error
                                                       {
                                                         Details = "One of the input data is aborted.",
                                                       },
                                             },
                                             CancellationToken.None)
                          .ConfigureAwait(false);
          messageHandler_.Status = QueueMessageStatus.Cancelled;

          return false;
        }
      }

      var isSessionCancelled = await sessionTable_.IsSessionCancelledAsync(taskData_.SessionId,
                                                                           cancellationToken)
                                                  .ConfigureAwait(false);

      if (isSessionCancelled && taskData_.Status is not (TaskStatus.Canceled or TaskStatus.Completed or TaskStatus.Error))
      {
        logger_.LogInformation("Task is being cancelled");

        messageHandler_.Status = QueueMessageStatus.Cancelled;
        await taskTable_.SetTaskCanceledAsync(messageHandler_.TaskId,
                                              cancellationToken)
                        .ConfigureAwait(false);
        await resultTable_.AbortTaskResults(taskData_.SessionId,
                                            taskData_.TaskId,
                                            CancellationToken.None)
                          .ConfigureAwait(false);
        return false;
      }

      logger_.LogDebug("Trying to acquire task");
      taskData_ = await taskTable_.AcquireTask(messageHandler_.TaskId,
                                               ownerPodId_,
                                               cancellationToken)
                                  .ConfigureAwait(false);

      // empty OwnerPodId means that the task was not acquired because not ready
      if (taskData_.OwnerPodId == "")
      {
        messageHandler_.Status = QueueMessageStatus.Postponed;
        return false;
      }

      // we check if the task was acquired by this pod
      if (taskData_.OwnerPodId != ownerPodId_)
      {
        // if the task is acquired by another pod, we check if the task is running on the other pod
        var taskProcessingElsewhere = await taskProcessingChecker_.Check(taskData_.TaskId,
                                                                         taskData_.OwnerPodId,
                                                                         cancellationToken)
                                                                  .ConfigureAwait(false);

        logger_.LogInformation("Task {taskId} already acquired by {OtherOwnerPodId}, treating it {processing}",
                               taskData_.TaskId,
                               taskData_.OwnerPodId,
                               taskProcessingElsewhere);

        // if the task is not running on the other pod, we resubmit the task in the queue
        if (!taskProcessingElsewhere)
        {
          taskData_ = await taskTable_.ReadTaskAsync(messageHandler_.TaskId,
                                                     cancellationToken)
                                      .ConfigureAwait(false);
          if (taskData_.Status is TaskStatus.Processing or TaskStatus.Dispatched or TaskStatus.Processed)
          {
            logger_.LogDebug("Resubmitting task {task} on another pod",
                             taskData_.TaskId);
            await submitter_.CompleteTaskAsync(taskData_,
                                               true,
                                               new Output
                                               {
                                                 Error = new Output.Types.Error
                                                         {
                                                           Details = "Other pod seems to have crashed, resubmitting task",
                                                         },
                                               },
                                               CancellationToken.None)
                            .ConfigureAwait(false);
          }

          if (taskData_.Status is TaskStatus.Canceling)
          {
            messageHandler_.Status = QueueMessageStatus.Cancelled;
            await taskTable_.SetTaskCanceledAsync(messageHandler_.TaskId,
                                                  CancellationToken.None)
                            .ConfigureAwait(false);
            await resultTable_.AbortTaskResults(taskData_.SessionId,
                                                taskData_.TaskId,
                                                CancellationToken.None)
                              .ConfigureAwait(false);
            return false;
          }
        }

        // if the task is running elsewhere, then the message is duplicated so we remove it from the queue
        // and do not acquire the task
        messageHandler_.Status = QueueMessageStatus.Processed;
        return false;
      }

      logger_.LogInformation("Task preconditions are OK");
      return true;
    }
    catch (TaskNotFoundException e)
    {
      logger_.LogWarning(e,
                         "TaskId coming from message queue was not found, delete message from queue");
      messageHandler_.Status = QueueMessageStatus.Processed;
      return false;
    }
  }

  /// <summary>
  /// Get the task id of the acquired task
  /// </summary>
  /// <returns>
  /// A string representing the acquired task id or null if there is no task acquired
  /// </returns>
  public string GetAcquiredTask()
  {
    return taskData_ != null
             ? taskData_.TaskId
             : "";
  }

  /// <summary>
  /// Preprocessing (including the data prefetching) of the acquired task
  /// </summary>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// Task representing the asynchronous execution of the method
  /// </returns>
  /// <exception cref="NullReferenceException">wrong order of execution</exception>
  public async Task PreProcessing(CancellationToken cancellationToken)
  {
    logger_.LogDebug("Start prefetch data");
    using var _ = logger_.BeginNamedScope("PreProcessing",
                                          ("taskId", messageHandler_.TaskId));
    if (taskData_ == null)
    {
      throw new NullReferenceException();
    }

    computeRequestStream_ = await dataPrefetcher_.PrefetchDataAsync(taskData_,
                                                                    socketPath_,
                                                                    cancellationToken)
                                                 .ConfigureAwait(false);
  }

  /// <summary>
  /// Execution of the acquired task on the worker
  /// </summary>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// Task representing the asynchronous execution of the method
  /// </returns>
  /// <exception cref="NullReferenceException">wrong order of execution</exception>
  public async Task ExecuteTask(CancellationToken cancellationToken)
  {
    using var _ = logger_.BeginNamedScope("TaskExecution",
                                          ("taskId", messageHandler_.TaskId));
    if (computeRequestStream_ == null || taskData_ == null)
    {
      throw new NullReferenceException();
    }

    logger_.LogDebug("Create agent server to receive requests from worker");

    agent_ = new Agent(submitter_,
                           objectStorageFactory_,
                           taskData_.SessionId,
                           taskData_.TaskId,
                           logger_);

    await agentHandler_.Start(agent_,
                              cancellationToken)
                       .ConfigureAwait(false);

    logger_.LogInformation("Start processing task");
    await submitter_.StartTask(taskData_.TaskId,
                               cancellationToken)
                    .ConfigureAwait(false);

    workerStreamHandler_.StartTaskProcessing(taskData_,
                                             cancellationToken);
    if (workerStreamHandler_.Pipe is null)
    {
      throw new ArmoniKException($"{nameof(IWorkerStreamHandler.Pipe)} should not be null");
    }

    while (computeRequestStream_.TryDequeue(out var computeRequest))
    {
      await workerStreamHandler_.Pipe.WriteAsync(new ProcessRequest
                                                 {
                                                   Compute = computeRequest,
                                                 })
                                .ConfigureAwait(false);
    }
  }

  /// <summary>
  /// Post processing of the acquired task
  /// </summary>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// Task representing the asynchronous execution of the method
  /// </returns>
  /// <exception cref="NullReferenceException">wrong order of execution</exception>
  public async Task PostProcessing(CancellationToken cancellationToken)
  {
    using var _ = logger_.BeginNamedScope("PostProcessing",
                                          ("taskId", messageHandler_.TaskId));

    if (workerStreamHandler_.Pipe == null || taskData_ == null || agent_ == null)
    {
      throw new ArgumentNullException();
    }

    logger_.LogDebug("Waiting for task output");
    var reply = await workerStreamHandler_.Pipe.Read(cancellationToken)
                                          .ConfigureAwait(false);

    logger_.LogDebug("Stop agent server");
    await agentHandler_.Stop(CancellationToken.None)
                       .ConfigureAwait(false);

    logger_.LogInformation("Process task output of type {type}",
                           reply.Output.TypeCase);
    // at this point worker requests should have ended
    try
    {
      await workerStreamHandler_.Pipe.CompleteAsync()
                                .ConfigureAwait(false);

      if (reply.Output.TypeCase is Output.TypeOneofCase.Ok)
      {

        logger_.LogDebug("Complete processing of the request");
        await agent_.FinalizeTaskCreation(CancellationToken.None)
                    .ConfigureAwait(false);
      }

    }
    catch (RpcException e)
    {
      logger_.LogError(e,
                       "Error while computing task, retrying task");
      await submitter_.CompleteTaskAsync(taskData_,
                                         true,
                                         new Output
                                         {
                                           Error = new Output.Types.Error
                                                   {
                                                     Details = e.Message,
                                                   },
                                         },
                                         CancellationToken.None)
                      .ConfigureAwait(false);
      messageHandler_.Status = QueueMessageStatus.Cancelled;
      return;
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                         "Error while finalizing task processing");
      await submitter_.CompleteTaskAsync(taskData_,
                                         false,
                                         new Output
                                         {
                                           Error = new Output.Types.Error
                                                   {
                                                     Details = e.Message,
                                                   },
                                         },
                                         CancellationToken.None)
                      .ConfigureAwait(false);
      messageHandler_.Status = QueueMessageStatus.Processed;

      logger_.LogDebug("End Task Epilog");
      return;
    }

    await submitter_.CompleteTaskAsync(taskData_,
                                       false,
                                       reply.Output,
                                       CancellationToken.None)
                    .ConfigureAwait(false);
    messageHandler_.Status = QueueMessageStatus.Processed;

    logger_.LogDebug("End Task Processing");
  }


  /// <inheritdoc />
  public async ValueTask DisposeAsync()
  {
    using var _ = logger_.BeginNamedScope("DisposeAsync",
                                          ("taskId", messageHandler_.TaskId));

    logger_.LogDebug("MessageHandler status is {status}",
                     messageHandler_.Status);
    await messageHandler_.DisposeAsync()
                         .ConfigureAwait(false);

    agent_?.Dispose();
  }
}
