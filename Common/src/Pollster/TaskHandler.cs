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
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Stream.Worker;

using Microsoft.Extensions.Logging;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Pollster;

internal class TaskHandler : IAsyncDisposable
{
  private readonly ISessionTable                               sessionTable_;
  private readonly ITaskTable                                  taskTable_;
  private readonly IResultTable                                resultTable_;
  private readonly IQueueMessageHandler                        messageHandler_;
  private readonly ISubmitter                                  submitter_;
  private readonly DataPrefetcher                              dataPrefetcher_;
  private readonly IWorkerStreamHandler                        workerStreamHandler_;
  private readonly IObjectStorageFactory                       objectStorageFactory_;
  private readonly ActivitySource                              activitySource_;
  private readonly ILogger                                     logger_;
  private          TaskData?                                   taskData_;
  private          Queue<ProcessRequest.Types.ComputeRequest>? computeRequestStream_;
  private          Task?                                       processResult_;

  public TaskHandler(ISessionTable         sessionTable,
                     ITaskTable            taskTable,
                     IResultTable          resultTable,
                     ISubmitter            submitter,
                     DataPrefetcher        dataPrefetcher,
                     IWorkerStreamHandler  workerStreamHandler,
                     IObjectStorageFactory objectStorageFactory,
                     IQueueMessageHandler  messageHandler,
                     ActivitySource        activitySource,
                     ILogger               logger)
  {
    sessionTable_         = sessionTable;
    taskTable_            = taskTable;
    resultTable_          = resultTable;
    messageHandler_       = messageHandler;
    submitter_            = submitter;
    dataPrefetcher_       = dataPrefetcher;
    workerStreamHandler_  = workerStreamHandler;
    objectStorageFactory_ = objectStorageFactory;
    activitySource_       = activitySource;
    logger_               = logger;
    taskData_             = null;
  }

  public async Task<bool> AcquireTask(CancellationToken cancellationToken)
  {
    logger_.LogInformation("Acquiring task");
    using var activity = activitySource_.StartActivity($"{nameof(AcquireTask)}");

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
          await taskTable_.UpdateTaskStatusAsync(messageHandler_.TaskId,
                                                 TaskStatus.Canceled,
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
          return false;
        case TaskStatus.Timeout:
          logger_.LogInformation("Task was timeout elsewhere ; taking over here");
          break;
        case TaskStatus.Canceled:
          logger_.LogInformation("Task has been cancelled");
          messageHandler_.Status = QueueMessageStatus.Cancelled;
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

      var dependencyCheckTask = taskData_.DataDependencies.Any()
                                  ? resultTable_.AreResultsAvailableAsync(taskData_.SessionId,
                                                                          taskData_.DataDependencies,
                                                                          cancellationToken)
                                  : Task.FromResult(true);

      var isSessionCancelled = await sessionTable_.IsSessionCancelledAsync(taskData_.SessionId,
                                                                           cancellationToken)
                                                  .ConfigureAwait(false);

      if (isSessionCancelled && taskData_.Status is not (TaskStatus.Canceled or TaskStatus.Completed or TaskStatus.Error))
      {
        logger_.LogInformation("Task is being cancelled");

        messageHandler_.Status = QueueMessageStatus.Cancelled;
        await taskTable_.UpdateTaskStatusAsync(messageHandler_.TaskId,
                                               TaskStatus.Canceled,
                                               cancellationToken)
                        .ConfigureAwait(false);
        return false;
      }

      if (!await dependencyCheckTask.ConfigureAwait(false))
      {
        logger_.LogDebug("Dependencies are not complete yet.");
        messageHandler_.Status = QueueMessageStatus.Postponed;
        return false;
      }

      logger_.LogDebug("checking that the number of retries is not greater than the max retry number");
      var acquireTask = await taskTable_.AcquireTask(messageHandler_.TaskId,
                                                     cancellationToken)
                                        .ConfigureAwait(false);
      if (!acquireTask)
      {
        messageHandler_.Status = QueueMessageStatus.Postponed;
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

  public async Task PreProcessing(CancellationToken cancellationToken)
  {
    logger_.LogDebug("Start prefetch data");
    if (taskData_ == null)
    {
      throw new NullReferenceException();
    }

    computeRequestStream_ = await dataPrefetcher_.PrefetchDataAsync(taskData_,
                                                                    cancellationToken)
                                                 .ConfigureAwait(false);
  }

  public async Task ExecuteTask(CancellationToken cancellationToken)
  {
    if (computeRequestStream_ == null || taskData_ == null)
    {
      throw new NullReferenceException();
    }

    logger_.LogDebug("Start a new Task to process the messageHandler");
    using var requestProcessor = new RequestProcessor(workerStreamHandler_,
                                                      objectStorageFactory_,
                                                      logger_,
                                                      submitter_,
                                                      activitySource_);

    processResult_ = await requestProcessor.ProcessAsync(messageHandler_,
                                                         taskData_,
                                                         computeRequestStream_,
                                                         cancellationToken)
                                           .ConfigureAwait(false);
  }

  public async Task PostProcessing(CancellationToken cancellationToken)
  {
    if (processResult_ == null)
    {
      throw new NullReferenceException();
    }

    await processResult_.WaitAsync(cancellationToken)
                        .ConfigureAwait(false);
  }

  public async ValueTask DisposeAsync()
  {
    await messageHandler_.DisposeAsync()
                         .ConfigureAwait(false);
  }
}
