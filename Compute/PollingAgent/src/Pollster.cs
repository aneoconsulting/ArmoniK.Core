// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
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
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using ResultReply = ArmoniK.Api.gRPC.V1.ResultReply;
using TaskCanceledException = ArmoniK.Core.Common.Exceptions.TaskCanceledException;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;
using TimeoutException = ArmoniK.Core.Common.Exceptions.TimeoutException;

namespace ArmoniK.Core.Compute.PollingAgent;

public class Pollster
{
  private readonly WorkerClientProvider     workerClientProvider_;
  private readonly IHostApplicationLifetime lifeTime_;
  private readonly ILogger<Pollster>        logger_;
  private readonly int                      messageBatchSize_;
  private readonly IQueueStorage            queueStorage_;
  private readonly ITableStorage            tableStorage_;
  private readonly IObjectStorageFactory    objectStorageFactory_;

  public Pollster(ILogger<Pollster>        logger,
                  ComputePlan              options,
                  IQueueStorage            queueStorage,
                  ITableStorage            tableStorage,
                  IObjectStorageFactory    objectStorageFactory,
                  WorkerClientProvider     workerClientProvider,
                  IHostApplicationLifetime lifeTime)
  {
    if (options.MessageBatchSize < 1)
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"The minimum value for {nameof(ComputePlan.MessageBatchSize)} is 1.");

    logger_               = logger;
    queueStorage_         = queueStorage;
    tableStorage_         = tableStorage;
    objectStorageFactory_ = objectStorageFactory;
    workerClientProvider_ = workerClientProvider;
    lifeTime_             = lifeTime;
    messageBatchSize_     = options.MessageBatchSize;
  }

  private async Task Init(CancellationToken cancellationToken)
  {
    var client = workerClientProvider_.GetAsync();
    var queue  = queueStorage_.Init(cancellationToken);
    var table  = tableStorage_.Init(cancellationToken);
    var obj    = objectStorageFactory_.Init(cancellationToken);
    await client;
    await queue;
    await table;
    await obj;
  }

  public async Task MainLoop(CancellationToken cancellationToken)
  {
    await Init(cancellationToken);

    cancellationToken.Register(() => logger_.LogError("Global cancellation has been triggered."));
    try
    {
      logger_.LogFunction(functionName: $"{nameof(Pollster)}.{nameof(MainLoop)}.prefetchTask.WhileLoop");
      while (!cancellationToken.IsCancellationRequested)
      {
        logger_.LogInformation("Trying to fetch messages");

        logger_.LogFunction(functionName:
                            $"{nameof(Pollster)}.{nameof(MainLoop)}.prefetchTask.WhileLoop.{nameof(queueStorage_.PullAsync)}");
        var messages = queueStorage_.PullAsync(messageBatchSize_,
                                               cancellationToken);

        await foreach (var message in messages.WithCancellation(cancellationToken))
        {
          await using var msg = message;

          using var scopedLogger = logger_.BeginNamedScope("Prefetch messageHandler",
                                                           ("messageHandler", message.MessageId),
                                                           ("task", message.TaskId));
          logger_.LogDebug("Start a new Task to process the messageHandler");

          var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(message.CancellationToken,
                                                                            cancellationToken);
          try
          {
            logger_.LogDebug("Loading task data");
            var taskData = await tableStorage_.ReadTaskAsync(message.TaskId,
                                                             combinedCts.Token);

            var dispatchHandler = await CheckPreconditions(message,
                                                           taskData,
                                                           combinedCts,
                                                           cancellationToken);
            if (dispatchHandler is not null)
            {
              await using var _ = dispatchHandler;

              taskData = await PrefetchPayload(taskData,
                                               combinedCts.Token);

              logger_.LogDebug("Start a new Task to process the messageHandler");

              await ProcessTaskAsync(taskData,
                                     msg,
                                     combinedCts.Token,
                                     cancellationToken);

              logger_.LogDebug("Task returned");
            }
          }
          catch (Exception e)
          {
            logger_.LogWarning(e,
                               "Error with messageHandler {messageId}",
                               message.MessageId);
            throw;
          }
        }
      }

    }
    catch (Exception e)
    {
      logger_.LogCritical(e,
                          "Error in pollster");
    }

    lifeTime_.StopApplication();
  }
  
  public async Task<DispatchHandler?> CheckPreconditions(IQueueMessageHandler    messageHandler,
                                                         ITaskData               taskData,
                                                         CancellationTokenSource combinedCts,
                                                         CancellationToken       cancellationToken)
  {
    /*
     * Check preconditions:
     *  - Session is not cancelled
     *  - Task is not cancelled
     *  - Task status is OK
     *  - Dependencies have been checked
     *  - Max number of retries has not been reached
     */

    logger_.LogDebug("checking that the session is not cancelled");
    var isSessionCancelled = await tableStorage_.IsSessionCancelledAsync(new()
                                                                         {
                                                                           Session    = taskData.SessionId,
                                                                           ParentTaskId = taskData.ParentTaskId,
                                                                         },
                                                                         combinedCts.Token);

    if (isSessionCancelled &&
        taskData.Status is not (TaskStatus.Canceled or TaskStatus.Completed or TaskStatus.Error))
    {
      logger_.LogInformation("Task is being cancelled");

      messageHandler.Status = QueueMessageStatus.Cancelled;
      await tableStorage_.UpdateTaskStatusAsync(messageHandler.TaskId,
                                                TaskStatus.Canceled,
                                                cancellationToken);
      return null;
    }

    Task<bool> dependencyCheckTask;
    if (taskData.DataDependencies.Any())
      dependencyCheckTask = tableStorage_.AreResultsAvailableAsync(taskData.SessionId,
                                                                   taskData.DataDependencies,
                                                                   cancellationToken);
    else
      dependencyCheckTask = Task.FromResult(true);
    

    logger_.LogDebug("Handling the task status ({status})",
                     taskData.Status);
    switch (taskData.Status)
    {
      case TaskStatus.Canceling:
        logger_.LogInformation("Task is being cancelled");
        messageHandler.Status = QueueMessageStatus.Cancelled;
        await tableStorage_.UpdateTaskStatusAsync(messageHandler.TaskId,
                                                  TaskStatus.Canceled,
                                                  CancellationToken.None);
        return null;
      case TaskStatus.Completed:
        logger_.LogInformation("Task was already completed");
        messageHandler.Status = QueueMessageStatus.Processed;
        return null;
      case TaskStatus.Creating:
        break;
      case TaskStatus.Submitted:
        break;
      case TaskStatus.Dispatched:
        break;
      case TaskStatus.Error:
        logger_.LogInformation("Task was on error elsewhere ; retrying");
        break;
      case TaskStatus.Timeout:
        logger_.LogInformation("Task was timeout elsewhere ; taking over here");
        break;
      case TaskStatus.Canceled:
        logger_.LogInformation("Task has been cancelled");
        messageHandler.Status = QueueMessageStatus.Cancelled;
        return null;
      case TaskStatus.Processing:
        logger_.LogInformation("Task is processing elsewhere ; taking over here");
        break;
      case TaskStatus.Failed:
        logger_.LogInformation("Task is failed");
        messageHandler.Status = QueueMessageStatus.Poisonous;
        return null;
      default:
        logger_.LogCritical("Task was in an unknown state {state}",
                            taskData.Status);
        throw new ArgumentOutOfRangeException(nameof(taskData));
    }

    logger_.LogDebug("Changing task status to 'Dispatched'");
    var updateTask = tableStorage_.UpdateTaskStatusAsync(messageHandler.TaskId,
                                                         TaskStatus.Dispatched,
                                                         combinedCts.Token);

    if (!await dependencyCheckTask)
    {
      logger_.LogInformation("Dependencies are not complete yet.");
      messageHandler.Status = QueueMessageStatus.Postponed;
      await updateTask;
      return null;
    }



    logger_.LogDebug("checking that the number of retries is not greater than the max retry number");
    var dispatch = await tableStorage_.AcquireDispatchHandler($"{taskData.TaskId}-{DateTime.Now.Ticks}",
                                                        taskData.TaskId,
                                                        cancellationToken: cancellationToken);

    if (dispatch.Attempt >= taskData.Options.MaxRetries)
    {
      logger_.LogInformation("Task has been retried too many times");
      messageHandler.Status = QueueMessageStatus.Poisonous;
      await Task.WhenAll(tableStorage_.UpdateTaskStatusAsync(messageHandler.TaskId,
                                                             TaskStatus.Failed,
                                                             CancellationToken.None),
                         dispatch.DisposeAsync().AsTask(),
                         tableStorage_.DeleteDispatch(dispatch.Id,
                                                      cancellationToken));
      return null;
    }


    logger_.LogInformation("Task preconditions are OK");
    await updateTask;
    return dispatch;
  }

  private async Task ProcessTaskAsync(ITaskData          taskData,
                                      IQueueMessageHandler     messageHandler,
                                      CancellationToken combinedCt,
                                      CancellationToken cancellationToken)
  {
    using var _ = logger_.LogFunction(taskData.TaskId);
    /*
     * Compute Task
     */

    var request = new ProcessRequest.Types.ComputeRequest
    {
      Session    = taskData.SessionId,
      TaskId     = taskData.TaskId,
      Payload    = taskData.Payload,
      Dependencies =
      {
        taskData.Dependencies,
      },
    };
    request.TaskOptions.Add(taskData.Options.Options);

    logger_.LogDebug("Get client connection to the worker");
    var client = await workerClientProvider_.GetAsync();


    logger_.LogDebug("Set task status to Processing");
    var updateTask = tableStorage_.UpdateTaskStatusAsync(taskData.TaskId,
                                                         TaskStatus.Processing,
                                                         combinedCt);

    logger_.LogInformation("Send compute request to the worker");
    var call = client.ProcessAsync(request,
                                   deadline: DateTime.UtcNow +
                                             taskData.Options.MaxDuration.ToTimeSpan(),
                                   cancellationToken: CancellationToken.None);

    try
    {
      await updateTask;
      var result = await call.WrapRpcException();
      logger_.LogInformation("Compute finished successfully.");

      /*
       * Store Data
       */

      logger_.LogInformation("Sending result to storage.");
      await taskResultStorage_.AddOrUpdateAsync(taskData.TaskId,
                                                result,
                                                CancellationToken.None);
      logger_.LogInformation("Data sent.");
      messageHandler.Status = QueueMessageStatus.Processed;
      await Task.WhenAll(tableStorage_.UpdateTaskStatusAsync(taskData.TaskId,
                                                             TaskStatus.Completed,
                                                             CancellationToken.None));
    }
    catch (Exception e)
    {
      if (!await HandleExceptionAsync(e,
                                      taskData,
                                      messageHandler,
                                      cancellationToken))
        throw;
    }
  }

  private async Task<bool> HandleExceptionAsync(Exception e, ITaskData taskData, IQueueMessageHandler messageHandler, CancellationToken cancellationToken)
  {
    switch (e)
    {
      case TimeoutException:
      {
        logger_.LogError(e,
                         "Deadline exceeded when computing task {taskId} from session {sessionId}",
                         taskData.TaskId,
                         taskData.SessionId);
        messageHandler.Status = QueueMessageStatus.Failed;
        await tableStorage_.UpdateTaskStatusAsync(taskData.TaskId,
                                                  TaskStatus.Timeout,
                                                  CancellationToken.None);
        return true;
      }
      case TaskCanceledException:
      {
        var details = string.Empty;

        if (messageHandler.CancellationToken.IsCancellationRequested) details += "Message was cancelled. ";
        if (cancellationToken.IsCancellationRequested) details         += "Root token was cancelled. ";

        logger_.LogError(e,
                         "Execution has been cancelled for task {taskId} from session {sessionId}. {details}",
                         taskData.TaskId,
                         taskData.SessionId,
                         details);
        messageHandler.Status = QueueMessageStatus.Cancelled;
        await tableStorage_.UpdateTaskStatusAsync(taskData.TaskId,
                                                  TaskStatus.Canceling,
                                                  CancellationToken.None);
        return true;
      }
      case ArmoniKException:
      {
        logger_.LogError(e,
                         "Execution has failed for task {taskId} from session {sessionId}. {details}",
                         taskData.TaskId,
                         taskData.SessionId,
                         e.ToString());

        messageHandler.Status = QueueMessageStatus.Failed;
        await tableStorage_.UpdateTaskStatusAsync(taskData.TaskId,
                                                  TaskStatus.Error,
                                                  CancellationToken.None);
        return true;
      }
      case AggregateException ae:
      {
        foreach (var ie in ae.InnerExceptions)
          // If the exception was not handled, lazily allocate a list of unhandled
          // exceptions (to be rethrown later) and add it.
          if (!await HandleExceptionAsync(ie,
                                          taskData,
                                          messageHandler,
                                          cancellationToken))
            return false;

        return true;
      }
      default:
      {
        logger_.LogError(e,
                         "Exception encountered when computing task {taskId} from session {sessionId}",
                         taskData.TaskId,
                         taskData.SessionId);
        messageHandler.Status = QueueMessageStatus.Failed;
        await tableStorage_.UpdateTaskStatusAsync(taskData.TaskId,
                                                  TaskStatus.Error,
                                                  CancellationToken.None);
        Console.WriteLine(e);
        return false;
      }
    }
  }

  private async Task<ITaskData> PrefetchPayload(ITaskData taskData, CancellationToken combinedCt)
  {
    using var _ = logger_.LogFunction(taskData.TaskId);
    /*
     * Prefetch Data
     */

    if (!taskData.IsPayloadAvailable)
    {
      logger_.LogInformation("Start retrieving payload");
      var payload = await taskPayloadStorage_.TryGetValuesAsync(taskData.TaskId,
                                                                combinedCt);
      logger_.LogInformation("Payload retrieved");
      taskData.Payload            = payload;
      taskData.IsPayloadAvailable = true;
    }

    return taskData;
  }
}