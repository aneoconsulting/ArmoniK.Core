﻿// This file is part of the ArmoniK project
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
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.gRPC.V1;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using TaskCanceledException = ArmoniK.Core.Common.Exceptions.TaskCanceledException;
using TaskStatus = ArmoniK.Core.gRPC.V1.TaskStatus;
using TimeoutException = ArmoniK.Core.Common.Exceptions.TimeoutException;

namespace ArmoniK.Core.Compute.PollingAgent;

public class Pollster
{
  private readonly ClientServiceProvider                 clientProvider_;
  private readonly IHostApplicationLifetime              lifeTime_;
  private readonly ILogger<Pollster>                     logger_;
  private readonly int                                   messageBatchSize_;
  private readonly IQueueStorage                         queueStorage_;
  private readonly ITableStorage                         tableStorage_;
  private readonly KeyValueStorage<TaskId, Payload>      taskPayloadStorage_;
  private readonly KeyValueStorage<TaskId, ComputeReply> taskResultStorage_;

  public Pollster(ILogger<Pollster>                     logger,
                  ComputePlan                           options,
                  IQueueStorage                         queueStorage,
                  ITableStorage                         tableStorage,
                  KeyValueStorage<TaskId, ComputeReply> taskResultStorage,
                  KeyValueStorage<TaskId, Payload>      taskPayloadStorage,
                  ClientServiceProvider                 clientProvider,
                  IHostApplicationLifetime              lifeTime)
  {
    if (options.MessageBatchSize < 1)
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"The minimum value for {nameof(ComputePlan.MessageBatchSize)} is 1.");

    logger_             = logger;
    queueStorage_       = queueStorage;
    tableStorage_       = tableStorage;
    taskResultStorage_  = taskResultStorage;
    taskPayloadStorage_ = taskPayloadStorage;
    clientProvider_     = clientProvider;
    lifeTime_           = lifeTime;
    messageBatchSize_   = options.MessageBatchSize;
  }

  private async Task Init(CancellationToken cancellationToken)
  {
    var client      = clientProvider_.GetAsync();
    var queue       = queueStorage_.Init(cancellationToken);
    var table       = tableStorage_.Init(cancellationToken);
    var taskPayload = taskPayloadStorage_.Init(cancellationToken);
    var taskResult  = taskResultStorage_.Init(cancellationToken);
    await client;
    await queue;
    await table;
    await taskPayload;
    await taskResult;
  }

  public async Task MainLoop(CancellationToken cancellationToken)
  {
    await Init(cancellationToken);

    cancellationToken.Register(() => logger_.LogError("Global cancellation has been triggered."));
    try
    {
      var prefetchChannel =
        Channel.CreateBounded<(TaskData taskData, IQueueMessage message, CancellationToken combinedCT)>(new BoundedChannelOptions(1)
        {
          SingleReader                  = true,
          SingleWriter                  = true,
          FullMode                      = BoundedChannelFullMode.Wait,
          Capacity                      = 1,
          AllowSynchronousContinuations = false,
        });

      logger_.LogInformation("Prefetching loop started.");

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

            using var scopedLogger = logger_.BeginNamedScope("Prefetch message",
                                                             ("message", message.MessageId),
                                                             ("session", message.TaskId.Session),
                                                             ("task", message.TaskId.Task));
            logger_.LogDebug("Start a new Task to process the message");

            var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(message.CancellationToken,
                                                                              cancellationToken);
            try
            {
              logger_.LogDebug("Loading task data");
              var taskData = await tableStorage_.ReadTaskAsync(message.TaskId,
                                                               combinedCts.Token);

              if (await CheckPreconditions(message,
                                           taskData,
                                           combinedCts,
                                           cancellationToken))
              {
                taskData = await PrefetchPayload(taskData,
                                                 combinedCts.Token);

                logger_.LogDebug("Start a new Task to process the message");

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
                                 "Error with message {messageId}",
                                 message.MessageId);
              throw;
            }
          }
        }
      }
      finally
      {
        prefetchChannel.Writer.Complete();
      }
    }
    catch (Exception e)
    {
      logger_.LogCritical(e,
                          "Error in pollster");
    }

    lifeTime_.StopApplication();
  }

  public async Task MainLoopPrefetch(CancellationToken cancellationToken)
  {
    cancellationToken.Register(() => logger_.LogError("Global cancellation has been triggered."));
    try
    {
      var prefetchChannel =
        Channel.CreateBounded<(TaskData taskData, IQueueMessage message, CancellationToken combinedCT)>(new BoundedChannelOptions(1)
        {
          SingleReader                  = true,
          SingleWriter                  = true,
          FullMode                      = BoundedChannelFullMode.Wait,
          Capacity                      = 1,
          AllowSynchronousContinuations = false,
        });

      var prefetchTask = Task<Task>.Factory.StartNew(async () =>
                                                     {
                                                       logger_.LogInformation("Prefetching loop started.");

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
                                                             var dispose = true;

                                                             using var scopedLogger = logger_.BeginNamedScope("Prefetch message",
                                                                                                              ("message", message.MessageId),
                                                                                                              ("session", message.TaskId.Session),
                                                                                                              ("task", message.TaskId.Task));
                                                             logger_.LogDebug("Start a new Task to process the message");

                                                             var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(message.CancellationToken,
                                                                                                                               cancellationToken);
                                                             try
                                                             {
                                                               logger_.LogDebug("Loading task data");
                                                               var taskData = await tableStorage_.ReadTaskAsync(message.TaskId,
                                                                                                                combinedCts.Token);

                                                               if (await CheckPreconditions(message,
                                                                                            taskData,
                                                                                            combinedCts,
                                                                                            cancellationToken))
                                                               {
                                                                 taskData = await PrefetchPayload(taskData,
                                                                                                  combinedCts.Token);

                                                                 await prefetchChannel.Writer.WaitToWriteAsync(combinedCts.Token);
                                                                 await prefetchChannel.Writer.WriteAsync((taskData, message, combinedCts.Token),
                                                                                                         combinedCts.Token);
                                                                 dispose = false;
                                                               }
                                                             }
                                                             catch (Exception e)
                                                             {
                                                               logger_.LogWarning(e,
                                                                                  "Error while prefetching message");
                                                               throw;
                                                             }
                                                             finally
                                                             {
                                                               if (dispose)
                                                                 await message.DisposeAsync();
                                                             }
                                                           }
                                                         }
                                                       }
                                                       catch (Exception e)
                                                       {
                                                         logger_.LogWarning(e,
                                                                            "Error while prefetching task");
                                                         throw;
                                                       }
                                                       finally
                                                       {
                                                         prefetchChannel.Writer.Complete();
                                                       }
                                                     },
                                                     cancellationToken,
                                                     TaskCreationOptions.LongRunning,
                                                     TaskScheduler.Current)
                                   .Unwrap();


      logger_.LogInformation("Processing loop started.");
      while (!cancellationToken.IsCancellationRequested)
        await foreach (var (taskData, message, combinedCt) in prefetchChannel.Reader.ReadAllAsync(cancellationToken).WithCancellation(cancellationToken))
        {
          await using var msg = message;
          using var scopedLogger = logger_.BeginNamedScope("Process message",
                                                           ("message", msg.MessageId),
                                                           ("session", msg.TaskId.Session),
                                                           ("task", msg.TaskId.Task));
          logger_.LogDebug("Start a new Task to process the message");

          try
          {
            await ProcessTaskAsync(taskData,
                                   msg,
                                   combinedCt,
                                   cancellationToken);
          }
          catch (Exception e)
          {
            logger_.LogWarning(e,
                               "Error while processing message");
            throw;
          }

          logger_.LogDebug("Task returned");
        }

      await prefetchTask;
    }
    catch (Exception e)
    {
      logger_.LogCritical(e,
                          "Error in pollster");
    }

    lifeTime_.StopApplication();
  }

  private async Task<bool> CheckPreconditions(IQueueMessage           message,
                                              TaskData                taskData,
                                              CancellationTokenSource combinedCts,
                                              CancellationToken       cancellationToken)
  {
    /*
     * Check preconditions:
     *  - Session is not cancelled
     *  - Task is not cancelled
     *  - Max number of retries has not been reached
     *  - Task status is OK
     *  - Dependencies have been checked
     */

    logger_.LogDebug("checking that the session is not cancelled");
    var isSessionCancelled = await tableStorage_.IsSessionCancelledAsync(new()
                                                                         {
                                                                           Session    = message.TaskId.Session,
                                                                           SubSession = message.TaskId.SubSession,
                                                                         },
                                                                         combinedCts.Token);

    if (isSessionCancelled &&
        taskData.Status is not (TaskStatus.Canceled or TaskStatus.Completed or TaskStatus.WaitingForChildren))
    {
      logger_.LogInformation("Task is being cancelled");
      // cannot get lease: task is already running elsewhere
      message.Status = QueueMessageStatus.Cancelled;
      await tableStorage_.UpdateTaskStatusAsync(message.TaskId,
                                                TaskStatus.Canceled,
                                                cancellationToken);
      return true;
    }

    Task<int> dependencyCheckTask;
    if (taskData.Dependencies.Any())
      dependencyCheckTask = tableStorage_.CountSubTasksAsync(new()
                                                             {
                                                               SessionId    = taskData.Id.Session,
                                                               SubSessionId = taskData.Id.SubSession,
                                                               IncludedTaskIds =
                                                               {
                                                                 taskData.Dependencies,
                                                               },
                                                               ExcludedStatuses =
                                                               {
                                                                 TaskStatus.Completed,
                                                               },
                                                             },
                                                             combinedCts.Token)
                                         .ContinueWith(task => task.Result
                                                                   .Where(tuple => tuple.Status != TaskStatus.Completed)
                                                                   .Sum(tuple => tuple.Count),
                                                       cancellationToken);
    else
      dependencyCheckTask = Task.FromResult(0);


    logger_.LogDebug("checking that the number of retries is not greater than the max retry number");
    if (taskData.Retries >= taskData.Options.MaxRetries)
    {
      logger_.LogInformation("Task has been retried too many times");
      message.Status = QueueMessageStatus.Poisonous;
      await tableStorage_.UpdateTaskStatusAsync(message.TaskId,
                                                TaskStatus.Failed,
                                                CancellationToken.None);
      return false;
    }


    logger_.LogDebug("Handling the task status ({status})",
                     taskData.Status);
    switch (taskData.Status)
    {
      case TaskStatus.Canceling:
        logger_.LogInformation("Task is being cancelled");
        message.Status = QueueMessageStatus.Cancelled;
        await tableStorage_.UpdateTaskStatusAsync(message.TaskId,
                                                  TaskStatus.Canceled,
                                                  CancellationToken.None);
        return false;
      case TaskStatus.Completed:
        logger_.LogInformation("Task was already completed");
        message.Status = QueueMessageStatus.Processed;
        return false;
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
        message.Status = QueueMessageStatus.Cancelled;
        return false;
      case TaskStatus.Processing:
        logger_.LogInformation("Task is processing elsewhere ; taking over here");
        break;
      case TaskStatus.WaitingForChildren:
        logger_.LogInformation("Task was already processed and is waiting for children");
        return false;
      case TaskStatus.Failed:
        logger_.LogInformation("Task is failed");
        message.Status = QueueMessageStatus.Poisonous;
        break;
      default:
        logger_.LogCritical("Task was in an unknown state {state}",
                            taskData.Status);
        throw new ArgumentOutOfRangeException(nameof(taskData));
    }

    logger_.LogDebug("Changing task status to 'Dispatched'");
    var updateTask = tableStorage_.UpdateTaskStatusAsync(message.TaskId,
                                                         TaskStatus.Dispatched,
                                                         combinedCts.Token);

    if (await dependencyCheckTask > 0)
    {
      logger_.LogInformation("Dependencies are not complete yet.");
      message.Status = QueueMessageStatus.Postponed;
      await updateTask;
      return false;
    }


    logger_.LogInformation("Task preconditions are OK");
    await updateTask;
    return true;
  }

  private async Task ProcessTaskAsync(TaskData          taskData,
                                      IQueueMessage     message,
                                      CancellationToken combinedCt,
                                      CancellationToken cancellationToken)
  {
    using var _ = logger_.LogFunction(taskData.Id.ToPrintableId());
    /*
     * Compute Task
     */

    logger_.LogInformation("Increasing the retry counter");
    var increaseTask = tableStorage_.IncreaseRetryCounterAsync(taskData.Id,
                                                               CancellationToken.None);

    var request = new ComputeRequest
    {
      Session    = taskData.Id.Session,
      Subsession = taskData.Id.SubSession,
      TaskId     = taskData.Id.Task,
      Payload    = taskData.Payload.Data,
      Dependencies =
      {
        taskData.Dependencies,
      },
    };
    request.TaskOptions.Add(taskData.Options.Options);

    logger_.LogDebug("Get client connection to the worker");
    var client = await clientProvider_.GetAsync();


    logger_.LogDebug("Set task status to Processing");
    var updateTask = tableStorage_.UpdateTaskStatusAsync(taskData.Id,
                                                         TaskStatus.Processing,
                                                         combinedCt);

    logger_.LogInformation("Send compute request to the worker");
    var call = client.ExecuteAsync(request,
                                   deadline: DateTime.UtcNow +
                                             taskData.Options.MaxDuration.ToTimeSpan(),
                                   cancellationToken: CancellationToken.None);

    try
    {
      await updateTask;
      var result = await call.WrapRpcException();
      logger_.LogInformation("Compute finished successfully.");

      /*
       * Store Result
       */

      logger_.LogInformation("Sending result to storage.");
      await taskResultStorage_.AddOrUpdateAsync(taskData.Id,
                                                result,
                                                CancellationToken.None);
      logger_.LogInformation("Result sent.");
      message.Status = QueueMessageStatus.Processed;
      await Task.WhenAll(tableStorage_.UpdateTaskStatusAsync(taskData.Id,
                                                             TaskStatus.Completed,
                                                             CancellationToken.None),
                         increaseTask);
    }
    catch (Exception e)
    {
      if (!await HandleExceptionAsync(e,
                                      taskData,
                                      message,
                                      cancellationToken))
        throw;
    }
  }

  private async Task<bool> HandleExceptionAsync(Exception e, TaskData taskData, IQueueMessage message, CancellationToken cancellationToken)
  {
    switch (e)
    {
      case TimeoutException:
      {
        logger_.LogError(e,
                         "Deadline exceeded when computing task {taskId} from session {sessionId}",
                         taskData.Id.Task,
                         taskData.Id.Session);
        message.Status = QueueMessageStatus.Failed;
        await tableStorage_.UpdateTaskStatusAsync(taskData.Id,
                                                  TaskStatus.Timeout,
                                                  CancellationToken.None);
        return true;
      }
      case TaskCanceledException:
      {
        var details = string.Empty;

        if (message.CancellationToken.IsCancellationRequested) details += "Message was cancelled. ";
        if (cancellationToken.IsCancellationRequested) details         += "Root token was cancelled. ";

        logger_.LogError(e,
                         "Execution has been cancelled for task {taskId} from session {sessionId}. {details}",
                         taskData.Id.Task,
                         taskData.Id.Session,
                         details);
        message.Status = QueueMessageStatus.Cancelled;
        await tableStorage_.UpdateTaskStatusAsync(taskData.Id,
                                                  TaskStatus.Canceling,
                                                  CancellationToken.None);
        return true;
      }
      case ArmoniKException:
      {
        logger_.LogError(e,
                         "Execution has failed for task {taskId} from session {sessionId}. {details}",
                         taskData.Id.Task,
                         taskData.Id.Session,
                         e.ToString());

        message.Status = QueueMessageStatus.Failed;
        await tableStorage_.UpdateTaskStatusAsync(taskData.Id,
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
                                          message,
                                          cancellationToken))
            return false;

        return true;
      }
      default:
      {
        logger_.LogError(e,
                         "Exception encountered when computing task {taskId} from session {sessionId}",
                         taskData.Id.Task,
                         taskData.Id.Session);
        message.Status = QueueMessageStatus.Failed;
        await tableStorage_.UpdateTaskStatusAsync(taskData.Id,
                                                  TaskStatus.Error,
                                                  CancellationToken.None);
        Console.WriteLine(e);
        return false;
      }
    }
  }

  private async Task<TaskData> PrefetchPayload(TaskData taskData, CancellationToken combinedCt)
  {
    using var _ = logger_.LogFunction(taskData.Id.ToPrintableId());
    /*
     * Prefetch Data
     */

    if (!taskData.IsPayloadAvailable)
    {
      logger_.LogInformation("Start retrieving payload");
      var payload = await taskPayloadStorage_.GetValuesAsync(taskData.Id,
                                                                combinedCt);
      logger_.LogInformation("Payload retrieved");
      taskData.Payload            = payload;
      taskData.IsPayloadAvailable = true;
    }

    return taskData;
  }
}