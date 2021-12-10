// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Linq;
using System.Net.Mime;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Core;
using ArmoniK.Core.Exceptions;
using ArmoniK.Core.gRPC;
using ArmoniK.Core.gRPC.V1;
using ArmoniK.Core.Injection.Options;
using ArmoniK.Core.Storage;
using ArmoniK.Core.Utils;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using TaskCanceledException = ArmoniK.Core.Exceptions.TaskCanceledException;
using TaskStatus = ArmoniK.Core.gRPC.V1.TaskStatus;
using TimeoutException = ArmoniK.Core.Exceptions.TimeoutException;

namespace ArmoniK.Compute.PollingAgent
{
  public class Pollster
  {
    private readonly ILogger<Pollster>                     logger_;
    private readonly IQueueStorage                         queueStorage_;
    private readonly ITableStorage                         tableStorage_;
    private readonly KeyValueStorage<TaskId, ComputeReply> taskResultStorage_;
    private readonly KeyValueStorage<TaskId, Payload>      taskPayloadStorage_;
    private readonly ClientServiceProvider                 clientProvider_;
    private readonly IHostApplicationLifetime              lifeTime_;

    public Pollster(ILogger<Pollster> logger,
                    // ReSharper disable once UnusedParameter.Local
                    IOptions<ComputePlan>                 options,
                    IQueueStorage                         queueStorage,
                    ITableStorage                         tableStorage,
                    KeyValueStorage<TaskId, ComputeReply> taskResultStorage,
                    KeyValueStorage<TaskId, Payload>      taskPayloadStorage,
                    ClientServiceProvider                 clientProvider,
                    IHostApplicationLifetime              lifeTime)
    {
      logger_             = logger;
      queueStorage_       = queueStorage;
      tableStorage_       = tableStorage;
      taskResultStorage_  = taskResultStorage;
      taskPayloadStorage_ = taskPayloadStorage;
      clientProvider_     = clientProvider;
      lifeTime_           = lifeTime;
    }

    public async Task MainLoop(CancellationToken cancellationToken)
    {
      cancellationToken.Register(() => logger_.LogError("Global cancellation has been triggered."));
      try
      {

        var prefetchChannel = Channel.CreateBounded<(TaskData taskData, IQueueMessage message, CancellationToken combinedCT)>(new BoundedChannelOptions(1)
                                                                                                                              {
                                                                                                                                SingleReader = true,
                                                                                                                                SingleWriter = true,
                                                                                                                                FullMode = BoundedChannelFullMode.Wait,
                                                                                                                                Capacity = 1,
                                                                                                                                AllowSynchronousContinuations = false,
                                                                                                                              });

        var prefetchTask = Task<Task>.Factory.StartNew(async () =>
                                                       {
                                                         logger_.LogInformation("Prefetching loop started.");

                                                         try
                                                         {
                                                           while (!cancellationToken.IsCancellationRequested)
                                                           {
                                                             logger_.LogInformation("Trying to fetch messages");
                                                             var messages = queueStorage_.PullAsync(1,
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
        {
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

    private async Task<bool> CheckPreconditions(IQueueMessage            message,
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
      var isSessionCancelled = await tableStorage_.IsSessionCancelledAsync(new SessionId
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

      var dependencyCheckTask = taskData.Options.Dependencies.AsParallel().Select(tid => IsDependencyCompleted(tid,
                                                                                                               combinedCts.Token)).WhenAll();


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
        case TaskStatus.Failed:
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
        default:
          logger_.LogCritical("Task was in an unknown state {state}",
                              taskData.Status);
          throw new ArgumentOutOfRangeException(nameof(taskData));
      }

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

      logger_.LogDebug("Changing task status to 'Dispatched'");
      var updateTask = tableStorage_.UpdateTaskStatusAsync(message.TaskId,
                                                           TaskStatus.Dispatched,
                                                           combinedCts.Token);

      if (!(await dependencyCheckTask).All(b => b))
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

    private Task<bool> IsDependencyCompleted(TaskId tid, CancellationToken cancellationToken)
    {
      var signal = new[] { false };
      return IsDependencyCompleted(tid,
                                   signal,
                                   cancellationToken);
    }

    private async Task<bool> IsDependencyCompleted(TaskId tid, bool[] signal, CancellationToken cancellationToken)
    {
      if (signal[0]) return false;

      logger_.LogDebug("Checking status for dependency with taskId {tid}",
                       tid.ToPrintableId());
      var tData = await tableStorage_.ReadTaskAsync(tid,
                                                    cancellationToken);

      if (tData.Status != TaskStatus.Completed)
      {
        signal[0] = true;
        return false;
      }

      return await tData.Options.Dependencies.AsParallel()
                        .Select(dependency => IsDependencyCompleted(dependency,
                                                                    signal,
                                                                    cancellationToken))
                        .ToAsyncEnumerable()
                        .AllAwaitAsync(async b => await b,
                                       cancellationToken);
    }

    private async Task ProcessTaskAsync(TaskData taskData,
                                        IQueueMessage message,
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
                    };

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

      ComputeReply result;
      try
      {
        await updateTask;
        result = await call.WrapRpcException();
      }
      catch (TimeoutException e)
      {
        logger_.LogError(e,
                         "Deadline exceeded when computing task {taskId} from session {sessionId}",
                         taskData.Id.Task,
                         taskData.Id.Session);
        message.Status = QueueMessageStatus.Failed;
        await tableStorage_.UpdateTaskStatusAsync(taskData.Id,
                                                  TaskStatus.Timeout,
                                                  CancellationToken.None);
        return;
      }
      catch (TaskCanceledException e)
      {
        var details = string.Empty;

        if (message.CancellationToken.IsCancellationRequested) details      += "Message was cancelled. ";
        if (cancellationToken.IsCancellationRequested) details += "Root token was cancelled. ";

        logger_.LogError(e,
                         "Execution has been cancelled for task {taskId} from session {sessionId}. {details}",
                         taskData.Id.Task,
                         taskData.Id.Session,
                         details);
        message.Status = QueueMessageStatus.Cancelled;
        await tableStorage_.UpdateTaskStatusAsync(taskData.Id,
                                                  TaskStatus.Canceling,
                                                  CancellationToken.None);
        return;
      }
      catch (ArmoniKException e)
      {
        logger_.LogError(e,
                         "Execution has failed for task {taskId} from session {sessionId}. {details}",
                         taskData.Id.Task,
                         taskData.Id.Session,
                         e.ToString());

        message.Status = QueueMessageStatus.Failed;
        await tableStorage_.UpdateTaskStatusAsync(taskData.Id,
                                                  TaskStatus.Failed,
                                                  CancellationToken.None);
        return;
      }
      catch (Exception e)
      {
        logger_.LogError(e,
                         "Exception encountered when computing task {taskId} from session {sessionId}",
                         taskData.Id.Task,
                         taskData.Id.Session);
        message.Status = QueueMessageStatus.Failed;
        await tableStorage_.UpdateTaskStatusAsync(taskData.Id,
                                                  TaskStatus.Failed,
                                                  CancellationToken.None);
        Console.WriteLine(e);
        throw;
      }

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

    private async Task<TaskData> PrefetchPayload(TaskData taskData, CancellationToken combinedCt)
    {
      using var _ = logger_.LogFunction(taskData.Id.ToPrintableId());
      /*
       * Prefetch Data
       */

      if (!taskData.HasPayload)
      {
      logger_.LogInformation("Start retrieving payload");
        var payload = await taskPayloadStorage_.TryGetValuesAsync(taskData.Id,
                                                                  combinedCt);
      logger_.LogInformation("Payload retrieved");
        taskData.Payload    = payload;
        taskData.HasPayload = true;

      }
      return taskData;
    }
  }
}