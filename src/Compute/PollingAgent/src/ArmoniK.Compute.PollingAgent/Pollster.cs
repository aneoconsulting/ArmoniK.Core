// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core;
using ArmoniK.Core.Exceptions;
using ArmoniK.Core.gRPC;
using ArmoniK.Core.gRPC.V1;
using ArmoniK.Core.Injection.Options;
using ArmoniK.Core.Storage;
using ArmoniK.Core.Utils;

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
    private readonly int                                   messageBatchSize_;
    private readonly IQueueStorage                         queueStorage_;
    private readonly ITableStorage                         tableStorage_;
    private readonly KeyValueStorage<TaskId, ComputeReply> taskResultStorage_;
    private readonly KeyValueStorage<TaskId, Payload>      taskPayloadStorage_;
    private readonly ClientServiceProvider                 clientProvider_;

    public Pollster(ILogger<Pollster>                     logger,
                    IOptions<ComputePlan>                 options,
                    IQueueStorage                         queueStorage,
                    ITableStorage                         tableStorage,
                    KeyValueStorage<TaskId, ComputeReply> taskResultStorage,
                    KeyValueStorage<TaskId, Payload>      taskPayloadStorage,
                    ClientServiceProvider                 clientProvider)
    {
      logger_             = logger;
      messageBatchSize_   = options.Value.MessageBatchSize;
      queueStorage_       = queueStorage;
      tableStorage_       = tableStorage;
      taskResultStorage_  = taskResultStorage;
      taskPayloadStorage_ = taskPayloadStorage;
      clientProvider_     = clientProvider;
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage",
                                                     "CA2208:Instantiate argument exceptions correctly",
                                                     Justification = "<Pending>")]
    public async Task MainLoop(CancellationToken cancellationToken)
    {
      logger_.LogInformation("Main loop started.");
      while (!cancellationToken.IsCancellationRequested)
      {
        QueueMessage message = null;
        do
        {
          logger_.LogInformation("Trying to fetch messages");
          message = await queueStorage_.PullAsync(1, cancellationToken)
                                       .FirstOrDefaultAsync(cancellationToken);
        } while (message is null);

        await Task.Factory.StartNew(() => ProcessMessageAsync(
                                                              message,
                                                              cancellationToken
                                                             ),
                                    cancellationToken,
                                    TaskCreationOptions.None,
                                    TaskScheduler.Current);

      }
    }

    private async Task ProcessMessageAsync(QueueMessage message, CancellationToken cancellationToken)
    {
      await using var msg = message;
      using var scopedLogger = logger_.BeginNamedScope("Message",
                                                       ("message", message.MessageId),
                                                       ("session", message.TaskId.Session),
                                                       ("task", message.TaskId.Task));
      logger_.LogInformation(LogEvents.StartMessageProcessing,
                             "Message acquired.");

      var combinedCTS = CancellationTokenSource.CreateLinkedTokenSource(message.CancellationToken,
                                                                        cancellationToken);

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
                                                                           combinedCTS.Token);

      logger_.LogDebug("Loading task data");
      var taskData = await tableStorage_.ReadTaskAsync(message.TaskId,
                                                       combinedCTS.Token);

      if (isSessionCancelled &&
          taskData.Status is not (TaskStatus.Canceled or TaskStatus.Completed or TaskStatus.WaitingForChildren))
      {
        logger_.LogInformation("Task is being cancelled");
        // cannot get lease: task is already running elsewhere
        await queueStorage_.MessageProcessedAsync(message.MessageId,
                                                  cancellationToken);
        await tableStorage_.UpdateTaskStatusAsync(message.TaskId,
                                                  TaskStatus.Canceled,
                                                  cancellationToken);
        return;
      }

      var dependencyCheckTask = taskData.Options.Dependencies.AsParallel().Select(tid => IsDependencyCompleted(tid,
                                                                                                               combinedCTS.Token)).WhenAll();

      logger_.LogDebug("checking that the number of retries is not greater than the max retry number");
      if (taskData.Retries >= taskData.Options.MaxRetries)
      {
        logger_.LogInformation("Task has been retried too many times");
        await Task.WhenAll(tableStorage_.UpdateTaskStatusAsync(message.TaskId,
                                                               TaskStatus.Failed,
                                                               CancellationToken.None),
                           queueStorage_.MessageRejectedAsync(message.MessageId,
                                                              CancellationToken.None));
        return;
      }

      logger_.LogDebug("Handling the task status ({status})",
                       taskData.Status);
      switch (taskData.Status)
      {
        case TaskStatus.Canceling:
          logger_.LogInformation("Task is being cancelled");
          await Task.WhenAll(tableStorage_.UpdateTaskStatusAsync(message.TaskId,
                                                                 TaskStatus.Canceled,
                                                                 CancellationToken.None),
                             queueStorage_.MessageProcessedAsync(message.MessageId,
                                                                 CancellationToken.None));
          return;
        case TaskStatus.Completed:
          logger_.LogInformation("Task was already completed");
          await queueStorage_.MessageProcessedAsync(message.MessageId,
                                                    cancellationToken);
          return;
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
          await queueStorage_.MessageProcessedAsync(message.MessageId,
                                                    cancellationToken);
          return;
        case TaskStatus.Processing:
          logger_.LogInformation("Task is processing elsewhere ; taking over here");
          break;
        case TaskStatus.WaitingForChildren:
          logger_.LogInformation("Task was already processed and is waiting for children");
          return;
        default:
          logger_.LogCritical("Task was in an unknown state {state}",
                              taskData.Status);
          throw new ArgumentOutOfRangeException(nameof(taskData.Status));
      }

      logger_.LogDebug("Changing task status to 'Dispatched'");
      var updateTask = tableStorage_.UpdateTaskStatusAsync(message.TaskId,
                                                           TaskStatus.Dispatched,
                                                           combinedCTS.Token);

      if (!(await dependencyCheckTask).All(b => b))
      {
        logger_.LogInformation("Dependencies are not complete yet.");
        await queueStorage_.RequeueMessageAsync(message.MessageId,
                                                cancellationToken);
        await updateTask;
        return;
      }


      logger_.LogInformation("Task preconditions are OK");
      await updateTask;

      await PrefetchAndCompute(taskData,
                               message.MessageId,
                               message.CancellationToken,
                               cancellationToken);
    }

    private Task<bool> IsDependencyCompleted(TaskId tid, CancellationToken cancellationToken)
    {
      var signal = new [] { false };
      return IsDependencyCompleted(tid, signal, cancellationToken);
    }

    private async Task<bool> IsDependencyCompleted(TaskId tid, bool[] signal, CancellationToken cancellationToken)
    {
      if (signal[0]) return false;

      logger_.LogDebug("Checking status for dependency with taskId {tid}", tid.ToPrintableId());
      var tData = await tableStorage_.ReadTaskAsync(tid, cancellationToken);

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
                        .AllAwaitAsync(async b => await b, cancellationToken);
    }

    private async Task PrefetchAndCompute(TaskData          taskData,
                                          string messageId,
                                          CancellationToken messageToken,
                                          CancellationToken cancellationToken)
    {
      using var _ = logger_.LogFunction();

      var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(messageToken, cancellationToken);
      /*
         * Prefetch Data
         */


      logger_.LogInformation("Start retrieving payload");
      var payloadTask = taskData.HasPayload
        ? ValueTask.FromResult(taskData.Payload)
        : new ValueTask<Payload>(taskPayloadStorage_.TryGetValuesAsync(taskData.Id, combinedCts.Token));

      var payload = await payloadTask;

      logger_.LogInformation("Payload retrieved");

      /*
         * Compute Task
         */

      logger_.LogInformation("Increasing the retry counter");
      var increaseTask = tableStorage_.IncreaseRetryCounterAsync(taskData.Id, CancellationToken.None);

      var request = new ComputeRequest
      {
        Session    = taskData.Id.Session,
        Subsession = taskData.Id.SubSession,
        TaskId     = taskData.Id.Task,
        Payload    = payload.Data,
      };

      logger_.LogDebug("Get client connection to the worker");
      var client = await clientProvider_.GetAsync();

      logger_.LogInformation("Send compute request to the worker");
      var call = client.ExecuteAsync(request,
                                     deadline: DateTime.UtcNow +
                                               taskData.Options.MaxDuration.ToTimeSpan(),
                                     cancellationToken: CancellationToken.None);

      ComputeReply result;
      try
      {
        result = await call.WrapRpcException();
      }
      catch (TimeoutException e)
      {
        logger_.LogError(e,
                         "Deadline exceeded when computing task {taskId} from session {sessionId}",
                         taskData.Id.Task,
                         taskData.Id.Session);
        await Task.WhenAll(tableStorage_.UpdateTaskStatusAsync(taskData.Id, TaskStatus.Timeout, CancellationToken.None),
                           queueStorage_.ReleaseMessageAsync(messageId, CancellationToken.None));
        return;
      }
      catch (TaskCanceledException e)
      {
        var details = string.Empty;

        if (messageToken.IsCancellationRequested) details += "Message was cancelled. ";
        if (cancellationToken.IsCancellationRequested) details                    += "Root token was cancelled. ";

        logger_.LogError(e,
                         "Execution has been cancelled for task {taskId} from session {sessionId}. {details}",
                         taskData.Id.Task,
                         taskData.Id.Session,
                         details);

        await Task.WhenAll(tableStorage_.UpdateTaskStatusAsync(taskData.Id, TaskStatus.Canceling, CancellationToken.None),
                           queueStorage_.ReleaseMessageAsync(messageId, CancellationToken.None));
        return;
      }
      catch (ArmoniKException e)
      {
        logger_.LogError(e,
                         "Execution has failed for task {taskId} from session {sessionId}. {details}",
                         taskData.Id.Task,
                         taskData.Id.Session,
                         e.ToString());

        await Task.WhenAll(tableStorage_.UpdateTaskStatusAsync(taskData.Id, TaskStatus.Failed, CancellationToken.None),
                           queueStorage_.ReleaseMessageAsync(messageId, CancellationToken.None));
        return;
      }
      catch (Exception e)
      {
        logger_.LogError(e,
                         "Exception encountered when computing task {taskId} from session {sessionId}",
                         taskData.Id.Task,
                         taskData.Id.Session);
        await Task.WhenAll(tableStorage_.UpdateTaskStatusAsync(taskData.Id, TaskStatus.Failed, CancellationToken.None),
                           queueStorage_.ReleaseMessageAsync(messageId, CancellationToken.None));
        Console.WriteLine(e);
        throw;
      }

      logger_.LogInformation("Compute finished successfully.");

      /*
         * Store Result
         */

      logger_.LogInformation("Sending result to storage.");
      await taskResultStorage_.AddOrUpdateAsync(taskData.Id, result, CancellationToken.None);
      logger_.LogInformation("Result sent.");

      await Task.WhenAll(tableStorage_.UpdateTaskStatusAsync(taskData.Id, TaskStatus.Completed, CancellationToken.None),
                         queueStorage_.MessageProcessedAsync(messageId, CancellationToken.None),
                         increaseTask);
    }
  }
}