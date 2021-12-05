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

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using TaskCanceledException = ArmoniK.Core.Exceptions.TaskCanceledException;
using TaskStatus = ArmoniK.Core.gRPC.V1.TaskStatus;
using TimeoutException = ArmoniK.Core.Exceptions.TimeoutException;

namespace ArmoniK.Compute.PollingAgent
{
  public class Pollster
  {
    private readonly ILogger<Pollster> logger_;

    [System.Diagnostics.CodeAnalysis.SuppressMessage("CodeQuality",
                                                     "IDE0052:Remove unread private members",
                                                     Justification = "TODO: use this field")]
    private readonly int messageBatchSize_;

    private readonly ILockedQueueStorage                         lockedQueueStorage_;
    private readonly ITableStorage                         tableStorage_;
    private readonly ILeaseProvider                        leaseProvider_;
    private readonly KeyValueStorage<TaskId, ComputeReply> taskResultStorage_;
    private readonly KeyValueStorage<TaskId, Payload>      taskPayloadStorage_;
    private readonly ClientServiceProvider                 clientProvider_;

    public Pollster(ILogger<Pollster>                     logger,
                    IOptions<ComputePlan>                 options,
                    ILockedQueueStorage                         lockedQueueStorage,
                    ITableStorage                         tableStorage,
                    ILeaseProvider                        leaseProvider,
                    KeyValueStorage<TaskId, ComputeReply> taskResultStorage,
                    KeyValueStorage<TaskId, Payload>      taskPayloadStorage,
                    ClientServiceProvider                 clientProvider)
    {
      logger_             = logger;
      messageBatchSize_   = options.Value.MessageBatchSize;
      lockedQueueStorage_       = lockedQueueStorage;
      tableStorage_       = tableStorage;
      leaseProvider_      = leaseProvider;
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
        for (var i = 0; i < 5; ++i)
        {
          try
          {
            logger_.LogInformation("Trying to fetch messages");
            message = await lockedQueueStorage_.PullAsync(1, cancellationToken).SingleAsync(cancellationToken);
            break;
          }
          catch (Exception)
          {
            await Task.Delay(lockedQueueStorage_.LockRefreshPeriodicity, cancellationToken);
          }
        }

        if (message is null)
        {
          logger_.LogInformation("No more message in queue, shutting the polling agent down");
          // No more messages in queue. Application exits to help K8s in scaling down.
          // TODO : use kubectl to terminate the whole pod ?
          Environment.Exit(0);
        }

        using var scopedLogger = logger_.BeginNamedScope("Message",
                                                         ("message", message.MessageId),
                                                         ("session", message.TaskId.Session),
                                                         ("task", message.TaskId.Task));
        logger_.LogInformation(LogEvents.StartMessageProcessing, "Message acquired.");

        /*
         * Acquire locks
         */

        logger_.LogInformation("Start lease provider");
        var leaseProviderTask = leaseProvider_.GetLeaseHandler(message.TaskId, logger_, CancellationToken.None);

        logger_.LogInformation("Start queue deadline handler");
        await using var queueDeadlineHandler =
          lockedQueueStorage_.GetDeadlineHandler(message.MessageId, logger_, CancellationToken.None);

        if (queueDeadlineHandler.MessageLockLost.IsCancellationRequested)
        {
          logger_.LogWarning("Cannot refresh queue lock for message");
          // lock has been acquired elsewhere, pass to another task.
          continue;
        }

        await using var leaseHandler = await leaseProviderTask;
        if (leaseHandler.LeaseExpired.IsCancellationRequested)
        {
          logger_.LogWarning("Cannot refresh lease for task");
          // cannot get lease: task is already running elsewhere
          await lockedQueueStorage_.DeleteAsync(message.MessageId, CancellationToken.None);
          await queueDeadlineHandler.DisposeAsync();
          continue;
        }

        var combinesCTS =
          CancellationTokenSource.CreateLinkedTokenSource(queueDeadlineHandler.MessageLockLost,
                                                          leaseHandler.LeaseExpired,
                                                          cancellationToken);

        queueDeadlineHandler.MessageLockLost.Register(
          () => logger_.LogWarning("lockedQueueDeadlineHandler: message has been lost"));
        leaseHandler.LeaseExpired.Register(() => logger_.LogWarning("leaseHandler: lease has expired"));
        cancellationToken.Register(() => logger_.LogWarning("CancellationToken has been triggered"));

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
                                                                             combinesCTS.Token);

        logger_.LogDebug("Loading task data");
        var taskData = await tableStorage_.ReadTaskAsync(message.TaskId, combinesCTS.Token);

        if (isSessionCancelled &&
            taskData.Status is not (TaskStatus.Canceled or TaskStatus.Completed or TaskStatus.WaitingForChildren))
        {
          logger_.LogInformation("Task is being cancelled");
          // cannot get lease: task is already running elsewhere
          await lockedQueueStorage_.DeleteAsync(message.MessageId, cancellationToken);
          await queueDeadlineHandler.DisposeAsync();
          await tableStorage_.UpdateTaskStatusAsync(message.TaskId, TaskStatus.Canceled, cancellationToken);
          continue;
        }

        logger_.LogDebug("checking that the number of retries is not greater than the max retry number");
        if (taskData.Retries >= taskData.Options.MaxRetries)
        {
          logger_.LogInformation("Task has been retried too many times");
          await lockedQueueStorage_.DeleteAsync(message.MessageId, cancellationToken);
          await queueDeadlineHandler.DisposeAsync();
          await tableStorage_.UpdateTaskStatusAsync(message.TaskId, TaskStatus.Failed, cancellationToken);
          continue;
        }

        logger_.LogDebug("Handling the task status ({status})", taskData.Status);
        switch (taskData.Status)
        {
          case TaskStatus.Canceling:
            logger_.LogInformation("Task is being cancelled");
            await lockedQueueStorage_.DeleteAsync(message.MessageId, cancellationToken);
            await queueDeadlineHandler.DisposeAsync();
            await tableStorage_.UpdateTaskStatusAsync(message.TaskId, TaskStatus.Canceled, cancellationToken);
            continue;
          case TaskStatus.Completed:
            logger_.LogInformation("Task was already completed");
            await lockedQueueStorage_.DeleteAsync(message.MessageId, cancellationToken);
            await queueDeadlineHandler.DisposeAsync();
            continue;
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
            await lockedQueueStorage_.DeleteAsync(message.MessageId, cancellationToken);
            await queueDeadlineHandler.DisposeAsync();
            continue;
          case TaskStatus.Processing:
            logger_.LogInformation("Task is processing elsewhere ; taking over here");
            break;
          case TaskStatus.WaitingForChildren:
            logger_.LogInformation("Task was already processed and is waiting for children");
            continue;
          default:
            logger_.LogCritical("Task was in an unknown state {state}", taskData.Status);
            throw new ArgumentOutOfRangeException(nameof(taskData.Status));
        }

        logger_.LogDebug("Changing task status to 'Dispatched'");
        var updateTask = tableStorage_.UpdateTaskStatusAsync(message.TaskId, TaskStatus.Dispatched, combinesCTS.Token);

        // TODO: optimize the approach to reduce the number of requests
        async Task<bool> IsDependencyCompleted(TaskId tid)
        {
          logger_.LogDebug("Checking status for dependency with taskId {tid}", tid.ToPrintableId());
          var tData = await tableStorage_.ReadTaskAsync(tid, combinesCTS.Token);
          if (tData.Status != TaskStatus.Completed) return false;

          foreach (var dependency in tData.Options.Dependencies)
          {
            if (!await IsDependencyCompleted(dependency)) return false;
          }

          return true;
        }

        var dependencyCheckTask = taskData.Options.Dependencies.AsParallel().Select(IsDependencyCompleted).ToList();

        await Task.WhenAll(dependencyCheckTask);

        if (!dependencyCheckTask.All(task => task.Result))
        {
          logger_.LogInformation("Dependencies are not complete yet.");
          await lockedQueueStorage_.RequeueMessage(message, cancellationToken);
          await lockedQueueStorage_.DeleteAsync(message.MessageId, cancellationToken);
          await queueDeadlineHandler.DisposeAsync();
          await updateTask;
          continue;
        }


        logger_.LogInformation("Task preconditions are OK");
        await updateTask;

        await PrefetchAndCompute(taskData, queueDeadlineHandler, leaseHandler, combinesCTS, cancellationToken);
      }
    }

    private async Task PrefetchAndCompute(TaskData                    taskData,
                                          LockedQueueMessageDeadlineHandler lockedQueueDeadlineHandler,
                                          LeaseHandler                leaseHandler,
                                          CancellationTokenSource     combinesCTS,
                                          CancellationToken           cancellationToken)
    {
      using var _ = logger_.LogFunction();
      /*
         * Prefetch Data
         */


      logger_.LogInformation("Start retrieving payload");
      var payloadTask = taskData.HasPayload
        ? ValueTask.FromResult(taskData.Payload)
        : new ValueTask<Payload>(taskPayloadStorage_.TryGetValuesAsync(taskData.Id, combinesCTS.Token));

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

        await tableStorage_.UpdateTaskStatusAsync(taskData.Id, TaskStatus.Timeout, CancellationToken.None);
        return;
      }
      catch (TaskCanceledException e)
      {
        var details = string.Empty;

        if (lockedQueueDeadlineHandler.MessageLockLost.IsCancellationRequested) details += "Lock was lost on queueMessage. ";
        if (leaseHandler.LeaseExpired.IsCancellationRequested) details            += "Lease was lost on the task. ";
        if (cancellationToken.IsCancellationRequested) details                    += "Root token was cancelled. ";

        logger_.LogError(e,
                         "Execution has been cancelled for task {taskId} from session {sessionId}. {details}",
                         taskData.Id.Task,
                         taskData.Id.Session,
                         details);

        await tableStorage_.UpdateTaskStatusAsync(taskData.Id, TaskStatus.Timeout, CancellationToken.None);
        return;
      }
      catch (ArmoniKException e)
      {
        logger_.LogError(e,
                         "Execution has failed for task {taskId} from session {sessionId}. {details}",
                         taskData.Id.Task,
                         taskData.Id.Session,
                         e.ToString());

        await tableStorage_.UpdateTaskStatusAsync(taskData.Id, TaskStatus.Failed, CancellationToken.None);
        return;
      }
      catch (Exception e)
      {
        logger_.LogError(e,
                         "Exception encountered when computing task {taskId} from session {sessionId}",
                         taskData.Id.Task,
                         taskData.Id.Session);
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

      var finishedUpdate = tableStorage_.UpdateTaskStatusAsync(taskData.Id, TaskStatus.Completed, CancellationToken.None);
      await increaseTask;
      await finishedUpdate;
    }
  }
}