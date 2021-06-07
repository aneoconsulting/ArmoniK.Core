using System;
using System.Threading;
using System.Threading.Tasks;
using ArmoniK.Common;
using ArmoniK.Common.Exceptions;
using ArmoniK.Common.gRPC;
using ArmoniK.Common.gRPC.V1;
using ArmoniK.Common.gRPC.V1.DispatchStatus;

using Google.Protobuf;

using Microsoft.Extensions.Logging;

using TaskStatus = ArmoniK.DevelopmentKit.Common.TaskStatus;

namespace ArmoniK.DevelopmentKit.Client
{
  public class Dispatch : IDispatch
  {

    // TODO handle preemption : intercept SIGTERM (using AssemblyLoadContext.Default.Unloading ?)

    private readonly DispatchService.DispatchServiceClient dispatchClient_; 
    private readonly ILogger                               logger_;
    private const    int                                   CallDeadlineS    = 2;
    private const    int                                   RefreshDelayS    = 10;
    private const    int                                   HeartbeatPeriodS = RefreshDelayS;
    private readonly IdMessage                             dispatchTaskIdMessage_;
    private readonly CancellationTokenSource               computeCancellationTokenSource_ = new ();
    private readonly IHeartbeat                            heartbeat_;
    private          bool                                  isCancellationRequested_;
#pragma warning disable 649
    private          bool                                  isPreemptionRequested_;
#pragma warning restore 649
    private          bool                                  isTimeoutRequested_;

    public Dispatch(ITask                                 task,
                           DispatchService.DispatchServiceClient dispatchClient,
                           TaskDispatchId                        taskDispatchId,
                           ILogger                               logger)
    {
      dispatchClient_        = dispatchClient;
      TaskDispatchId         = taskDispatchId;
      logger_                = logger;
      Task                   = task;
      dispatchTaskIdMessage_ = new IdMessage {Id = TaskDispatchId.Value};
      heartbeat_             = new Heart().Start(Beat, TimeSpan.FromSeconds(HeartbeatPeriodS));
    }

    private async System.Threading.Tasks.Task UpdateAndRefreshDispatchStatusAsync(CancellationToken cancellationToken = default)
    {
      logger_.LogTrace("Updating a dispatch status");
      var sm = await dispatchClient_.UpdateDispatchStatusAsync(new DispatchStatusUpdateRequest
                                                       {
                                                         TaskDispatchId = dispatchTaskIdMessage_,
                                                         Status         = (EnumDispatchStatus) NextStatus,
                                                       },
                                                       null,
                                                       DateTime.UtcNow + TimeSpan.FromSeconds(CallDeadlineS),
                                                       cancellationToken)
                            .WrapRpcException();
      logger_.LogTrace("Updating a dispatch status: done.");
      CurrentStatus = (DispatchStatus) sm.Status;
      if (sm.IsCancellationRequested && !computeCancellationTokenSource_.IsCancellationRequested)
      {
        isCancellationRequested_ = true;
        computeCancellationTokenSource_.Cancel();
        NextStatus = DispatchStatus.Canceling;
      }

      if (sm.IsTimeoutDetected && !computeCancellationTokenSource_.IsCancellationRequested)
      {
        isTimeoutRequested_ = true;
        computeCancellationTokenSource_.Cancel();
        NextStatus = DispatchStatus.Canceling;
      }

    }

    /// <inheritdoc />
    public async Task<DispatchStatus> UpdateDispatchStatusAsync(DispatchStatus status, CancellationToken cancellationToken = default)
    {
      NextStatus = status;
      if ((heartbeat_.BeatWaiter.IsCompleted || heartbeat_.BeatWaiter.IsCanceled) && status == DispatchStatus.Completed)
      {
        heartbeat_.BeatNow();
      }

      await heartbeat_.BeatWaiter;
      return CurrentStatus;
    }

    /// <inheritdoc />
    public DispatchStatus CurrentStatus { get; private set; } = DispatchStatus.Preparing;

    private DispatchStatus NextStatus { get;   set; } = DispatchStatus.Preparing;


    /// <inheritdoc />
    public async Task<byte[]> GetPayloadAsync(CancellationToken cancellationToken = default)
    {
      var res = await  Task.GetPayloadAsync(cancellationToken);
      NextStatus = DispatchStatus.Running;
      return res;
    }

    /// <inheritdoc />
    public async System.Threading.Tasks.Task SendResultAsync(byte[] result, CancellationToken cancellationToken = default)
    {
      logger_.LogTrace("Sending result");
      NextStatus = DispatchStatus.Finishing;
      _ = await dispatchClient_.SendResultAsync(new ResultRequest
                                        {
                                          TaskDispatchId = dispatchTaskIdMessage_,
                                          Result         = new DataMessage {Data = ByteString.CopyFrom(result)},
                                          Status         = EnumDispatchStatus.Completed,
                                        },
                                        null,
                                        DateTime.UtcNow + TimeSpan.FromSeconds(CallDeadlineS),
                                        cancellationToken)
                       .WrapRpcException();
      logger_.LogTrace("Sending result: updating status.");
      var completing    = UpdateDispatchStatusAsync(DispatchStatus.Completed, cancellationToken);
      var taskCompleted = Task.UpdateStatusAsync(TaskStatus.Completed, cancellationToken);
      heartbeat_.BeatNow();
      await System.Threading.Tasks.Task.WhenAll(completing, taskCompleted);
      logger_.LogTrace("Sending result: done.");
    }

    /// <inheritdoc />
    public async System.Threading.Tasks.Task SendFailureAsync(CancellationToken cancellationToken = default)
    {
      logger_.LogWarning("Sending failure");
      var dispatchUpdate = UpdateDispatchStatusAsync(DispatchStatus.Failed, cancellationToken);
      var taskUpdate     = Task.UpdateStatusAsync(TaskStatus.ReSubmitted, cancellationToken);
      heartbeat_.BeatNow();
      await System.Threading.Tasks.Task.WhenAll(dispatchUpdate, taskUpdate);
      logger_.LogWarning("Sending failure: done.");
    }

    /// <inheritdoc />
    public async System.Threading.Tasks.Task SendCancellationAsync(CancellationToken cancellationToken = default)
    {
      Task<DispatchStatus> dispatchUpdate;
      Task<TaskStatus>     taskUpdate;
      if (isCancellationRequested_)
      {
        logger_.LogWarning("Sending cancellation");
        dispatchUpdate = UpdateDispatchStatusAsync(DispatchStatus.Cancelled, cancellationToken);
        taskUpdate     = Task.UpdateStatusAsync(TaskStatus.Cancelled, cancellationToken);
        heartbeat_.BeatNow();
        await System.Threading.Tasks.Task.WhenAll(dispatchUpdate, taskUpdate);
        logger_.LogWarning("Sending cancellation: done.");
      }
      else if (isTimeoutRequested_)
      {
        logger_.LogWarning("Sending timeout");

        dispatchUpdate = UpdateDispatchStatusAsync(DispatchStatus.Timeout, cancellationToken);
        taskUpdate     = Task.UpdateStatusAsync(TaskStatus.ReSubmitted, cancellationToken);
        heartbeat_.BeatNow();
        await System.Threading.Tasks.Task.WhenAll(dispatchUpdate, taskUpdate);
        logger_.LogWarning("Sending timeout: done.");
      }
      else if (isPreemptionRequested_)
      {
        logger_.LogWarning("Sending preemption");

        dispatchUpdate = UpdateDispatchStatusAsync(DispatchStatus.Preempted, cancellationToken);
        taskUpdate     = Task.UpdateStatusAsync(TaskStatus.ReSubmitted, cancellationToken);
        heartbeat_.BeatNow();
        await System.Threading.Tasks.Task.WhenAll(dispatchUpdate, taskUpdate);
        logger_.LogWarning("Sending preemption: done.");
      }
      else
      {
        await SendFailureAsync(cancellationToken);
      }
    }

    private async Task<bool> Beat(CancellationToken cancellationToken)
    {
      try
      {
        await UpdateAndRefreshDispatchStatusAsync(cancellationToken);
        return CurrentStatus switch
               {
                 DispatchStatus.Completed => false,
                 DispatchStatus.Cancelled => false,
                 DispatchStatus.Failed    => false,
                 DispatchStatus.Timeout   => false,
                 DispatchStatus.Preempted => false,
                 DispatchStatus.Canceling => true,
                 DispatchStatus.Created   => true,
                 DispatchStatus.Preparing => true,
                 DispatchStatus.Running   => true,
                 DispatchStatus.Finishing => true,
                 _                        => throw new ArgumentOutOfRangeException(nameof(CurrentStatus), CurrentStatus, "invalid value"),
               };
      }
      catch (Exception e)
      {
        computeCancellationTokenSource_.Cancel();
        Console.WriteLine(e);
        throw;
      }
    }

    /// <inheritdoc />
    public CancellationToken ComputeCancellationToken => computeCancellationTokenSource_.Token;

    /// <inheritdoc />
    public TaskDispatchId TaskDispatchId { get; }

    /// <inheritdoc />
    public ITask Task { get; }

    /// <inheritdoc />
    public System.Threading.Tasks.TaskStatus HeartbeatStatus => heartbeat_.BeatWaiter.Status;
  }
}