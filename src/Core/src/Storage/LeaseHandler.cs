// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.gRPC.V1;
using ArmoniK.Core.Utils;

namespace ArmoniK.Core.Storage
{
  public class LeaseHandler : IAsyncDisposable
  {
    private readonly ILeaseProvider    leaseProvider_;
    private readonly TaskId            taskId_;
    private readonly TimeSpan          refreshPeriod_;
    private readonly CancellationToken cancellationToken_;
    private          Heart             heart_;
    private          string            leaseId_;

    public LeaseHandler(ILeaseProvider    leaseProvider,
                        TaskId            taskId,
                        TimeSpan          refreshPeriod,
                        CancellationToken cancellationToken)
    {
      leaseProvider_     = leaseProvider;
      taskId_            = taskId;
      refreshPeriod_     = refreshPeriod;
      cancellationToken_ = cancellationToken;
    }

    public async Task Start()
    {
      var retentionSpan = refreshPeriod_ + refreshPeriod_;
      var lease         = await leaseProvider_.TryAcquireLease(taskId_, DateTime.UtcNow + retentionSpan, cancellationToken_);
      leaseId_ = lease.LeaseId;
      heart_ = new Heart(async ct =>
                         {
                           var renewedLease = await leaseProvider_.TryRenewLease(taskId_,
                                                                                 leaseId_,
                                                                                 DateTime.UtcNow + retentionSpan, ct);
                           return renewedLease.IsValid();
                         },
                         refreshPeriod_, cancellationToken_);

      if (lease.IsValid())
      {
        heart_.Start();
      }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
      if (!heart_.HeartStopped.IsCancellationRequested)
      {
        await heart_.Stop();
      }
      await leaseProvider_.ReleaseLease(taskId_, leaseId_, cancellationToken_);
    }
  }
}
