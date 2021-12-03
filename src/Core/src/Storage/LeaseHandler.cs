// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.gRPC.V1;
using ArmoniK.Core.Utils;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Storage
{
  public class LeaseHandler : IAsyncDisposable
  {
    private readonly ILeaseProvider    leaseProvider_;
    private readonly TaskId            taskId_;
    private readonly CancellationToken cancellationToken_;
    private          Heart             heart_;
    private          string            leaseId_;
    private readonly ILogger           logger_;

    public LeaseHandler(ILeaseProvider    leaseProvider,
                        TaskId            taskId,
                        ILogger           logger,
                        CancellationToken cancellationToken = default)
    {
      leaseProvider_     = leaseProvider;
      taskId_            = taskId;
      cancellationToken_ = cancellationToken;
      logger_            = logger;
    }

    public async Task Start()
    {
      using var _     = logger_.LogFunction();
      var       lease = await leaseProvider_.TryAcquireLease(taskId_, cancellationToken_);
      leaseId_ = lease.LeaseId;
      heart_ = new Heart(async ct =>
                         {
                           var renewedLease = await leaseProvider_.TryRenewLease(taskId_,
                                                                                 leaseId_,
                                                                                 ct);
                           return renewedLease.IsValid();
                         },
                         leaseProvider_.AcquisitionPeriod,
                         cancellationToken_);

      if (lease.IsValid())
      {
        heart_.Start();
      }
    }

    public CancellationToken LeaseExpired => heart_.HeartStopped;

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
      using var _ = logger_.LogFunction();
      if (!heart_.HeartStopped.IsCancellationRequested)
      {
        await heart_.Stop();
      }

      await leaseProvider_.ReleaseLease(taskId_, leaseId_, cancellationToken_);
      GC.SuppressFinalize(this);
    }
  }
}