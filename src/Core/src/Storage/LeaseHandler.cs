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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.gRPC;
using ArmoniK.Core.gRPC.V1;
using ArmoniK.Core.Utils;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Storage
{
  public class LeaseHandler : IAsyncDisposable
  {
    private readonly CancellationToken cancellationToken_;
    private readonly ILeaseProvider    leaseProvider_;
    private readonly ILogger           logger_;
    private readonly TaskId            taskId_;
    private          Heart             heart_;
    private          string            leaseId_;

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

    public CancellationToken LeaseExpired => heart_.HeartStopped;

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
      using var _ = logger_.LogFunction(taskId_.ToPrintableId());
      if (!heart_.HeartStopped.IsCancellationRequested)
        await heart_.Stop();
      await leaseProvider_.ReleaseLease(taskId_,
                                        leaseId_,
                                        cancellationToken_);
      GC.SuppressFinalize(this);
    }

    public async Task Start()
    {
      using var _ = logger_.LogFunction(taskId_.ToPrintableId());
      var lease = await leaseProvider_.TryAcquireLeaseAsync(taskId_,
                                                            cancellationToken_);
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
        heart_.Start();
    }
  }
}
