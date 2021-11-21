// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Utils;

using JetBrains.Annotations;

namespace ArmoniK.Core.Storage
{
  [PublicAPI]
  public class QueueMessageDeadlineHandler : IAsyncDisposable
  {
    private readonly IQueueStorage     queueStorage_;
    private readonly string            id_;
    private readonly CancellationToken cancellationToken_;
    private readonly Heart             heart_;

    public QueueMessageDeadlineHandler(IQueueStorage     queueStorage,
                                       string            id,
                                       CancellationToken cancellationToken)
    {
      queueStorage_      = queueStorage;
      id_                = id;
      cancellationToken_ = cancellationToken;
      heart_ = new Heart(async ct =>
                         {
                           var modified = await queueStorage_.RenewLockAsync(id_, ct);
                           return !modified;
                         },
                         queueStorage_.LockRefreshPeriodicity, 
                         cancellationToken_);
      heart_.Start();
    }

    public CancellationToken MessageLockLost => heart_.HeartStopped;

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
      if (!heart_.HeartStopped.IsCancellationRequested)
      {
        await heart_.Stop();
      }
      await queueStorage_.RenewLockAsync(id_, cancellationToken_);
    }
  }
}
