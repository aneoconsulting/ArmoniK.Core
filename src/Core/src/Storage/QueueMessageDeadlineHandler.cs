// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Utils;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Storage
{
  [PublicAPI]
  public class QueueMessageDeadlineHandler : IAsyncDisposable
  {
    private readonly IQueueStorage     queueStorage_;
    private readonly string            id_;
    private readonly CancellationToken cancellationToken_;
    private readonly Heart             heart_;
    private readonly ILogger           logger_;

    public QueueMessageDeadlineHandler(IQueueStorage queueStorage,
                                       string id,
                                       ILogger logger,
                                       CancellationToken cancellationToken = default)
    {
      queueStorage_ = queueStorage;
      id_ = id;
      cancellationToken_ = cancellationToken;
      heart_ = new Heart(async ct =>
                         {
                           var modified = await queueStorage_.RenewLockAsync(id_, ct);
                           return !modified;
                         },
                         queueStorage_.LockRefreshPeriodicity,
                         cancellationToken_);
      heart_.Start();
      logger_ = logger;
    }

    public CancellationToken MessageLockLost => heart_.HeartStopped;

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
      using var _ = logger_.LogFunction(functionName: $"{nameof(QueueMessageDeadlineHandler)}.{nameof(DisposeAsync)}");
      if (!heart_.HeartStopped.IsCancellationRequested)
      {
        await heart_.Stop();
      }
      await queueStorage_.RenewLockAsync(id_, cancellationToken_);
      GC.SuppressFinalize(this);
    }
  }
}
