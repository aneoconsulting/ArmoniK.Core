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
                                       TimeSpan          refreshPeriod,
                                       CancellationToken cancellationToken)
    {
      queueStorage_      = queueStorage;
      id_                = id;
      cancellationToken_ = cancellationToken;
      var retentionSpan = refreshPeriod + refreshPeriod;
      heart_ = new Heart(async ct =>
                         {
                           var modified = await queueStorage_.ModifyVisibilityAsync(id_, DateTime.UtcNow + retentionSpan, ct);
                           return !modified;
                         },
                         refreshPeriod, cancellationToken_);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
      if (!heart_.HeartStopped.IsCancellationRequested)
      {
        await heart_.Stop();
      }
      await queueStorage_.ModifyVisibilityAsync(id_, DateTime.UtcNow, cancellationToken_);
    }
  }
}
