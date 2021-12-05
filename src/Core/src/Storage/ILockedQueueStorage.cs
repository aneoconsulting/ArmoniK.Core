// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using JetBrains.Annotations;

namespace ArmoniK.Core.Storage
{
  [PublicAPI]
  public interface ILockedQueueStorage
  {
    TimeSpan LockRefreshPeriodicity { get; }

    TimeSpan LockRefreshExtension { get; }

    int MaxPriority { get; }

    IAsyncEnumerable<QueueMessage> PullAsync(int nbMessages, CancellationToken cancellationToken = default);

    Task DeleteAsync(string id, CancellationToken cancellationToken = default);

    Task<bool> RenewLockAsync(string id, CancellationToken cancellationToken = default);

    Task UnlockAsync(string id, CancellationToken cancellationToken = default);

    Task EnqueueMessagesAsync(IEnumerable<QueueMessage> messages,
                              int                       priority          = 1,
                              CancellationToken         cancellationToken = default);

    Task RequeueMessage(QueueMessage message, CancellationToken cancellationToken = default);

    //TODO: add support for DLQ
    //Task SendToDeadLetterQueue(QueueMessage message, CancellationToken cancellationToken = default);
  }
}