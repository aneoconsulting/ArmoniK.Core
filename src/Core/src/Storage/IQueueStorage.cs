// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Compute.gRPC.V1;

using JetBrains.Annotations;

namespace ArmoniK.Core.Storage
{
  [PublicAPI]
  public interface IQueueStorage
  {
    TimeSpan LockRefreshPeriodicity { get; }

    TimeSpan LockRefreshExtension { get; }
      
    IAsyncEnumerable<QueueMessage> PullAsync(int nbMessages, CancellationToken cancellationToken = default);

    Task<QueueMessage> ReadAsync(string id, CancellationToken cancellationToken = default);

    Task DeleteAsync(string id, CancellationToken cancellationToken = default);

    Task<bool> RenewLockAsync(string id, CancellationToken cancellationToken = default);

    Task UnlockAsync(string id, CancellationToken cancellationToken = default);

    IAsyncEnumerable<string> EnqueueMessagesAsync(IEnumerable<QueueMessage> messages, CancellationToken cancellationToken = default);

    Task<string> RequeueMessage(QueueMessage message, CancellationToken cancellationToken = default);

  }
}
