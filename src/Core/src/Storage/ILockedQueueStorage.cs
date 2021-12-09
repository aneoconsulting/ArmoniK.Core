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
  public interface ILockedQueueStorage : IQueueStorageBase
  {
    TimeSpan LockRefreshPeriodicity { get; }

    TimeSpan LockRefreshExtension { get; }

    bool AreMessagesUnique { get; }

    Task<bool> RenewDeadlineAsync(string id, CancellationToken cancellationToken = default);


    /// <summary>
    /// Indicates that the message was successfully processed
    /// </summary>
    /// <param name="id"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    Task MessageProcessedAsync(string id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Indicates that the message is poisonous
    /// </summary>
    /// <param name="id"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    Task MessageRejectedAsync(string id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Places the message in the back of the queue
    /// </summary>
    /// <param name="id"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    Task RequeueMessageAsync(string id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Places the message in the front of the queue
    /// </summary>
    /// <param name="id"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    Task ReleaseMessageAsync(string id, CancellationToken cancellationToken = default);
  }
}