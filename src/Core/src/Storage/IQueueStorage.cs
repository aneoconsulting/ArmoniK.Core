// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Threading;
using System.Threading.Tasks;

using JetBrains.Annotations;

namespace ArmoniK.Core.Storage
{
  [PublicAPI]
  public interface IQueueStorage
  {
    Task<QueueMessage> PullAsync(DateTime deadline, CancellationToken cancellationToken = default);

    Task DeleteAsync(string id, CancellationToken cancellationToken = default);

    Task ModifyDeadlineAsync(string id, DateTime deadline, CancellationToken cancellationToken = default);

    Task<string> EnqueueAsync(QueueMessage message, CancellationToken cancellationToken = default);
  }
}
