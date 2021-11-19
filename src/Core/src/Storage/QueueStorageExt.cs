// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using JetBrains.Annotations;

namespace ArmoniK.Core.Storage
{
  [PublicAPI]
  public static class QueueStorageExt
  {
    public static async Task<string> EnqueueAsync(this IQueueStorage queueStorage, QueueMessage message, CancellationToken cancellationToken = default)
    {
      return await queueStorage.EnqueueMessagesAsync(new[] { message }, cancellationToken).SingleAsync(cancellationToken);
    }

    public static QueueMessageDeadlineHandler GetDeadlineHandler(this IQueueStorage queueStorage,
                                                                 string             id,
                                                                 TimeSpan           refreshPeriod,
                                                                 CancellationToken  cancellationToken)
      => new (queueStorage, id, refreshPeriod, cancellationToken);
  }
}
