// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.gRPC.V1;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Storage
{
  [PublicAPI]
  public static class LockedQueueStorageExt
  {
    public static Task EnqueueAsync(this ILockedQueueStorage lockedQueueStorage,
                                    TaskId                   message,
                                    int                      priority,
                                    CancellationToken        cancellationToken = default)
      => lockedQueueStorage.EnqueueMessagesAsync(new[] { message }, priority, cancellationToken);

    public static LockedQueueMessageDeadlineHandler GetDeadlineHandler(this ILockedQueueStorage lockedQueueStorage,
                                                                       string                   messageId,
                                                                       ILogger                  logger,
                                                                       CancellationToken        cancellationToken = default)
      => new(lockedQueueStorage, messageId, logger, cancellationToken);
  }
}