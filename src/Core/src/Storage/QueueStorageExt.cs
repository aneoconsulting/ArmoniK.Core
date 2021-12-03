// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System.Threading;
using System.Threading.Tasks;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Storage
{
  [PublicAPI]
  public static class QueueStorageExt
  {
    public static Task EnqueueAsync(this IQueueStorage queueStorage, 
                                    QueueMessage message, 
                                    int priority, 
                                    CancellationToken cancellationToken = default) 
      => queueStorage.EnqueueMessagesAsync(new[] { message }, priority, cancellationToken);

    public static QueueMessageDeadlineHandler GetDeadlineHandler(this IQueueStorage queueStorage,
                                                                 string             messageId,
                                                                 ILogger            logger,
                                                                 CancellationToken  cancellationToken = default)
      => new(queueStorage, messageId, logger, cancellationToken);
  }
}
