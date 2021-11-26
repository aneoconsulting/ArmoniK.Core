// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Linq;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

using JetBrains.Annotations;

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
                                                                 CancellationToken  cancellationToken = default)
      => new (queueStorage, messageId, cancellationToken);
  }
}
