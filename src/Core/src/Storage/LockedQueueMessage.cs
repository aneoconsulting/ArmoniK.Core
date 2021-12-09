// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.gRPC.V1;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Storage
{
  public class LockedQueueMessage : IQueueMessage
  {
    private readonly ILockedQueueStorage lockedQueueStorage_;
    private readonly ILogger             logger_;
    private readonly CancellationToken   cancellationToken_;

    public LockedQueueMessage(ILockedQueueStorage lockedQueueStorage, string messageId, TaskId taskId, Microsoft.Extensions.Logging.ILogger logger, CancellationToken cancellationToken)
    {
      lockedQueueStorage_ = lockedQueueStorage;
      logger_             = logger;
      MessageId           = messageId;
      TaskId              = taskId;
      cancellationToken_  = cancellationToken;
    }

    /// <inheritdoc />
    public CancellationToken CancellationToken => CancellationToken.None;

    /// <inheritdoc />
    public string MessageId { get; }

    /// <inheritdoc />
    public TaskId TaskId { get; }

    /// <inheritdoc />
    public QueueMessageStatus Status { get; set; }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
      using var _ = logger_.LogFunction(MessageId,
                                        functionName: $"{nameof(LockedQueueMessage)}.{nameof(DisposeAsync)}");
      switch (Status)
      {
        case QueueMessageStatus.Postponed:
          await lockedQueueStorage_.RequeueMessageAsync(MessageId,
                                                        cancellationToken_);
          break;
        case QueueMessageStatus.Failed:
          await lockedQueueStorage_.ReleaseMessageAsync(MessageId,
                                                        cancellationToken_);
          break;
        case QueueMessageStatus.Processed:
          await lockedQueueStorage_.MessageProcessedAsync(MessageId,
                                                          cancellationToken_);
          break;
        case QueueMessageStatus.Poisonous:
          await lockedQueueStorage_.MessageRejectedAsync(MessageId,
                                                         cancellationToken_);
          break;
        default:
          throw new ArgumentOutOfRangeException();
      }
    }
  }
}
