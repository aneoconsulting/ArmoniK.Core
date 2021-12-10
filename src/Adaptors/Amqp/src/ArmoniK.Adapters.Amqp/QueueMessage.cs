// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Threading;
using System.Threading.Tasks;

using Amqp;

using ArmoniK.Core;
using ArmoniK.Core.gRPC.V1;
using ArmoniK.Core.Storage;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Adapters.Amqp
{
  public class QueueMessage : IQueueMessage
  {
    private readonly Message       message_;
    private readonly ISenderLink   sender_;
    private readonly IReceiverLink receiver_;
    private readonly ILogger       logger_;

    public QueueMessage(Message message, ISenderLink sender, IReceiverLink receiver, TaskId taskId, ILogger logger, CancellationToken cancellationToken)
    {
      message_          = message;
      sender_           = sender;
      receiver_         = receiver;
      logger_           = logger;
      TaskId            = taskId;
      CancellationToken = cancellationToken;
    }

    /// <inheritdoc />
    public CancellationToken CancellationToken { get; }

    /// <inheritdoc />
    public string MessageId => message_.Properties.MessageId;

    /// <inheritdoc />
    public TaskId TaskId { get; }

    /// <inheritdoc />
    public QueueMessageStatus Status { get; set; }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
      logger_.LogFunction(MessageId, functionName:$"{nameof(QueueStorage)}.{nameof(DisposeAsync)}");
      switch (Status)
      {
        case QueueMessageStatus.Postponed:
          await sender_.SendAsync(message_);
          receiver_.Accept(message_);
          break;
        case QueueMessageStatus.Failed:
          receiver_.Release(message_);
          break;
        case QueueMessageStatus.Processed:
          receiver_.Accept(message_);
          break;
        case QueueMessageStatus.Poisonous:
          receiver_.Reject(message_);
          break;
        default:
          throw new ArgumentOutOfRangeException(nameof(Status),
                                                Status,
                                                null);
      }
      GC.SuppressFinalize(this);
    }
  }
}
