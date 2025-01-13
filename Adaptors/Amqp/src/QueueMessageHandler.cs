// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY, without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Threading;
using System.Threading.Tasks;

using Amqp;

using ArmoniK.Core.Base;

namespace ArmoniK.Core.Adapters.Amqp;

public class QueueMessageHandler : IQueueMessageHandler
{
  private readonly Message       message_;
  private readonly IReceiverLink receiver_;
  private readonly ISenderLink   sender_;

  public QueueMessageHandler(Message           message,
                             ISenderLink       sender,
                             IReceiverLink     receiver,
                             string            taskId,
                             CancellationToken cancellationToken)
  {
    message_          = message;
    sender_           = sender;
    receiver_         = receiver;
    TaskId            = taskId;
    CancellationToken = cancellationToken;
    ReceptionDateTime = DateTime.UtcNow;
  }

  /// <inheritdoc />
  public CancellationToken CancellationToken { get; set; }

  /// <inheritdoc />
  public string MessageId
    => message_.Properties.MessageId;

  /// <inheritdoc />
  public string TaskId { get; }

  /// <inheritdoc />
  public QueueMessageStatus Status { get; set; }

  /// <inheritdoc />
  public DateTime ReceptionDateTime { get; init; }

  /// <inheritdoc />
  public ValueTask DisposeAsync()
  {
    switch (Status)
    {
      case QueueMessageStatus.Postponed:
      case QueueMessageStatus.Failed:
      case QueueMessageStatus.Running:
      case QueueMessageStatus.Waiting:
        receiver_.Release(message_);
        break;
      case QueueMessageStatus.Processed:
      case QueueMessageStatus.Cancelled:
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

    return new ValueTask();
  }
}
