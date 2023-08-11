// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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

using ArmoniK.Core.Base;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ArmoniK.Core.Adapters.RabbitMQ;

public class QueueMessageHandler : IQueueMessageHandler
{
  private readonly IModel                channel_;
  private readonly BasicDeliverEventArgs deliverEvent_;

  public QueueMessageHandler(IModel                channel,
                             BasicDeliverEventArgs deliverEvent,
                             string                taskId,
                             CancellationToken     cancellationToken)
  {
    TaskId            = taskId;
    deliverEvent_     = deliverEvent;
    CancellationToken = cancellationToken;
    channel_          = channel;
    ReceptionDateTime = DateTime.UtcNow;
  }

  /// <inheritdoc />
  public CancellationToken CancellationToken { get; set; }

  /// <inheritdoc />
  public string MessageId
    => deliverEvent_.BasicProperties.MessageId;

  /// <inheritdoc />
  public string TaskId { get; }

  /// <inheritdoc />
  public QueueMessageStatus Status { get; set; }

  /// <inheritdoc />
  public DateTime ReceptionDateTime { get; init; }

  public ValueTask DisposeAsync()
  {
    switch (Status)
    {
      case QueueMessageStatus.Postponed:
        /* Negative acknowledging this message will send it
         to the retry exchange, see PullQueueStorage.cs */
        channel_.BasicNack(deliverEvent_.DeliveryTag,
                           false,
                           false);
        break;
      case QueueMessageStatus.Failed:

      case QueueMessageStatus.Running:

      case QueueMessageStatus.Cancelled:

      case QueueMessageStatus.Waiting:

      case QueueMessageStatus.Processed:

      case QueueMessageStatus.Poisonous:
        /* Failed, Processed and Poisonous messages are
         * acknowledged so they are not send to Retry exchange */
        channel_.BasicAck(deliverEvent_.DeliveryTag,
                          false);
        break;
      default:
        throw new ArgumentOutOfRangeException(nameof(Status),
                                              Status,
                                              null);
    }

    GC.SuppressFinalize(this);

    return ValueTask.CompletedTask;
  }
}
