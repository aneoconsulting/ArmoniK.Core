// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
//   D. Brasseur       <dbrasseur@aneo.fr>
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

using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Logging;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ArmoniK.Core.Adapters.RabbitMQ;

public class QueueMessageHandler : IQueueMessageHandler
{
  private readonly IModel                channel_;
  private readonly BasicDeliverEventArgs deliverEvent_;
  private readonly ILogger               logger_;

  public QueueMessageHandler(IModel                channel,
                             BasicDeliverEventArgs deliverEvent,
                             string                taskId,
                             ILogger               logger,
                             CancellationToken     cancellationToken)
  {
    logger_           = logger;
    TaskId            = taskId;
    deliverEvent_     = deliverEvent;
    CancellationToken = cancellationToken;
    channel_          = channel;
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

  public ValueTask DisposeAsync()
  {
    using var _ = logger_.LogFunction(MessageId,
                                      functionName: $"{nameof(QueueStorage)}.{nameof(DisposeAsync)}");
    switch (Status)
    {
      case QueueMessageStatus.Postponed:
        channel_.BasicPublish(deliverEvent_.Exchange,
                              deliverEvent_.RoutingKey,
                              deliverEvent_.BasicProperties,
                              deliverEvent_.Body);
        channel_.BasicAck(deliverEvent_.DeliveryTag,
                          false);
        break;
      case QueueMessageStatus.Failed:
        channel_.BasicNack(deliverEvent_.DeliveryTag,
                           false,
                           false);
        break;
      case QueueMessageStatus.Processed:
        channel_.BasicAck(deliverEvent_.DeliveryTag,
                          false);
        break;
      case QueueMessageStatus.Poisonous:
        channel_.BasicReject(deliverEvent_.DeliveryTag,
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
