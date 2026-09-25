// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Broker;

internal sealed class QueueMessageHandler : IQueueMessageHandler, IDataAffinityMessageHandler
{
  private readonly BrokerClient client_;
  private readonly string          consumerId_;
  private readonly ILogger         logger_;
  private          int             disposed_;

  private IReadOnlyCollection<(string Id, long Size)>? outputs_;

  public QueueMessageHandler(BrokerClient client,
                             string          consumerId,
                             PulledMessage   message,
                             ILogger         logger)
  {
    client_           = client;
    consumerId_       = consumerId;
    logger_           = logger;
    MessageId         = message.Token;
    TaskId            = message.TaskId;
    ReceptionDateTime = DateTime.UtcNow;
  }

  /// <inheritdoc />
  public void SetOutputs(IReadOnlyCollection<(string Id, long Size)> outputs)
    => outputs_ = outputs;

  /// <inheritdoc />
  [Obsolete("ArmoniK now manages loss of link with the queue")]
  public CancellationToken CancellationToken { get; set; }

  /// <inheritdoc />
  public string MessageId { get; }

  /// <inheritdoc />
  public string TaskId { get; }

  /// <inheritdoc />
  public QueueMessageStatus Status { get; set; } = QueueMessageStatus.Waiting;

  /// <inheritdoc />
  public DateTime ReceptionDateTime { get; init; }

  /// <summary>
  ///   Settles the message. Leases are renewed in batches by the client, which stops renewing this one
  ///   before settling it: a failure here is logged, not thrown, and the broker redelivers after the lease.
  /// </summary>
  public async ValueTask DisposeAsync()
  {
    if (Interlocked.Exchange(ref disposed_,
                             1) != 0)
    {
      return;
    }

    try
    {
      switch (Status)
      {
        case QueueMessageStatus.Waiting:
        case QueueMessageStatus.Failed:
        case QueueMessageStatus.Running:
        case QueueMessageStatus.Postponed:
          await client_.NackAsync(consumerId_,
                                  MessageId)
                       .ConfigureAwait(false);
          break;
        case QueueMessageStatus.Cancelled:
        case QueueMessageStatus.Processed:
        case QueueMessageStatus.Poisonous:
          await client_.AckAsync(consumerId_,
                                 MessageId,
                                 OutputsBody())
                       .ConfigureAwait(false);
          break;
        default:
          throw new ArgumentOutOfRangeException(nameof(Status));
      }
    }
    catch (Exception e) when (e is not ArgumentOutOfRangeException)
    {
      logger_.LogWarning(e,
                         "Could not settle broker message {MessageId} of task {TaskId} as {Status}; its lease will expire",
                         MessageId,
                         TaskId,
                         Status);
    }
  }

  private BrokerClient.OutputsBody? OutputsBody()
  {
    if (outputs_ is not { Count: > 0 })
    {
      return null;
    }

    var a = Affinity.Select(outputs_.Select(o => (o.Id, (ulong)Math.Max(0,
                                                                         o.Size))));
    return a is null
             ? null
             : new BrokerClient.OutputsBody(a.Hashes,
                                               a.Sizes.Select(s => (int)s)
                                                .ToArray());
  }
}
