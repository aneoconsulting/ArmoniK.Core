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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Broker;

internal sealed class QueueMessageHandler : IQueueMessageHandler
{
  private readonly Heart        autoRenewLease_;
  private readonly BrokerClient client_;
  private readonly ILogger      logger_;
  private readonly TimeSpan     renewPeriod_;
  private          int          disposed_;
  private          bool         lost_;

  private IReadOnlyCollection<(string Id, long Size)>? outputs_;

  public QueueMessageHandler(BrokerClient  client,
                             PulledMessage message,
                             TimeSpan      lease,
                             ILogger       logger)
  {
    client_           = client;
    logger_           = logger;
    MessageId         = message.Token;
    TaskId            = message.TaskId;
    ReceptionDateTime = DateTime.UtcNow;
    // A third of the lease: a renewal lost in a row still leaves time for the next one.
    renewPeriod_ = TimeSpan.FromTicks(Math.Max(TimeSpan.TicksPerMillisecond,
                                               lease.Ticks / 3));
    autoRenewLease_ = new Heart(RenewLease,
                                renewPeriod_);
    autoRenewLease_.Start();
  }

  private async Task RenewLease(CancellationToken cancellationToken)
  {
    if (lost_)
    {
      return;
    }

    try
    {
      if (!await client_.RenewAsync(MessageId,
                                    renewPeriod_,
                                    cancellationToken)
                        .ConfigureAwait(false))
      {
        // The message designates no current distribution any more: stop renewing it.
        lost_ = true;
        logger_.LogWarning("Broker no longer holds message {MessageId} of task {TaskId}",
                           MessageId,
                           TaskId);
      }
    }
    catch (Exception e) when (e is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
    {
      logger_.LogWarning(e,
                         "Lease renewal of broker message {MessageId} of task {TaskId} failed",
                         MessageId,
                         TaskId);
    }
  }

  /// <summary>
  ///   Kept only with <see cref="Broker.Affinity" />: the broker ignores the outputs otherwise.
  /// </summary>
  public void SetOutputs(IReadOnlyCollection<(string Id, long Size)> outputs)
  {
    if (client_.Affinity)
    {
      outputs_ = outputs;
    }
  }

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
  ///   Settles the message. Its lease stops being renewed first: if the settlement fails, it is logged, not
  ///   thrown, and the broker redelivers the message after the lease instead of keeping it forever.
  /// </summary>
  public async ValueTask DisposeAsync()
  {
    if (Interlocked.Exchange(ref disposed_,
                             1) != 0)
    {
      return;
    }

    await autoRenewLease_.Stop()
                         .ConfigureAwait(false);

    try
    {
      switch (Status)
      {
        case QueueMessageStatus.Waiting:
        case QueueMessageStatus.Failed:
        case QueueMessageStatus.Running:
        case QueueMessageStatus.Postponed:
          await client_.NackAsync(MessageId)
                       .ConfigureAwait(false);
          break;
        case QueueMessageStatus.Cancelled:
        case QueueMessageStatus.Processed:
        case QueueMessageStatus.Poisonous:
          await client_.AckAsync(MessageId,
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

    return Affinity.Select(outputs_) is { } a
             ? new BrokerClient.OutputsBody(a.Hashes,
                                            a.WireSizes())
             : null;
  }
}
