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
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace ArmoniK.Core.Adapters.Nats;

/// <inheritdoc />
internal class PullQueueStorage : IPullQueueStorage
{
  private readonly INatsJSContext js_;
  private readonly ILogger        logger_;
  private readonly Nats           options_;
  private          bool           isInitialized_;

  public PullQueueStorage(ILogger<PullQueueStorage> logger,
                          Nats                      options,
                          INatsJSContext            js)
  {
    options_ = options;
    js_      = js;
    logger_  = logger;
  }

  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(isInitialized_
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy("Plugin is not yet initialized."));

  public Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      isInitialized_ = true;
    }

    return Task.CompletedTask;
  }

  public int MaxPriority
    => int.MaxValue;

  /// <summary>
  ///   Retrieves messages from a JetStream subject (partition) and yields them as queue message handlers.
  /// </summary>
  /// <remarks>
  ///   The method performs the following steps:
  ///   1. Obtains a reference to the JetStream stream "armonik-stream".
  ///   2. Attempts to get an existing consumer bound to the given <paramref name="partitionId" />.
  ///   - If the consumer does not exist (<see cref="NatsJSException" />), it creates a new durable consumer
  ///   with both <c>Name</c> and <c>DurableName</c> set to <paramref name="partitionId" />, and with
  ///   <see cref="ConsumerConfigAckPolicy.Explicit" /> to require manual acknowledgement of messages.
  ///   3. Pulls up to <paramref name="nbMessages" /> messages from the consumer using <see cref="NatsJSFetchOpts.MaxMsgs" />
  ///   .
  ///   4. For each message fetched, wraps it into a <see cref="QueueMessageHandler" /> which manages acknowledgement
  ///   (Ack, Nack, extend deadlines, etc.) according to configured options (<c>AckWait</c> and <c>AckExtendDeadlineStep</c>
  ///   ).
  ///   5. Returns the message handlers as an asynchronous stream (<see cref="IAsyncEnumerable{T}" />), so messages
  ///   can be processed one by one as they arrive.
  ///   This ensures that if a consumer for the given partition does not exist, it is created on-demand,
  ///   and messages are always processed through a uniform handler abstraction.
  /// </remarks>
  /// <param name="partitionId">The subject/partition identifier to consume messages from.</param>
  /// <param name="nbMessages">The maximum number of messages to fetch in this pull request.</param>
  /// <param name="cancellationToken">Token to cancel the asynchronous operation.</param>
  public async IAsyncEnumerable<IQueueMessageHandler> PullMessagesAsync(string                                     partitionId,
                                                                        int                                        nbMessages,
                                                                        [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    INatsJSConsumer? consumer;
    var stream = await js_.GetStreamAsync("armonik-stream")
                          .ConfigureAwait(false);
    try
    {
      consumer = await stream.GetConsumerAsync(partitionId)
                             .ConfigureAwait(false);
    }
    catch (NatsJSException)
    {
      consumer = await stream.CreateOrUpdateConsumerAsync(new ConsumerConfig(partitionId)
                                                          {
                                                            Name        = partitionId,
                                                            DurableName = partitionId,
                                                            AckWait = TimeSpan.FromSeconds(options_.AckWait),
                                                            AckPolicy   = ConsumerConfigAckPolicy.Explicit,
                                                          })
                             .ConfigureAwait(false);
    }

    await foreach (var natsJSMsg in consumer.FetchAsync<string>(new NatsJSFetchOpts
                                                                {
                                                                  MaxMsgs = nbMessages,
                                                                }))
    {
      yield return new QueueMessageHandler(natsJSMsg,
                                           js_,
                                           options_.AckWait,
                                           options_.AckExtendDeadlineStep,
                                           logger_,
                                           cancellationToken);
    }

    ;
  }
}
