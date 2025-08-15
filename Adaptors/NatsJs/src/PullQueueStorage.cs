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

internal class PullQueueStorage : IPullQueueStorage
{
  private readonly INatsJSContext js_;

  private readonly ILogger logger_;

  //private readonly string project_;
  private readonly Nats options_;
  private          bool isInitialized_;

  public PullQueueStorage(ILogger<PullQueueStorage> logger,
                          Nats                      options,
                          INatsJSContext            js,
                          bool                      isInitialized)
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
                                                            AckPolicy = ConsumerConfigAckPolicy.Explicit,
                                                          })
                             .ConfigureAwait(false);
    }

    await foreach (var natsJSMsg in consumer.FetchAsync<string>(new NatsJSFetchOpts()))
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
