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
using System.Linq;
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

/// <summary>
///   Retrieve messages from the queue.
/// </summary>
internal class PullQueueStorage : IPullQueueStorage
{
  private readonly INatsJSContext js_;
  private readonly ILogger        logger_;
  private readonly Nats           options_;
  private          bool           isInitialized_;

  /// <summary>
  ///   Activate logger, get the options and the JetStreamContex through dependency injection
  /// </summary>
  /// <param name="logger">Enable activation of log in the class</param>
  /// <param name="options">Nats options </param>
  /// <param name="js">The JetStreamContext allowing to create and manage streams</param>
  public PullQueueStorage(ILogger<PullQueueStorage> logger,
                          Nats                      options,
                          INatsJSContext            js)
  {
    options_ = options;
    js_      = js;
    logger_  = logger;
  }

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(isInitialized_
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy("Plugin is not yet initialized."));

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      isInitialized_ = true;
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public int MaxPriority
    => int.Max(options_.MaxPriority,
               1);

  /// <inheritdoc />
  /// <remarks>
  ///   The method performs the following steps:
  ///   1. Obtains a reference to the JetStream stream "armonik-stream" or create the stream.
  ///   2. Attempts to get an existing consumer bound to the given <paramref name="partitionId" />.
  ///   - If the consumer does not exist (<see cref="NatsJSException" />), it creates a new durable consumer
  ///   with <c>DurableName</c> set to <paramref name="partitionId" />, and with
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
  public async IAsyncEnumerable<IQueueMessageHandler> PullMessagesAsync(string                                     partitionId,
                                                                        int                                        nbMessages,
                                                                        [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    INatsJSConsumer[] prioryties;
    INatsJSStream? stream;
    // Create a fonc commune create stream
    //var streamGestion = new StreamGestion(js_, options_);
    try
    {
      stream = await js_.GetStreamAsync("armonik-stream", cancellationToken:cancellationToken)
                        .ConfigureAwait(false);
      for (var prio = 0; prio < MaxPriority; prio++)
      {
        var priority = prio + 1;
        if (!stream.Info.Config.Subjects!.Contains(partitionId + priority))
        {
          stream.Info.Config.Subjects!.Add(partitionId + priority);
          await js_.UpdateStreamAsync(stream.Info.Config, cancellationToken: cancellationToken)
                   .ConfigureAwait(false);
        }
      }
    }
    catch (NatsJSApiException ex) when (ex.Error.Code == 404)
    {
      try
      {
        var subjects = Enumerable.Range(1,
                                      MaxPriority)
                               .Select(i => partitionId + i)
                               .ToArray();
        var config = new StreamConfig
        {
          Name = "armonik-stream",
          Storage = StreamConfigStorage.File,
          Subjects = subjects,
          Retention = StreamConfigRetention.Workqueue,
        };
        stream = await js_.CreateStreamAsync(config, cancellationToken)
                          .ConfigureAwait(false);

      }
      catch (NatsJSApiException) when (ex.Error.Code == 400)
      {
        stream = await js_.GetStreamAsync("armonik-stream", cancellationToken: cancellationToken)
                        .ConfigureAwait(false);
      }

      catch (NatsJSApiException e2)
      {
        logger_.LogError(e2,
          "{errorcode}", e2.Error.Code);
        throw;
      }
      //end
    }
    try
    {
      prioryties = await Task.WhenAll(Enumerable.Range(1,
                                                       MaxPriority)
                                                .Select(async i => await stream.GetConsumerAsync(partitionId + i, cancellationToken)
                                                                               .ConfigureAwait(false)));
    }
    catch (NatsJSApiException ex) when (ex.Error.Code == 404)
    {
      prioryties = await Task.WhenAll(Enumerable.Range(1,
                                                       MaxPriority)
                                                .Select(async i => await js_.CreateConsumerAsync("armonik-stream",
                                                                                                 new ConsumerConfig(partitionId + i)
                                                                                                 {
                                                                                                   DurableName   = partitionId + i,
                                                                                                   AckWait       = TimeSpan.FromSeconds(options_.AckWait),
                                                                                                   AckPolicy     = ConsumerConfigAckPolicy.Explicit,
                                                                                                   FilterSubject = partitionId + i,
                                                                                                 }, cancellationToken)));
    }

    var i = nbMessages;
    foreach (var consumer in prioryties.Reverse())
    {
      if (options_.WaitTimeSeconds < TimeSpan.FromSeconds(1))
      {
        await foreach (var natsJSMsg in consumer.FetchNoWaitAsync<string>(new NatsJSFetchOpts
        {
          MaxMsgs = i,
        }, cancellationToken:cancellationToken)
                                                .ConfigureAwait(false))
        {
          yield return new QueueMessageHandler(natsJSMsg,
                                               js_,
                                               options_.AckWait,
                                               options_.AckExtendDeadlineStep,
                                               logger_,
                                               cancellationToken);
          --i;
          if (i == 0)
          {
            break;
          }
        }
      }
      else
      {
          await foreach (var natsJSMsg in consumer.FetchAsync<string>(new NatsJSFetchOpts
          {
            MaxMsgs = i,
            Expires = options_.WaitTimeSeconds,
          }, cancellationToken: cancellationToken)
                                                  .ConfigureAwait(false))
          {
            yield return new QueueMessageHandler(natsJSMsg,
                                                 js_,
                                                 options_.AckWait,
                                                 options_.AckExtendDeadlineStep,
                                                 logger_,
                                                 cancellationToken);
            --i;
            if (i == 0)
            {
              break;
            }
        }
      }
    }
  }
}
