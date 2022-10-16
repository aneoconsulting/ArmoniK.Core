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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Logging;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ArmoniK.Core.Adapters.RabbitMQ;

public class PullQueueStorage : QueueStorage, IPullQueueStorage
{
  private readonly ILogger<PullQueueStorage>              logger_;
  private          ConcurrentQueue<IQueueMessageHandler>? queueMessageHandlers_;

  public PullQueueStorage(Amqp                      options,
                          IConnectionRabbit         connectionRabbit,
                          ILogger<PullQueueStorage> logger)
    : base(options,
           connectionRabbit)
  {
    logger_ = logger;

    if (string.IsNullOrEmpty(options.PartitionId))
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"{nameof(Options.PartitionId)} is not defined.");
    }
  }

  public override async Task Init(CancellationToken cancellationToken)
  {
    if (!IsInitialized)
    {
      await ConnectionRabbit.Init(cancellationToken)
                            .ConfigureAwait(false);

      /* Local storage where pulled messages from the consumer will be placed.
       * PollingAgent should fetch one message a time from here */
      queueMessageHandlers_ = new ConcurrentQueue<IQueueMessageHandler>();

      /* Work exchange to decide task execution */
      ConnectionRabbit.Channel.ExchangeDeclare("ArmoniK.QueueExchange",
                                               "direct");

      /* Retry exchange that will be used to place tasks from the Work
       * exchange that cannot be treated. The approach is necessary because
       * RabbitMQ will try to place a retry message in its original place in the
       * queue, which can lead to an undesired and potentially infinite retry-loop.
       * We will use the Retry exchange to place retry messages in a
       * another queue which is configured to resend the messages back to the work queue
       * after a given TTL expires. */
      ConnectionRabbit.Channel.ExchangeDeclare("ArmoniK.RetryExchange",
                                               "direct");

      /* Declare a working queue that will be bonded to the Work exchange
       * and configure to send rejected messages to the RetryExchange */
      var queueArgs = new Dictionary<string, object>
                      {
                        {
                          "x-max-priority", Options!.MaxPriority
                        },
                        {
                          "x-dead-letter-exchange", "ArmoniK.RetryExchange"
                        },
                        {
                          "x-queue-mode", "lazy" // queue will try to move messages to disk as early as practically possible
                        },
                      };

      var pullQueue = ConnectionRabbit.Channel!.QueueDeclare("",
                                                             false, /* to survive broker restart */
                                                             true,  /* used only by a connection, deleted after connection closes */
                                                             false, /* deleted when last consumer unsubscribes (if it has had one) */
                                                             queueArgs);

      ConnectionRabbit.Channel.QueueBind(pullQueue.QueueName,
                                         "ArmoniK.QueueExchange",
                                         Options!.PartitionId);

      /* Declare a retry queue that will be bonded to the Retry exchange
       * and configured to send expired messages  back to the Work Exchange */
      var retryArgs = new Dictionary<string, object>
                      {
                        {
                          "x-dead-letter-exchange", "ArmoniK.QueueExchange"
                        },
                        {
                          "x-message-ttl", 100 // TODO: Make this a configurable variable?
                        },
                      };

      var retryQueue = ConnectionRabbit.Channel!.QueueDeclare("",
                                                              false,
                                                              true,
                                                              false,
                                                              retryArgs);

      ConnectionRabbit.Channel.QueueBind(retryQueue.QueueName,
                                         "ArmoniK.RetryExchange",
                                         Options!.PartitionId);

      /* Setup prefetching. TODO: Rename LinkCredit to something less amqpLite specific */
      ConnectionRabbit.Channel.BasicQos(0,
                                        Convert.ToUInt16(Options.LinkCredit),
                                        false);

      var consumer = new AsyncEventingBasicConsumer(ConnectionRabbit.Channel!);

      // Delegate to declare a subscriber to the queue
      Task Subscriber(object?               model,
                      BasicDeliverEventArgs eventArgs)
      {
        var body    = eventArgs.Body.ToArray();
        var message = Encoding.UTF8.GetString(body);

        // Enqueue message in local storage to be pulled later, the
        // Cancellation token will be set by the pulling function.
        queueMessageHandlers_.Enqueue(new QueueMessageHandler(ConnectionRabbit.Channel!,
                                                              eventArgs,
                                                              message,
                                                              logger_,
                                                              cancellationToken));
        return Task.CompletedTask;
      }

      consumer.Received += Subscriber;

      ConnectionRabbit.Channel!.BasicConsume(pullQueue.QueueName,
                                             false,
                                             consumer);
      IsInitialized = true;
    }
  }

  public async IAsyncEnumerable<IQueueMessageHandler> PullMessagesAsync(int                                        nbMessages,
                                                                        [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    var nbPulledMessage = 0;

    if (!IsInitialized)
    {
      throw new ArmoniKException($"{nameof(PullQueueStorage)} should be initialized before calling this method.");
    }

    // TODO: Fix this, I put it here cuz the method must be async to return an IAsyncEnumerable.
    await Task.CompletedTask.ConfigureAwait(false);

    cancellationToken.ThrowIfCancellationRequested();
    while (nbPulledMessage < nbMessages)
    {
      if (!queueMessageHandlers_!.TryDequeue(out var qmh))
      {
        continue;
      }

      nbPulledMessage++;

      // Pass the cancellation token to the pulled handler
      qmh!.CancellationToken = cancellationToken;
      yield return qmh;
    }
  }
}
