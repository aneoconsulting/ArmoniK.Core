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

public class PullQueueStorage : QueueStorageBase, IPullQueueStorage
{
  private readonly IConnectionRabbit                      connection_;
  private readonly ILogger<PullQueueStorage>              logger_;
  private readonly ConcurrentQueue<IQueueMessageHandler?> queueHandlers_;

  public PullQueueStorage(Amqp                      options,
                          IConnectionRabbit         connectionRabbit,
                          ILogger<PullQueueStorage> logger)
    : base(options)
  {
    connection_ = connectionRabbit;
    logger_     = logger;

    if (string.IsNullOrEmpty(options.PartitionId))
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"{nameof(Options.PartitionId)} is not defined.");
    }

    queueHandlers_ = new ConcurrentQueue<IQueueMessageHandler?>();

    connection_.Channel.ExchangeDeclare("ArmoniK.QueueExchange",
                                        "direct");

    var queueArgs = new Dictionary<string, object>
                    {
                      {
                        "x-max-priority", Options!.MaxPriority
                      },
                    };

    var pullQueue = connection_.Channel!.QueueDeclare("",
                                                      false, /* to survive broker restart */
                                                      true,  /* used only by a connection, deleted after connection closes */
                                                      false, /* deleted when last consumer unsubscribes (if it has had one) */
                                                      queueArgs);

    connection_.Channel.QueueBind(pullQueue.QueueName,
                                  "ArmoniK.QueueExchange",
                                  Options!.PartitionId);

    /* Setup prefetching. TODO: Rename LinkCredit to something less amqpLite specific */
    connection_.Channel.BasicQos(0,
                                 Convert.ToUInt16(Options.LinkCredit),
                                 false);

    var consumer = new EventingBasicConsumer(connection_.Channel!);

    // Delegate to declare a subscriber to the queue
    void Subscriber(object?               model,
                    BasicDeliverEventArgs eventArgs)
    {
      var body    = eventArgs.Body.ToArray();
      var message = Encoding.UTF8.GetString(body);

      // Enqueue message in local storage to be pulled later, the
      // Cancellation token will be set by the pulling function.
      queueHandlers_.Enqueue(new QueueMessageHandler(connection_.Channel!,
                                                     eventArgs,
                                                     message,
                                                     logger_,
                                                     CancellationToken.None));
    }

    consumer.Received += Subscriber;

    connection_.Channel!.BasicConsume(pullQueue.QueueName,
                                      false,
                                      consumer);
  }

  public Task<IQueueMessageHandler?> PullMessagesAsync(CancellationToken cancellationToken = default)
  {
    if (!IsInitialized)
    {
      throw new ArmoniKException($"{nameof(PullQueueStorage)} should be initialized before calling this method.");
    }

    if (queueHandlers_.TryDequeue(out var qmh))
    {
      // Pass the cancellation token to the pulled handler
      qmh!.CancellationToken = cancellationToken;
    }
    else
    {
      qmh = null;
    }

    return Task.FromResult(qmh);
  }
}
