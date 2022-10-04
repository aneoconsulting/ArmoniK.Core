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
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Adapters.Amqp;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Logging;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace ArmoniK.Core.Adapters.RabbitMQ;

public class PullQueueStorage : QueueStorageBase, IPullQueueStorage
{
  private readonly IModel                    channel_;
  private readonly ILogger<PullQueueStorage> logger_;
  private readonly QueueDeclareOk            pullQueue_;

  public PullQueueStorage(Common.Injection.Options.Amqp options,
                          IModel                        channel,
                          ILogger<PullQueueStorage>     logger)
    : base(options)
  {
    channel_ = channel;
    logger_  = logger;

    channel_.ExchangeDeclare("ArmoniK.QueueExchange",
                             "direct");

    var queueArgs = new Dictionary<string, object>
                    {
                      {
                        "x-max-priority", Options!.MaxPriority
                      },
                    };

    pullQueue_ = channel_.QueueDeclare("",
                                       false, /* to survive broker restart */
                                       true /* used only by a connection, deleted after connection closes */,
                                       false, /* deleted when last consumer unsubscribes (if it has had one) */
                                       queueArgs);

    channel_.QueueBind(pullQueue_.QueueName,
                       "ArmoniK.QueueExchange",
                       Options!.PartitionId);

    /* Setup prefetching. TODO: Rename LinkCredit to something less amqpLite specific */
    channel_.BasicQos(0,
                      Convert.ToUInt16(Options.LinkCredit),
                      false);
  }

  public IAsyncEnumerable<IQueueMessageHandler> PullMessagesAsync(int               nbMessages,
                                                                  CancellationToken cancellationToken = default)
    => throw new NotImplementedException();


  public IList<IQueueMessageHandler> PullMessages(int                                        nbMessages,
                                                  [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    using var _               = logger_!.LogFunction();
    var       nbPulledMessage = 0;

    cancellationToken.ThrowIfCancellationRequested();
#if false
    while (nbPulledMessage < nbMessages)
    {
      var currentNbMessages = nbPulledMessage;
      var bGet = channel_.BasicGet(pullQueue_.QueueName,
                                   false);
      if (bGet is null)
      {
        logger_!.LogTrace("Message is null");
        continue;
      }

      yield return new QueueMessageHandler(channel_,
                                           Encoding.UTF8.GetString(bGet.Body.ToArray() ?? throw new InvalidOperationException("Error while deserializing message")),
                                           logger_!,
                                           cancellationToken);
      nbPulledMessage++;

      if (nbPulledMessage == currentNbMessages)
      {
        break;
      }
    }

#else
    var qmhList  = new List<IQueueMessageHandler>();
    var consumer = new EventingBasicConsumer(channel_);
    consumer.Received += (model,
                          eventArgs) =>
                         {
                           var message = Encoding.UTF8.GetString(eventArgs.Body.ToArray());
                           qmhList.Add(new QueueMessageHandler(channel_,
                                                               message,
                                                               logger_!,
                                                               cancellationToken));

                           nbPulledMessage++;

                           if (nbPulledMessage == nbMessages)
                           {
                             channel_.BasicCancel(eventArgs.ConsumerTag);
                           }
                         };
    channel_.BasicConsume(pullQueue_.QueueName,
                          false,
                          consumer);
    return qmhList;
#endif
  }
}
