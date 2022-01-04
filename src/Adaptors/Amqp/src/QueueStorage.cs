// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
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

using Amqp;

using ArmoniK.Core;
using ArmoniK.Core.gRPC.V1;
using ArmoniK.Core.Storage;
using ArmoniK.Core.Utils;

using Google.Protobuf;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ArmoniK.Adapters.Amqp
{
  public class QueueStorage : IQueueStorage
  {
    private readonly ILogger<QueueStorage>      logger_;
    private readonly AsyncLazy<IReceiverLink>[] receivers_;
    private readonly AsyncLazy<ISenderLink>[]   senders_;

    public QueueStorage(IOptions<Options.Amqp> options, SessionProvider sessionProvider, ILogger<QueueStorage> logger)
    {
      MaxPriority = options.Value.MaxPriority;
      logger_     = logger;

      var nbLinks = (MaxPriority + 9) / 10;

      senders_ = Enumerable.Range(0,
                                  nbLinks)
                           .Select(i => new AsyncLazy<ISenderLink>(async ()
                                                                     => new
                                                                       SenderLink(await sessionProvider.GetAsync(),
                                                                                  $"SenderLink{i}",
                                                                                  $"q{i}")))
                           .ToArray();

      receivers_ = Enumerable.Range(0,
                                    nbLinks)
                             .Select(i => new AsyncLazy<IReceiverLink>(async ()
                                                                         =>
                                                                       {
                                                                         var receiver =  new
                                                                           ReceiverLink(await sessionProvider.GetAsync(),
                                                                                        $"ReceiverLink{i}",
                                                                                        $"q{i}");
                                                                         receiver.SetCredit(10);
                                                                         return receiver;
                                                                       }))
                             .ToArray();
    }

    /// <inheritdoc />
    public Task Init(CancellationToken cancellationToken)
    {
      var senders = Task.WhenAll(senders_.Select(async lazy => await lazy));
      var receivers = Task.WhenAll(receivers_.Select(async lazy => await lazy));
      return Task.WhenAll(senders,
                          receivers);
    }

    /// <inheritdoc />
    public int MaxPriority { get; }

    /// <inheritdoc />
    public async IAsyncEnumerable<IQueueMessage> PullAsync(int nbMessages, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
      using var _               = logger_.LogFunction();
      var       nbPulledMessage = 0;

      while (nbPulledMessage < nbMessages)
      {
        var currentNbMessages = nbPulledMessage;
        for (var i = receivers_.Length - 1; i >= 0; --i)
        {
          cancellationToken.ThrowIfCancellationRequested();
          var receiver = await receivers_[i];
          var message  = await receiver.ReceiveAsync(TimeSpan.FromMilliseconds(100));
          if (message is null)
          {
            logger_.LogDebug("Message is null receiver {i}", i);
            continue;
          }

          if (TaskId.Parser.ParseFrom(message.Body as byte[]) is not TaskId taskId)
          {
            logger_.LogError("Body of message with Id={id} is not a TaskId",
                             message.Properties.MessageId);
            continue;
          }

          nbPulledMessage++;

          yield return new QueueMessage(message,
                                        await senders_[i],
                                        receiver,
                                        taskId,
                                        logger_,
                                        cancellationToken);

          break;
        }

        if (nbPulledMessage == currentNbMessages) break;
      }
    }

    /// <inheritdoc />
    public async Task EnqueueMessagesAsync(IEnumerable<TaskId> messages,
                                           int                 priority          = 1,
                                           CancellationToken   cancellationToken = default)
    {
      var sender = await senders_[priority / 10];
      await Task.WhenAll(messages.Select(id => sender.SendAsync(new(id.ToByteArray())
                                                                {
                                                                  Header = new()
                                                                           {
                                                                             Priority = (byte)((priority % 10)),
                                                                           },
                                                                  Properties = new (),
                                                                })));
    }
  }
}
