// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Amqp;

using ArmoniK.Core.Base;
using ArmoniK.Utils;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Amqp;

public class PullQueueStorage : QueueStorage, IPullQueueStorage
{
  private readonly ILogger<PullQueueStorage> logger_;

  private AsyncLazy<IReceiverLink>[] receivers_;
  private AsyncLazy<ISenderLink>[]   senders_;

  public PullQueueStorage(QueueCommon.Amqp          options,
                          IConnectionAmqp           connectionAmqp,
                          ILogger<PullQueueStorage> logger)
    : base(options,
           connectionAmqp)
  {
    if (string.IsNullOrEmpty(options.PartitionId))
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"{nameof(QueueCommon.Amqp.PartitionId)} is not defined.");
    }

    logger_    = logger;
    receivers_ = Array.Empty<AsyncLazy<IReceiverLink>>();
    senders_   = Array.Empty<AsyncLazy<ISenderLink>>();
  }

  public override Task<HealthCheckResult> Check(HealthCheckTag tag)
    => ConnectionAmqp.Check(tag);

  public override async Task Init(CancellationToken cancellationToken)
  {
    if (!IsInitialized)
    {
      await ConnectionAmqp.Init(cancellationToken)
                          .ConfigureAwait(false);

      var session = new Session(ConnectionAmqp.Connection);

      receivers_ = Enumerable.Range(0,
                                    NbLinks)
                             .Select(i => new AsyncLazy<IReceiverLink>(() =>
                                                                       {
                                                                         var rl = new ReceiverLink(session,
                                                                                                   $"{Options.PartitionId}###ReceiverLink{i}",
                                                                                                   $"{Options.PartitionId}###q{i}");

                                                                         /* linkCredit_: the maximum number of messages the
                                                                       * remote peer can send to the receiver.
                                                                       * With the goal of minimizing/deactivating
                                                                       * prefetching, a value of 1 gave us the desired
                                                                       * behavior. We pick a default value of 2 to have "some cache". */
                                                                         rl.SetCredit(Options.LinkCredit);
                                                                         return rl;
                                                                       }))
                             .ToArray();

      senders_ = Enumerable.Range(0,
                                  NbLinks)
                           .Select(i => new AsyncLazy<ISenderLink>(() => new SenderLink(session,
                                                                                        $"{Options.PartitionId}###SenderLink{i}",
                                                                                        $"{Options.PartitionId}###q{i}")))
                           .ToArray();

      var senders   = Task.WhenAll(senders_.Select(lazy => lazy.Value));
      var receivers = Task.WhenAll(receivers_.Select(lazy => lazy.Value));
      await Task.WhenAll(senders,
                         receivers)
                .ConfigureAwait(false);
      IsInitialized = true;
    }
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<IQueueMessageHandler> PullMessagesAsync(int                                        nbMessages,
                                                                        [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    var nbPulledMessage = 0;

    if (!IsInitialized)
    {
      throw new InvalidOperationException($"{nameof(PullQueueStorage)} should be initialized before calling this method.");
    }

    while (nbPulledMessage < nbMessages)
    {
      var currentNbMessages = nbPulledMessage;
      for (var i = receivers_.Length - 1; i >= 0; --i)
      {
        cancellationToken.ThrowIfCancellationRequested();
        var receiver = await receivers_[i];
        var message = await receiver.ReceiveAsync(TimeSpan.FromMilliseconds(100))
                                    .ConfigureAwait(false);
        if (message is null)
        {
          logger_.LogTrace("Message is null for receiver {receiver}",
                           i);
          continue;
        }

        nbPulledMessage++;

        var sender = await senders_[i];

        yield return new QueueMessageHandler(message,
                                             sender,
                                             receiver,
                                             Encoding.UTF8.GetString(message.Body as byte[] ?? throw new InvalidOperationException("Error while deserializing message")),
                                             logger_,
                                             cancellationToken);

        break;
      }

      if (nbPulledMessage == currentNbMessages)
      {
        break;
      }
    }
  }
}
