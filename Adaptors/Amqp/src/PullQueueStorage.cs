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

using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Amqp;

public class PullQueueStorage : QueueStorage, IPullQueueStorage
{
  private readonly ILogger<PullQueueStorage> logger_;

  private readonly AsyncLazy<IReceiverLink>[] receivers_;
  private readonly AsyncLazy<ISenderLink>[]   senders_;

  private bool isInitialized_;

  public PullQueueStorage(Options.Amqp              options,
                          IPullSessionAmqp          sessionAmqp,
                          ILogger<PullQueueStorage> logger)
    : base(options,
           sessionAmqp)
  {
    if (string.IsNullOrEmpty(options.PartitionId))
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"{nameof(Options.PartitionId)} is not defined.");
    }

    receivers_ = Enumerable.Range(0,
                                  NbLinks)
                           .Select(i => new AsyncLazy<IReceiverLink>(() =>
                                                                     {
                                                                       var rl = new ReceiverLink(sessionAmqp.Session,
                                                                                                 $"{options.PartitionId}###ReceiverLink{i}",
                                                                                                 $"{options.PartitionId}###q{i}");

                                                                       /* linkCredit_: the maximum number of messages the
                                                                     * remote peer can send to the receiver.
                                                                     * With the goal of minimizing/deactivating
                                                                     * prefetching, a value of 1 gave us the desired
                                                                     * behavior. We pick a default value of 2 to have "some cache". */
                                                                       rl.SetCredit(options.LinkCredit);
                                                                       return rl;
                                                                     }))
                           .ToArray();

    senders_ = Enumerable.Range(0,
                                NbLinks)
                         .Select(i => new AsyncLazy<ISenderLink>(() => new SenderLink(sessionAmqp.Session,
                                                                                      $"{options.PartitionId}###SenderLink{i}",
                                                                                      $"{options.PartitionId}###q{i}")))
                         .ToArray();

    logger_ = logger;
  }

  public new Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(isInitialized_
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy());

  public override async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      var senders   = Task.WhenAll(senders_.Select(async lazy => await lazy));
      var receivers = Task.WhenAll(receivers_.Select(async lazy => await lazy));
      await Task.WhenAll(senders,
                         receivers)
                .ConfigureAwait(false);
      isInitialized_ = true;
    }
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<IQueueMessageHandler> PullMessagesAsync(int                                        nbMessages,
                                                                        [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    using var _               = logger_!.LogFunction();
    var       nbPulledMessage = 0;

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
          logger_!.LogTrace($"Message is null for receiver {i}",
                            i);
          continue;
        }

        nbPulledMessage++;

        var sender = await senders_[i];

        yield return new QueueMessageHandler(message,
                                             sender,
                                             receiver,
                                             Encoding.UTF8.GetString(message.Body as byte[] ?? throw new InvalidOperationException("Error while deserializing message")),
                                             logger_!,
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
