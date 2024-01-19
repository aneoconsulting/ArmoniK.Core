// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Amqp;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Utils;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Amqp;

public class PullQueueStorage : QueueStorage, IPullQueueStorage
{
  private readonly ILogger<PullQueueStorage> logger_;
  private readonly ObjectPool<Session>       sessionPool_;


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

    logger_ = logger;
    sessionPool_ = new ObjectPool<Session>(200,
                                           () => new Session(connectionAmqp.Connection),
                                           session => !session.IsClosed);
  }

  public override Task<HealthCheckResult> Check(HealthCheckTag tag)
    => ConnectionAmqp.Check(tag);

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
      for (var i = NbLinks - 1; i >= 0; --i)
      {
        cancellationToken.ThrowIfCancellationRequested();

        var linkName      = $"{Options.PartitionId}###{Guid.NewGuid()}";
        var partitionName = $"{Options.PartitionId}###q{i}";
        var message = await sessionPool_.WithInstanceAsync(async session =>
                                                           {
                                                             var rl = new ReceiverLink(session,
                                                                                       linkName,
                                                                                       partitionName);
                                                             rl.SetCredit(Options.LinkCredit);

                                                             return await rl.ReceiveAsync(TimeSpan.FromMilliseconds(100))
                                                                            .ConfigureAwait(false);
                                                           },
                                                           cancellationToken)
                                        .ConfigureAwait(false);

        if (message is null)
        {
          logger_.LogTrace("Message is null for receiver {receiver}",
                           i);
          continue;
        }

        nbPulledMessage++;

        yield return new QueueMessageHandler(message,
                                             sessionPool_,
                                             partitionName,
                                             linkName,
                                             Encoding.UTF8.GetString(message.Body as byte[] ?? throw new InvalidOperationException("Error while deserializing message")),
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
