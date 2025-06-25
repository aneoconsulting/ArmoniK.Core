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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.QueueCommon;
using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.RabbitMQ;

public class PullQueueStorage : QueueStorage, IPullQueueStorage
{
  private const    int     Delay = 20;
  private readonly ILogger logger_;

  public PullQueueStorage(Amqp                      options,
                          IConnectionRabbit         connectionRabbit,
                          ILogger<PullQueueStorage> logger)
    : base(options,
           connectionRabbit)
    => logger_ = logger;

  public override Task<HealthCheckResult> Check(HealthCheckTag tag)
    => ConnectionRabbit.Check(tag);

  public override async Task Init(CancellationToken cancellationToken)
  {
    await ConnectionRabbit.Init(cancellationToken)
                          .ConfigureAwait(false);


    IsInitialized = true;
  }

  public async IAsyncEnumerable<IQueueMessageHandler> PullMessagesAsync(string                                     partitionId,
                                                                        int                                        nbMessages,
                                                                        [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    var nbPulledMessage = 0;

    if (!IsInitialized)
    {
      throw new InvalidOperationException($"{nameof(PullQueueStorage)} should be initialized before calling this method.");
    }

    var queueArgs = new Dictionary<string, object>
                    {
                      {
                        "x-max-priority", Options!.MaxPriority
                      },
                      {
                        "x-queue-mode", "lazy" // queue will try to move messages to disk as early as practically possible
                      },
                    };

    var connection = await ConnectionRabbit.GetConnectionAsync(cancellationToken)
                                           .ConfigureAwait(false);

    connection.QueueDeclare(partitionId,
                            false, /* to survive broker restart */
                            false, /* used by multiple connections */
                            false, /* not deleted when last consumer unsubscribes (if it has had one) */
                            queueArgs);

    while (nbPulledMessage < nbMessages)
    {
      cancellationToken.ThrowIfCancellationRequested();

      connection = await ConnectionRabbit.GetConnectionAsync(cancellationToken)
                                         .ConfigureAwait(false);

      var message = connection.BasicGet(partitionId,
                                        false);

      if (message is null)
      {
        await Task.Delay(Delay,
                         cancellationToken)
                  .ConfigureAwait(false);
        continue;
      }

      nbPulledMessage++;
      yield return new QueueMessageHandler(ConnectionRabbit,
                                           message,
                                           Encoding.UTF8.GetString(message.Body.ToArray()),
                                           logger_,
                                           cancellationToken);
    }
  }
}
