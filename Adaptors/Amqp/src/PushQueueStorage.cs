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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Amqp;
using Amqp.Framing;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Utils;
using ArmoniK.Utils.Pool;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Amqp;

public class PushQueueStorage : QueueStorage, IPushQueueStorage
{
  private const    int                                                  MaxInternalQueuePriority = 10;
  private readonly TimeSpan                                             baseDelay_               = TimeSpan.FromMilliseconds(100);
  private readonly ILogger<PushQueueStorage>                            logger_;
  private readonly int                                                  parallelismLimit_;
  private readonly ConcurrentDictionary<string, ObjectPool<SenderLink>> senders_ = new();

  public PushQueueStorage(QueueCommon.Amqp          options,
                          IConnectionAmqp           connectionAmqp,
                          ILogger<PushQueueStorage> logger)
    : base(options,
           connectionAmqp)
  {
    parallelismLimit_ = options.ParallelismLimit;
    logger_           = logger;
  }

  /// <inheritdoc />
  public async Task PushMessagesAsync(IEnumerable<MessageData> messages,
                                      string                   partitionId,
                                      CancellationToken        cancellationToken = default)
  {
    var priorityGroups = messages.GroupBy(msgData => msgData.Options.Priority);
    await priorityGroups.ParallelForEach(new ParallelTaskOptions(cancellationToken),
                                         group => PushMessagesAsync(group,
                                                                    partitionId,
                                                                    group.Key,
                                                                    cancellationToken))
                        .ConfigureAwait(false);
  }

  private async Task PushMessagesAsync(IEnumerable<MessageData> messages,
                                       string                   partitionId,
                                       int                      priority          = 1,
                                       CancellationToken        cancellationToken = default)
  {
    if (!IsInitialized)
    {
      throw new InvalidOperationException($"{nameof(PushQueueStorage)} should be initialized before calling this method.");
    }

    /* AMQP's JDBC supports (0-9) priority range. In order to allow for more levels of priority, we use multiple queues that we
     * interpret as priority queues, messages in queue_k will be dequeued before messages in queue_j for  k > j. Each priority queue
     * has its own internal (0-9) priority range provided by the protocol.
     * There should be at least one priority queue which is imposed via the restriction MaxPriority >= 1.
     * If a user tries to enqueue a message with priority larger or equal than MaxInternalQueuePriority, that message is put in a
     * priority queue with an internal priority defined by the arithmetic below */
    var whichQueue       = (priority - 1) / MaxInternalQueuePriority;
    var internalPriority = (priority - 1) % MaxInternalQueuePriority;

    logger_.LogDebug("Priority is {priority} ; will use queue {partitionId}###q{whichQueue} with internal priority {internalPriority}",
                     priority,
                     partitionId,
                     whichQueue,
                     internalPriority);


    await messages.ParallelForEach(new ParallelTaskOptions(parallelismLimit_,
                                                           cancellationToken),
                                   async msgData =>
                                   {
                                     for (var retry = 0; retry < Options.MaxRetries; retry++)
                                     {
                                       try
                                       {
                                         var pool = GetPool($"{partitionId}###q{whichQueue}");

                                         await pool.WithInstanceAsync(sender => sender.SendAsync(new Message(Encoding.UTF8.GetBytes(msgData.TaskId))
                                                                                                 {
                                                                                                   Header = new Header
                                                                                                            {
                                                                                                              Priority = (byte)internalPriority,
                                                                                                            },
                                                                                                   Properties = new Properties
                                                                                                                {
                                                                                                                  MessageId = Guid.NewGuid()
                                                                                                                                  .ToString(),
                                                                                                                },
                                                                                                 }),
                                                                      cancellationToken)
                                                   .ConfigureAwait(false);

                                         break;
                                       }
                                       catch (Exception e)
                                       {
                                         if (retry < Options.MaxRetries - 1)
                                         {
                                           await Task.Delay(retry * retry * baseDelay_,
                                                            cancellationToken)
                                                     .ConfigureAwait(false);

                                           logger_.LogError(e,
                                                            "Exception while sending message; sender replaced");
                                         }
                                         else
                                         {
                                           logger_.LogError(e,
                                                            "Exception while sending message");
                                           throw;
                                         }
                                       }
                                     }
                                   })
                  .ConfigureAwait(false);
  }

  private ObjectPool<SenderLink> GetPool(string address)
    => senders_.GetOrAdd(address,
                         s => new ObjectPool<SenderLink>(200,
                                                         async token => new SenderLink(new Session(await ConnectionAmqp.GetConnectionAsync(token)
                                                                                                                       .ConfigureAwait(false)),
                                                                                       Guid.NewGuid()
                                                                                           .ToString(),
                                                                                       s),
                                                         (link,
                                                          _) => new ValueTask<bool>(!link.IsClosed && !link.Session.IsClosed)));
}
