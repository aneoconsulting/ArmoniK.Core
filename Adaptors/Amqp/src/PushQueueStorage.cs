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
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Amqp;
using Amqp.Framing;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Utils;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Amqp;

public class PushQueueStorage : QueueStorage, IPushQueueStorage
{
  private const    int                       MaxInternalQueuePriority = 10;
  private readonly TimeSpan                  baseDelay_               = TimeSpan.FromMilliseconds(100);
  private readonly ILogger<PushQueueStorage> logger_;
  private readonly ObjectPool<Session>       sessionPool_;

  public PushQueueStorage(QueueCommon.Amqp          options,
                          IConnectionAmqp           connectionAmqp,
                          ILogger<PushQueueStorage> logger)
    : base(options,
           connectionAmqp)
  {
    logger_ = logger;
    sessionPool_ = new ObjectPool<Session>(200,
                                           async token => new Session(await connectionAmqp.GetConnectionAsync(token)
                                                                                          .ConfigureAwait(false)),
                                           (session,
                                            _) => new ValueTask<bool>(!session.IsClosed));
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

    /* Priority is handled using multiple queues; there should be at least one queue which
     * is imposed via the restriction MaxPriority > 1. If a user tries to enqueue a message
     * with priority larger or equal than MaxInternalQueuePriority, we put that message in
     * the last queue and set its internal priority MaxInternalQueuePriority.*/
    var whichQueue = priority < MaxInternalQueuePriority
                       ? priority / MaxInternalQueuePriority
                       : NbLinks - 1;
    var internalPriority = priority < MaxInternalQueuePriority
                             ? priority % MaxInternalQueuePriority
                             : MaxInternalQueuePriority;

    logger_.LogDebug("Priority is {priority} ; will use queue {partitionId}###q{whichQueue} with internal priority {internal priority}",
                     priority,
                     partitionId,
                     whichQueue,
                     internalPriority);


    await messages.ParallelForEach(new ParallelTaskOptions(cancellationToken),
                                   async msgData =>
                                   {
                                     for (var retry = 0; retry < Options.MaxRetries; retry++)
                                     {
                                       try
                                       {
                                         await using var session = await sessionPool_.GetAsync(cancellationToken)
                                                                                     .ConfigureAwait(false);

                                         var sender = new SenderLink(session,
                                                                     Guid.NewGuid()
                                                                         .ToString(),
                                                                     $"{partitionId}###q{whichQueue}");

                                         await sender.SendAsync(new Message(Encoding.UTF8.GetBytes(msgData.TaskId))
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
                                                                })
                                                     .ConfigureAwait(false);

                                         await sender.CloseAsync()
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

                                           logger_.LogDebug(e,
                                                            "Exception while receiving message; receiver replaced");
                                         }
                                         else
                                         {
                                           logger_.LogError(e,
                                                            "Exception while receiving message");
                                           throw;
                                         }
                                       }
                                     }
                                   })
                  .ConfigureAwait(false);
  }
}
