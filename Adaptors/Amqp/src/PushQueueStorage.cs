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
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using Amqp;
using Amqp.Framing;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.ObjectPool;

namespace ArmoniK.Core.Adapters.Amqp;

/// <summary>
///   Policy for creating a <see cref="Session" /> for the <see cref="ObjectPool{Session}" />
/// </summary>
internal sealed class SessionPooledObjectPolicy : IPooledObjectPolicy<Session>
{
  private readonly IConnectionAmqp connectionAmqp_;

  /// <summary>
  ///   Initializes a <see cref="SessionPooledObjectPolicy" />
  /// </summary>
  /// <param name="connectionAmqp">AMQP connection that will be used to create new sessions</param>
  public SessionPooledObjectPolicy(IConnectionAmqp connectionAmqp)
    => connectionAmqp_ = connectionAmqp;

  /// <inheritdoc />
  public Session Create()
    => new(connectionAmqp_.Connection);

  /// <inheritdoc />
  public bool Return(Session obj)
    => !obj.IsClosed;
}

public class PushQueueStorage : QueueStorage, IPushQueueStorage
{
  private const int MaxInternalQueuePriority = 10;

  private readonly ILogger<PushQueueStorage> logger_;
  private readonly ObjectPool<Session>       sessionPool_;


  public PushQueueStorage(QueueCommon.Amqp          options,
                          IConnectionAmqp           connectionAmqp,
                          ILogger<PushQueueStorage> logger)
    : base(options,
           connectionAmqp)
  {
    logger_      = logger;
    sessionPool_ = new DefaultObjectPool<Session>(new SessionPooledObjectPolicy(ConnectionAmqp));
  }

  /// <inheritdoc />
  public async Task PushMessagesAsync(IEnumerable<MessageData> messages,
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

    var session = sessionPool_.Get();
    try
    {
      var sender = new SenderLink(session,
                                  $"{partitionId}###SenderLink{whichQueue}",
                                  $"{partitionId}###q{whichQueue}");

      await Task.WhenAll(messages.Select(msgData => sender.SendAsync(new Message(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new
                                                                                                                                 {
                                                                                                                                   msgData.Message,
                                                                                                                                   msgData.SessionId,
                                                                                                                                   msgData.Options,
                                                                                                                                 })))
                                                                     {
                                                                       Header = new Header
                                                                                {
                                                                                  Priority = (byte)internalPriority,
                                                                                },
                                                                       Properties = new Properties(),
                                                                     })))
                .ConfigureAwait(false);


      await sender.CloseAsync()
                  .ConfigureAwait(false);
    }
    finally
    {
      sessionPool_.Return(session);
    }
  }
}
