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
using System.Threading;
using System.Threading.Tasks;

using Amqp;
using Amqp.Framing;

using ArmoniK.Core.Base;
using ArmoniK.Utils;

namespace ArmoniK.Core.Adapters.Amqp;

public class QueueMessageHandler : IQueueMessageHandler
{
  private readonly string              linkName_;
  private readonly Message             message_;
  private readonly string              partitionName_;
  private readonly ObjectPool<Session> sessionPool_;

  public QueueMessageHandler(Message             message,
                             ObjectPool<Session> sessionPool,
                             string              partitionName,
                             string              linkName,
                             string              taskId,
                             CancellationToken   cancellationToken)
  {
    TaskId            = taskId;
    message_          = message;
    sessionPool_      = sessionPool;
    partitionName_    = partitionName;
    linkName_         = linkName;
    CancellationToken = cancellationToken;
    ReceptionDateTime = DateTime.UtcNow;
  }

  /// <inheritdoc />
  public CancellationToken CancellationToken { get; set; }

  /// <inheritdoc />
  public string MessageId
    => message_.Properties.MessageId;

  /// <inheritdoc />
  public string TaskId { get; }

  /// <inheritdoc />
  public QueueMessageStatus Status { get; set; }

  /// <inheritdoc />
  public DateTime ReceptionDateTime { get; init; }

  /// <inheritdoc />
  public async ValueTask DisposeAsync()
  {
    switch (Status)
    {
      case QueueMessageStatus.Postponed:
        await sessionPool_.WithInstanceAsync(async session =>
                                             {
                                               var sl = new SenderLink(session,
                                                                       linkName_,
                                                                       partitionName_);
                                               await sl.SendAsync(new Message(message_.Body)
                                                                  {
                                                                    Header = new Header
                                                                             {
                                                                               Priority = message_.Header.Priority,
                                                                             },
                                                                    Properties = new Properties(),
                                                                  })
                                                       .ConfigureAwait(false);
                                             });


        sessionPool_.WithInstance(session =>
                                  {
                                    var rl = new ReceiverLink(session,
                                                              linkName_,
                                                              partitionName_);

                                    rl.Accept(message_);
                                  });

        break;
      case QueueMessageStatus.Failed:
      case QueueMessageStatus.Running:
      case QueueMessageStatus.Waiting:
        sessionPool_.WithInstance(session =>
                                  {
                                    var rl = new ReceiverLink(session,
                                                              linkName_,
                                                              partitionName_);

                                    rl.Release(message_);
                                  });
        break;
      case QueueMessageStatus.Processed:
      case QueueMessageStatus.Cancelled:
        sessionPool_.WithInstance(session =>
                                  {
                                    var rl = new ReceiverLink(session,
                                                              linkName_,
                                                              partitionName_);

                                    rl.Accept(message_);
                                  });
        break;
      case QueueMessageStatus.Poisonous:
        sessionPool_.WithInstance(session =>
                                  {
                                    var rl = new ReceiverLink(session,
                                                              linkName_,
                                                              partitionName_);

                                    rl.Reject(message_);
                                  });
        break;
      default:
        throw new ArgumentOutOfRangeException(nameof(Status),
                                              Status,
                                              null);
    }

    GC.SuppressFinalize(this);
  }
}
