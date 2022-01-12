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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.gRPC.V1;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Storage;

public class LockedQueueMessage : IQueueMessage
{
  private readonly CancellationToken   cancellationToken_;
  private readonly ILockedQueueStorage lockedQueueStorage_;
  private readonly ILogger             logger_;

  public LockedQueueMessage(ILockedQueueStorage lockedQueueStorage, string messageId, TaskId taskId, ILogger logger, CancellationToken cancellationToken)
  {
    lockedQueueStorage_ = lockedQueueStorage;
    logger_             = logger;
    MessageId           = messageId;
    TaskId              = taskId;
    cancellationToken_  = cancellationToken;
  }

  /// <inheritdoc />
  public CancellationToken CancellationToken => CancellationToken.None;

  /// <inheritdoc />
  public string MessageId { get; }

  /// <inheritdoc />
  public TaskId TaskId { get; }

  /// <inheritdoc />
  public QueueMessageStatus Status { get; set; }

  /// <inheritdoc />
  public async ValueTask DisposeAsync()
  {
    using var _ = logger_.LogFunction(MessageId,
                                      functionName: $"{nameof(LockedQueueMessage)}.{nameof(DisposeAsync)}");
    switch (Status)
    {
      case QueueMessageStatus.Postponed:
        await lockedQueueStorage_.RequeueMessageAsync(MessageId,
                                                      cancellationToken_);
        break;
      case QueueMessageStatus.Failed:
        await lockedQueueStorage_.ReleaseMessageAsync(MessageId,
                                                      cancellationToken_);
        break;
      case QueueMessageStatus.Processed:
        await lockedQueueStorage_.MessageProcessedAsync(MessageId,
                                                        cancellationToken_);
        break;
      case QueueMessageStatus.Poisonous:
        await lockedQueueStorage_.MessageRejectedAsync(MessageId,
                                                       cancellationToken_);
        break;
      default:
        throw new ArgumentOutOfRangeException();
    }

    GC.SuppressFinalize(this);
  }
}