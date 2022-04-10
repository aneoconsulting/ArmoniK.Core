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
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace ArmoniK.Core.Common.Storage;

public class LockedWrapperQueueMessageHandler : IQueueMessageHandler
{
  private readonly CancellationTokenSource           cancellationTokenSource_;
  private readonly LockedQueueMessageDeadlineHandler deadlineHandler_;
  private readonly IQueueMessageHandler              queueMessageHandler_;

  public LockedWrapperQueueMessageHandler(IQueueMessageHandler              queueMessageHandler,
                                          LockedQueueMessageDeadlineHandler deadlineHandler,
                                          CancellationToken                 cancellationToken)
  {
    queueMessageHandler_ = queueMessageHandler;
    deadlineHandler_     = deadlineHandler;
    cancellationTokenSource_ = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken,
                                                                               queueMessageHandler_.CancellationToken,
                                                                               deadlineHandler_.MessageLockLost);
  }

  /// <inheritdoc />
  public CancellationToken CancellationToken => cancellationTokenSource_.Token;

  /// <inheritdoc />
  public string MessageId => queueMessageHandler_.MessageId;

  /// <inheritdoc />
  public string TaskId => queueMessageHandler_.TaskId;

  /// <inheritdoc />
  public QueueMessageStatus Status
  {
    get => queueMessageHandler_.Status;
    set => queueMessageHandler_.Status = value;
  }

  /// <inheritdoc />
  public async ValueTask DisposeAsync()
  {
    await (deadlineHandler_?.DisposeAsync() ?? ValueTask.CompletedTask).ConfigureAwait(false);
    await queueMessageHandler_.DisposeAsync().ConfigureAwait(false);
    GC.SuppressFinalize(this);
  }
}
