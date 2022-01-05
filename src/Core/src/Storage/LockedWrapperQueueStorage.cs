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
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.gRPC;
using ArmoniK.Core.gRPC.V1;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Storage
{
  public class LockedWrapperQueueMessage : IQueueMessage
  {
    private readonly CancellationTokenSource           cancellationTokenSource_;
    private readonly LockedQueueMessageDeadlineHandler deadlineHandler_;
    private readonly LeaseHandler                      leaseHandler_;
    private readonly IQueueMessage                     queueMessage_;

    public LockedWrapperQueueMessage(IQueueMessage                     queueMessage,
                                     LockedQueueMessageDeadlineHandler deadlineHandler,
                                     LeaseHandler                      leaseHandler,
                                     CancellationToken                 cancellationToken)
    {
      queueMessage_    = queueMessage;
      deadlineHandler_ = deadlineHandler;
      leaseHandler_    = leaseHandler;
      cancellationTokenSource_ = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken,
                                                                                 queueMessage_.CancellationToken,
                                                                                 deadlineHandler_.MessageLockLost,
                                                                                 leaseHandler_?.LeaseExpired ?? CancellationToken.None);
    }

    /// <inheritdoc />
    public CancellationToken CancellationToken => cancellationTokenSource_.Token;

    /// <inheritdoc />
    public string MessageId => queueMessage_.MessageId;

    /// <inheritdoc />
    public TaskId TaskId => queueMessage_.TaskId;

    /// <inheritdoc />
    public QueueMessageStatus Status
    {
      get => queueMessage_.Status;
      set => queueMessage_.Status = value;
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
      await (deadlineHandler_ is null ? ValueTask.CompletedTask : deadlineHandler_.DisposeAsync());
      await (leaseHandler_ is null ? ValueTask.CompletedTask : leaseHandler_.DisposeAsync());
      await queueMessage_.DisposeAsync();
      GC.SuppressFinalize(this);
    }
  }

  [PublicAPI]
  public class LockedWrapperQueueStorage : IQueueStorage
  {
    private readonly ILeaseProvider                     leaseProvider_;
    private readonly ILockedQueueStorage                lockedQueueStorage_;
    private readonly ILogger<LockedWrapperQueueStorage> logger_;

    public LockedWrapperQueueStorage(ILockedQueueStorage lockedQueueStorage, ILeaseProvider leaseProvider, ILogger<LockedWrapperQueueStorage> logger)
    {
      lockedQueueStorage_ = lockedQueueStorage;
      leaseProvider_      = leaseProvider;
      logger_             = logger;
    }

    /// <inheritdoc />
    public Task Init(CancellationToken cancellationToken)
      => Task.WhenAll(leaseProvider_.Init(cancellationToken),
                      lockedQueueStorage_.Init(cancellationToken));

    /// <inheritdoc />
    public int MaxPriority => lockedQueueStorage_.MaxPriority;

    /// <inheritdoc />
    public async IAsyncEnumerable<IQueueMessage> PullAsync(int                                        nbMessages,
                                                           [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
      using var logFunction = logger_.LogFunction($"for {nbMessages} messages");

      await foreach (var qm in lockedQueueStorage_.PullAsync(nbMessages,
                                                             cancellationToken)
                                                  .WithCancellation(cancellationToken))
      {
        using var logScope = logger_.BeginPropertyScope(("messageId", qm.MessageId),
                                                        ("taskId", qm.TaskId.ToPrintableId()));

        logger_.LogInformation("Setting message lock");
        var deadlineHandler = lockedQueueStorage_.GetDeadlineHandler(qm.MessageId,
                                                                     logger_,
                                                                     cancellationToken);

        LeaseHandler leaseHandler = null;
        if (!lockedQueueStorage_.AreMessagesUnique)
        {
          logger_.LogInformation("Setting task lease");
          try
          {
            leaseHandler = await leaseProvider_.GetLeaseHandlerAsync(qm.TaskId,
                                                                     logger_,
                                                                     cancellationToken);
            leaseHandler.LeaseExpired.ThrowIfCancellationRequested();
          }
          catch (Exception e)
          {
            logger_.LogWarning(e,
                               "Could not acquire lease. Message is considered as a duplicate and will be rejected");
            qm.Status = QueueMessageStatus.Failed;
            await deadlineHandler.DisposeAsync();
            continue;
          }
        }

        logger_.LogInformation("Queue message ready to forward");
        yield return new LockedWrapperQueueMessage(qm,
                                                   deadlineHandler,
                                                   leaseHandler,
                                                   cancellationToken);
      }
    }

    /// <inheritdoc />
    public Task EnqueueMessagesAsync(IEnumerable<TaskId> messages,
                                     int                 priority          = 1,
                                     CancellationToken   cancellationToken = default)
      => lockedQueueStorage_.EnqueueMessagesAsync(messages,
                                                  priority,
                                                  cancellationToken);

    /// <inheritdoc />
    public Task MessageProcessedAsync(string id, CancellationToken cancellationToken = default)
      => lockedQueueStorage_.MessageProcessedAsync(id,
                                                   cancellationToken);

    /// <inheritdoc />
    public Task MessageRejectedAsync(string id, CancellationToken cancellationToken = default)
      => lockedQueueStorage_.MessageRejectedAsync(id,
                                                  cancellationToken);

    /// <inheritdoc />
    public Task RequeueMessageAsync(string id, CancellationToken cancellationToken = default)
      => lockedQueueStorage_.RequeueMessageAsync(id,
                                                 cancellationToken);

    /// <inheritdoc />
    public Task ReleaseMessageAsync(string id, CancellationToken cancellationToken = default)
      => lockedQueueStorage_.ReleaseMessageAsync(id,
                                                 cancellationToken);
  }
}
