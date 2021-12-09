// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Exceptions;
using ArmoniK.Core.gRPC;
using ArmoniK.Core.gRPC.V1;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Storage
{
  [PublicAPI]
  public class QueueStorageWrapper : IQueueStorage
  {
    private readonly ILockedQueueStorage                                             lockedQueueStorage_;
    private readonly ILeaseProvider                                                  leaseProvider_;
    private readonly ILogger<QueueStorageWrapper>                                    logger_;

    public QueueStorageWrapper(ILockedQueueStorage lockedQueueStorage, ILeaseProvider leaseProvider, ILogger<QueueStorageWrapper> logger)
    {
      lockedQueueStorage_ = lockedQueueStorage;
      leaseProvider_ = leaseProvider;
      logger_ = logger;
    }

    /// <inheritdoc />
    public int MaxPriority => lockedQueueStorage_.MaxPriority;

    /// <inheritdoc />
    public async IAsyncEnumerable<QueueMessage> PullAsync(int nbMessages,
                                                          [EnumeratorCancellation] 
                                                          CancellationToken cancellationToken = default)
    {
      using var logFunction = logger_.LogFunction();

      await foreach (var qm in lockedQueueStorage_.PullAsync(nbMessages, cancellationToken)
                                                  .WithCancellation(cancellationToken))
      {
        using var logScope = logger_.BeginPropertyScope(("messageId", qm.MessageId), ("taskId", qm.TaskId.ToPrintableId()));
        logger_.LogDebug("Pulled a message from Mongo queue. MessageId={messageId}", qm.MessageId);

        Func<Task> messageDisposeFunc = async () => await qm.DisposeAsync();

        var cancellationTokens = new List<CancellationToken>();

        logger_.LogInformation("Setting message lock");
        var deadlineHandler = lockedQueueStorage_.GetDeadlineHandler(qm.MessageId, logger_, cancellationToken);

        var messageDisposeFunc1 = messageDisposeFunc;
        messageDisposeFunc = async () =>
                             {
                               await deadlineHandler.DisposeAsync();
                               await messageDisposeFunc1();
                             };

        cancellationTokens.Add(deadlineHandler.MessageLockLost);

        if (!lockedQueueStorage_.AreMessagesUnique)
        {
          logger_.LogInformation("Setting task lease");
          LeaseHandler lease;
          try
          {
            lease = await leaseProvider_.GetLeaseHandlerAsync(qm.TaskId, logger_, cancellationToken);
            lease.LeaseExpired.ThrowIfCancellationRequested();
          }
          catch (Exception e)
          {
            logger_.LogWarning(e, "Could not acquire lease. Message is considered as a duplicate and will be rejected");
            var deleteTask = lockedQueueStorage_.MessageRejectedAsync(qm.MessageId, cancellationToken);
            await deadlineHandler.DisposeAsync();
            await deleteTask;
            continue;
          }

          var messageDisposeFunc2 = messageDisposeFunc;
          messageDisposeFunc = async () =>
                               {
                                 await lease.DisposeAsync();
                                 await messageDisposeFunc2();
                               };

          cancellationTokens.Add(lease.LeaseExpired);
        }

        var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationTokens.ToArray());

        logger_.LogInformation("Queue message ready to forward");
        yield return new QueueMessage(qm.MessageId,
                                      qm.TaskId,
                                      messageDisposeFunc,
                                      cts.Token
                                     );
      }
    }

    /// <inheritdoc />
    public Task MessageProcessedAsync(string id, CancellationToken cancellationToken = default) 
      => lockedQueueStorage_.MessageProcessedAsync(id, cancellationToken);

    /// <inheritdoc />
    public Task MessageRejectedAsync(string id, CancellationToken cancellationToken = default) 
      => lockedQueueStorage_.MessageRejectedAsync(id, cancellationToken);

    /// <inheritdoc />
    public Task EnqueueMessagesAsync(IEnumerable<TaskId> messages,
                                     int                 priority          = 1,
                                     CancellationToken   cancellationToken = default)
      => lockedQueueStorage_.EnqueueMessagesAsync(messages, priority, cancellationToken);

    /// <inheritdoc />
    public Task RequeueMessageAsync(string id, CancellationToken cancellationToken = default) 
      => lockedQueueStorage_.RequeueMessageAsync(id, cancellationToken);

    /// <inheritdoc />
    public Task ReleaseMessageAsync(string id, CancellationToken cancellationToken = default) 
      => lockedQueueStorage_.ReleaseMessageAsync(id, cancellationToken);
  }
}
