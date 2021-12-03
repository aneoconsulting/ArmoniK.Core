// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core;
using ArmoniK.Core.Storage;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using MongoDB.Driver;

namespace ArmoniK.Adapters.MongoDB
{
  public class QueueStorage : IQueueStorage
  {
    private readonly MongoCollectionProvider<QueueMessageModel> queueCollectionProvider_;
    private readonly SessionProvider                            sessionProvider_;
    private readonly ILogger<QueueStorage>                      logger_;
    private readonly string                                     ownerId_ = Guid.NewGuid().ToString();

    public QueueStorage(MongoCollectionProvider<QueueMessageModel> queueCollectionProvider,
                        SessionProvider                            sessionProvider,
                        IOptions<Options.QueueStorage>             options,
                        ILogger<QueueStorage>                      logger)
    {
      queueCollectionProvider_ = queueCollectionProvider;
      sessionProvider_         = sessionProvider;
      logger_                  = logger;
      LockRefreshExtension     = options.Value.LockRefreshExtension;
      PollPeriodicity          = options.Value.PollPeriodicity;
      LockRefreshPeriodicity   = options.Value.LockRefreshPeriodicity;
    }

    /// <inheritdoc />
    public TimeSpan LockRefreshPeriodicity { get; }


    public TimeSpan PollPeriodicity { get; }

    /// <inheritdoc />
    public TimeSpan LockRefreshExtension { get; }

    /// <inheritdoc />
    public int MaxPriority => int.MaxValue;

    /// <inheritdoc />
    public async IAsyncEnumerable<QueueMessage> PullAsync(
      int                                        nbMessages,
      [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
      using var _               = logger_.LogFunction();
      var       sessionHandle   = await sessionProvider_.GetAsync();
      var       queueCollection = await queueCollectionProvider_.GetAsync();

      for (var messageIdx = 0; messageIdx < nbMessages; messageIdx++)
      {
        var updateDefinition = Builders<QueueMessageModel>.Update
                                                          .Set(qmdm => qmdm.OwnedUntil,
                                                               DateTime.UtcNow + LockRefreshExtension)
                                                          .Set(qmm => qmm.OwnerId,
                                                               ownerId_);

        var sort = Builders<QueueMessageModel>.Sort
                                              .Ascending(qmm => qmm.SubmissionDate)
                                              .Descending(qmm => qmm.Priority);

        var message = await queueCollection.FindOneAndUpdateAsync<QueueMessageModel>(sessionHandle,
                                                                                     qmdm => qmdm.OwnedUntil == default ||
                                                                                             qmdm.OwnedUntil <
                                                                                             DateTime.UtcNow,
                                                                                     updateDefinition,
                                                                                     new FindOneAndUpdateOptions<
                                                                                       QueueMessageModel>()
                                                                                     {
                                                                                       ReturnDocument = ReturnDocument.After,
                                                                                       IsUpsert       = false,
                                                                                       Sort           = sort,
                                                                                     },
                                                                                     cancellationToken);

        if (message is not null)
          yield return new QueueMessage(message.MessageId, message.TaskId);
        else
          await Task.Delay(PollPeriodicity, cancellationToken);
      }
    }

    /// <inheritdoc />
    public async Task DeleteAsync(string id, CancellationToken cancellationToken = default)
    {
      using var _               = logger_.LogFunction();
      var       sessionHandle   = await sessionProvider_.GetAsync();
      var       queueCollection = await queueCollectionProvider_.GetAsync();

      await queueCollection.FindOneAndDeleteAsync(sessionHandle,
                                                  qmm => qmm.MessageId == id && qmm.OwnerId == ownerId_,
                                                  cancellationToken: cancellationToken);
    }

    /// <inheritdoc />
    public async Task<bool> RenewLockAsync(string id, CancellationToken cancellationToken = default)
    {
      using var _               = logger_.LogFunction();
      var       sessionHandle   = await sessionProvider_.GetAsync();
      var       queueCollection = await queueCollectionProvider_.GetAsync();

      var updateDefinition = Builders<QueueMessageModel>.Update
                                                        .Set(qmdm => qmdm.OwnedUntil,
                                                             DateTime.UtcNow + LockRefreshExtension);

      var message = await queueCollection.FindOneAndUpdateAsync<QueueMessageModel>(
        sessionHandle,
        qmdm => qmdm.MessageId == id &&
                qmdm.OwnerId == ownerId_,
        updateDefinition,
        new FindOneAndUpdateOptions<QueueMessageModel>()
        {
          ReturnDocument = ReturnDocument.After,
          IsUpsert       = false,
        },
        cancellationToken
      );
      return message is not null;
    }

    /// <inheritdoc />
    public async Task UnlockAsync(string id, CancellationToken cancellationToken = default)
    {
      using var _               = logger_.LogFunction();
      var       sessionHandle   = await sessionProvider_.GetAsync();
      var       queueCollection = await queueCollectionProvider_.GetAsync();

      var updateDefinition = Builders<QueueMessageModel>.Update
                                                        .Set(qmdm => qmdm.OwnedUntil,
                                                             DateTime.UtcNow - LockRefreshExtension);

      await queueCollection.FindOneAndUpdateAsync<QueueMessageModel>(
        sessionHandle,
        qmdm => qmdm.MessageId == id && qmdm.OwnerId == ownerId_,
        updateDefinition,
        new FindOneAndUpdateOptions<QueueMessageModel> { IsUpsert = false },
        cancellationToken
      );
    }

    /// <inheritdoc />
    public async Task EnqueueMessagesAsync(IEnumerable<QueueMessage> messages,
                                           int                       priority          = 1,
                                           CancellationToken         cancellationToken = default)
    {
      using var _               = logger_.LogFunction();
      var       sessionHandle   = await sessionProvider_.GetAsync();
      var       queueCollection = await queueCollectionProvider_.GetAsync();

      var qmms = messages.Select(message => new QueueMessageModel
      {
        TaskId         = message.TaskId,
        Priority       = priority,
        SubmissionDate = DateTime.UtcNow,
      });

      await queueCollection.InsertManyAsync(sessionHandle, qmms, cancellationToken: cancellationToken);
    }

    /// <inheritdoc />
    public async Task RequeueMessage(QueueMessage message, CancellationToken cancellationToken = default)
    {
      using var _               = logger_.LogFunction();
      var       sessionHandle   = await sessionProvider_.GetAsync();
      var       queueCollection = await queueCollectionProvider_.GetAsync();

      var updateDefinition = Builders<QueueMessageModel>.Update
                                                        .Unset(qmm => qmm.OwnerId)
                                                        .Unset(qmm => qmm.OwnedUntil)
                                                        .Set(qmm => qmm.SubmissionDate, DateTime.UtcNow);

      await queueCollection.FindOneAndUpdateAsync<QueueMessageModel>(
        sessionHandle,
        qmm => qmm.MessageId == message.MessageId,
        updateDefinition,
        cancellationToken: cancellationToken
      );
    }
  }
}