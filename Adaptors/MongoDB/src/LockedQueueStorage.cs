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
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Options;
using ArmoniK.Core.Adapters.MongoDB.Queue;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Logging;

using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB;

public class LockedQueueStorage : ILockedQueueStorage
{
  private readonly ILogger<LockedQueueStorage> logger_;

  private readonly string ownerId_ = Guid.NewGuid()
                                         .ToString();

  private readonly MongoCollectionProvider<QueueMessageModelMapping, QueueMessageModelMapping> queueCollectionProvider_;


  private bool isInitialized_;

  public LockedQueueStorage(MongoCollectionProvider<QueueMessageModelMapping, QueueMessageModelMapping> queueCollectionProvider,
                            QueueStorage                                                                options,
                            ILogger<LockedQueueStorage>                                                 logger)
  {
    if (options.LockRefreshExtension == TimeSpan.Zero)
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"{nameof(QueueStorage.LockRefreshExtension)} is not defined.");
    }

    if (options.PollPeriodicity == TimeSpan.Zero)
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"{nameof(QueueStorage.PollPeriodicity)} is not defined.");
    }

    if (options.LockRefreshPeriodicity == TimeSpan.Zero)
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"{nameof(QueueStorage.LockRefreshPeriodicity)} is not defined.");
    }

    queueCollectionProvider_ = queueCollectionProvider;
    logger_                  = logger;
    LockRefreshExtension     = options.LockRefreshExtension;
    PollPeriodicity          = options.PollPeriodicity;
    LockRefreshPeriodicity   = options.LockRefreshPeriodicity;
  }


  public TimeSpan PollPeriodicity { get; }

  /// <inheritdoc />
  public TimeSpan LockRefreshPeriodicity { get; }

  /// <inheritdoc />
  public TimeSpan LockRefreshExtension { get; }

  /// <inheritdoc />
  public bool AreMessagesUnique
    => true;

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      await queueCollectionProvider_.GetAsync()
                                    .ConfigureAwait(false);
    }

    isInitialized_ = true;
  }

  /// <inheritdoc />
  public ValueTask<bool> Check(HealthCheckTag tag)
    => ValueTask.FromResult(isInitialized_);

  /// <inheritdoc />
  public int MaxPriority
    => int.MaxValue;

  /// <inheritdoc />
  public async IAsyncEnumerable<IQueueMessageHandler> PullAsync(int                                        nbMessages,
                                                                [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    using var _ = logger_.LogFunction();
    var queueCollection = await queueCollectionProvider_.GetAsync()
                                                        .ConfigureAwait(false);

    var nbPulledMessage = 0;

    while (nbPulledMessage < nbMessages && !cancellationToken.IsCancellationRequested)
    {
      var updateDefinition = Builders<QueueMessageModelMapping>.Update.Set(qmdm => qmdm.OwnedUntil,
                                                                           DateTime.UtcNow + LockRefreshExtension)
                                                               .Set(qmm => qmm.OwnerId,
                                                                    ownerId_);

      var sort = Builders<QueueMessageModelMapping>.Sort.Descending(qmm => qmm.Priority)
                                                   .Ascending(qmm => qmm.SubmissionDate);

      var filter = Builders<QueueMessageModelMapping>.Filter.Or(Builders<QueueMessageModelMapping>.Filter.Exists(model => model.OwnedUntil,
                                                                                                                 false),
                                                                Builders<QueueMessageModelMapping>.Filter.Where(model => model.OwnedUntil < DateTime.UtcNow));

      logger_.LogDebug("Trying to get a new messageHandler from Mongo queue");
      var message = await queueCollection.FindOneAndUpdateAsync(filter,
                                                                updateDefinition,
                                                                new FindOneAndUpdateOptions<QueueMessageModelMapping>
                                                                {
                                                                  ReturnDocument = ReturnDocument.After,
                                                                  IsUpsert       = false,
                                                                  Sort           = sort,
                                                                },
                                                                cancellationToken)
                                         .ConfigureAwait(false);

      if (message is not null)
      {
        nbPulledMessage++;
        yield return new LockedQueueMessageHandler(this,
                                                   message.MessageId,
                                                   message.TaskId,
                                                   logger_,
                                                   CancellationToken.None);
      }
      else
      {
        await Task.Delay(PollPeriodicity,
                         cancellationToken)
                  .ConfigureAwait(false);
      }
    }
  }

  /// <inheritdoc />
  public async Task MessageProcessedAsync(string            id,
                                          CancellationToken cancellationToken = default)
  {
    using var _ = logger_.LogFunction(id);
    var queueCollection = await queueCollectionProvider_.GetAsync()
                                                        .ConfigureAwait(false);

    await queueCollection.FindOneAndDeleteAsync(qmm => qmm.MessageId == id && qmm.OwnerId == ownerId_,
                                                cancellationToken: cancellationToken)
                         .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task<bool> RenewDeadlineAsync(string            id,
                                             CancellationToken cancellationToken = default)
  {
    using var _ = logger_.LogFunction(id);
    var queueCollection = await queueCollectionProvider_.GetAsync()
                                                        .ConfigureAwait(false);

    var updateDefinition = Builders<QueueMessageModelMapping>.Update.Set(qmdm => qmdm.OwnedUntil,
                                                                         DateTime.UtcNow + LockRefreshExtension);

    var message = await queueCollection.FindOneAndUpdateAsync<QueueMessageModelMapping>(qmdm => qmdm.MessageId == id && qmdm.OwnerId == ownerId_,
                                                                                        updateDefinition,
                                                                                        new FindOneAndUpdateOptions<QueueMessageModelMapping>
                                                                                        {
                                                                                          ReturnDocument = ReturnDocument.After,
                                                                                          IsUpsert       = false,
                                                                                        },
                                                                                        cancellationToken)
                                       .ConfigureAwait(false);
    return message is not null;
  }

  /// <inheritdoc />
  public async Task EnqueueMessagesAsync(IEnumerable<string> messages,
                                         int                 priority          = 1,
                                         CancellationToken   cancellationToken = default)
  {
    using var _ = logger_.LogFunction();
    var queueCollection = await queueCollectionProvider_.GetAsync()
                                                        .ConfigureAwait(false);

    var qmms = messages.Select(message => new QueueMessageModelMapping
                                          {
                                            TaskId         = message,
                                            Priority       = priority,
                                            SubmissionDate = DateTime.UtcNow,
                                          });

    await queueCollection.InsertManyAsync(qmms,
                                          cancellationToken: cancellationToken)
                         .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task RequeueMessageAsync(string            id,
                                        CancellationToken cancellationToken = default)
  {
    using var _ = logger_.LogFunction(id);
    var queueCollection = await queueCollectionProvider_.GetAsync()
                                                        .ConfigureAwait(false);

    var updateDefinition = Builders<QueueMessageModelMapping>.Update.Unset(qmm => qmm.OwnerId)
                                                             .Unset(qmdm => qmdm.OwnedUntil)
                                                             .Set(qmm => qmm.SubmissionDate,
                                                                  DateTime.UtcNow);

    await queueCollection.FindOneAndUpdateAsync(qmm => qmm.MessageId == id,
                                                updateDefinition,
                                                cancellationToken: cancellationToken)
                         .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task MessageRejectedAsync(string            id,
                                         CancellationToken cancellationToken)
  {
    using var _ = logger_.LogFunction(id);
    var queueCollection = await queueCollectionProvider_.GetAsync()
                                                        .ConfigureAwait(false);

    await queueCollection.FindOneAndDeleteAsync(qmm => qmm.MessageId == id,
                                                cancellationToken: cancellationToken)
                         .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task ReleaseMessageAsync(string            id,
                                        CancellationToken cancellationToken)
  {
    using var _ = logger_.LogFunction(id);
    var queueCollection = await queueCollectionProvider_.GetAsync()
                                                        .ConfigureAwait(false);

    var updateDefinition = Builders<QueueMessageModelMapping>.Update.Unset(qmdm => qmdm.OwnedUntil);

    await queueCollection.FindOneAndUpdateAsync<QueueMessageModelMapping>(qmdm => qmdm.MessageId == id && qmdm.OwnerId == ownerId_,
                                                                          updateDefinition,
                                                                          new FindOneAndUpdateOptions<QueueMessageModelMapping>
                                                                          {
                                                                            IsUpsert = false,
                                                                          },
                                                                          cancellationToken)
                         .ConfigureAwait(false);
  }
}
