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
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Adapters.MongoDB.Common;
using ArmoniK.Adapters.MongoDB.Queue;
using ArmoniK.Core;
using ArmoniK.Core.gRPC.V1;
using ArmoniK.Core.Storage;

using Microsoft.Extensions.Logging;

using MongoDB.Driver;

namespace ArmoniK.Adapters.MongoDB
{
  public class LockedQueueStorage : ILockedQueueStorage
  {
    private readonly ILogger<LockedQueueStorage> logger_;
    private readonly string ownerId_ = Guid.NewGuid().ToString();
    private readonly MongoCollectionProvider<QueueMessageModel> queueCollectionProvider_;

    public LockedQueueStorage(MongoCollectionProvider<QueueMessageModel> queueCollectionProvider,
                              Options.QueueStorage                       options,
                              ILogger<LockedQueueStorage>                logger)
    {
      if (options.LockRefreshExtension == TimeSpan.Zero)
        throw new ArgumentOutOfRangeException(nameof(options),
                                              $"{nameof(Options.QueueStorage.LockRefreshExtension)} is not defined.");

      if (options.PollPeriodicity == TimeSpan.Zero)
        throw new ArgumentOutOfRangeException(nameof(options),
                                              $"{nameof(Options.QueueStorage.PollPeriodicity)} is not defined.");

      if (options.LockRefreshPeriodicity == TimeSpan.Zero)
        throw new ArgumentOutOfRangeException(nameof(options),
                                              $"{nameof(Options.QueueStorage.LockRefreshPeriodicity)} is not defined.");

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
    public bool AreMessagesUnique => true;

    /// <inheritdoc />
    public async Task Init(CancellationToken cancellationToken)
    {
      if(!isInitialized_)
        await queueCollectionProvider_.GetAsync();

      isInitialized_ = true;
    }


    private bool isInitialized_ = false;

    /// <inheritdoc />
    public ValueTask<bool> Check(HealthCheckTag tag) => ValueTask.FromResult(isInitialized_);

    /// <inheritdoc />
    public int MaxPriority => int.MaxValue;

    /// <inheritdoc />
    public async IAsyncEnumerable<IQueueMessage> PullAsync(
      int nbMessages,
      [EnumeratorCancellation] CancellationToken cancellationToken = default
    )
    {
      using var _ = logger_.LogFunction();
      var queueCollection = await queueCollectionProvider_.GetAsync();

      var nbPulledMessage = 0;

      while (nbPulledMessage < nbMessages && !cancellationToken.IsCancellationRequested)
      {
        var updateDefinition = Builders<QueueMessageModel>.Update
                                                          .Set(qmdm => qmdm.OwnedUntil,
                                                               DateTime.UtcNow + LockRefreshExtension)
                                                          .Set(qmm => qmm.OwnerId,
                                                               ownerId_);

        var sort = Builders<QueueMessageModel>.Sort
                                              .Descending(qmm => qmm.Priority)
                                              .Ascending(qmm => qmm.SubmissionDate);

        var filter = Builders<QueueMessageModel>.Filter
                                                .Or(Builders<QueueMessageModel>.Filter.Exists(model => model.OwnedUntil,
                                                                                              false),
                                                    Builders<QueueMessageModel>.Filter.Where(model => model.OwnedUntil < DateTime.UtcNow));

        logger_.LogDebug("Trying to get a new message from Mongo queue");
        var message = await queueCollection.FindOneAndUpdateAsync(filter,
                                                                  updateDefinition,
                                                                  new FindOneAndUpdateOptions<
                                                                    QueueMessageModel>
                                                                  {
                                                                    ReturnDocument = ReturnDocument.After,
                                                                    IsUpsert = false,
                                                                    Sort = sort,
                                                                  },
                                                                  cancellationToken);

        if (message is not null)
        {
          nbPulledMessage++;
          yield return new LockedQueueMessage(this,
                                              message.MessageId,
                                              message.TaskId,
                                              logger_,
                                              CancellationToken.None);
        }
        else
        {
          await Task.Delay(PollPeriodicity,
                           cancellationToken);
        }
      }
    }

    /// <inheritdoc />
    public async Task MessageProcessedAsync(string id, CancellationToken cancellationToken = default)
    {
      using var _ = logger_.LogFunction(id);
      var queueCollection = await queueCollectionProvider_.GetAsync();

      await queueCollection.FindOneAndDeleteAsync(qmm => qmm.MessageId == id && qmm.OwnerId == ownerId_,
                                                  cancellationToken: cancellationToken);
    }

    /// <inheritdoc />
    public async Task<bool> RenewDeadlineAsync(string id, CancellationToken cancellationToken = default)
    {
      using var _ = logger_.LogFunction(id);
      var queueCollection = await queueCollectionProvider_.GetAsync();

      var updateDefinition = Builders<QueueMessageModel>.Update
                                                        .Set(qmdm => qmdm.OwnedUntil,
                                                             DateTime.UtcNow + LockRefreshExtension);

      var message = await queueCollection.FindOneAndUpdateAsync<QueueMessageModel>(qmdm => qmdm.MessageId == id &&
                                                                                           qmdm.OwnerId == ownerId_,
                                                                                   updateDefinition,
                                                                                   new FindOneAndUpdateOptions<QueueMessageModel>
                                                                                   {
                                                                                     ReturnDocument =
                                                                                       ReturnDocument.After,
                                                                                     IsUpsert = false,
                                                                                   },
                                                                                   cancellationToken);
      return message is not null;
    }

    /// <inheritdoc />
    public async Task EnqueueMessagesAsync(IEnumerable<TaskId> messages,
                                           int priority = 1,
                                           CancellationToken cancellationToken = default)
    {
      using var _ = logger_.LogFunction();
      var queueCollection = await queueCollectionProvider_.GetAsync();

      var qmms = messages.Select(message => new QueueMessageModel
      {
        TaskId = message,
        Priority = priority,
        SubmissionDate = DateTime.UtcNow,
      });

      await queueCollection.InsertManyAsync(qmms,
                                            cancellationToken: cancellationToken);
    }

    /// <inheritdoc />
    public async Task RequeueMessageAsync(string id, CancellationToken cancellationToken = default)
    {
      using var _ = logger_.LogFunction(id);
      var queueCollection = await queueCollectionProvider_.GetAsync();

      var updateDefinition = Builders<QueueMessageModel>.Update
                                                        .Unset(qmm => qmm.OwnerId)
                                                        .Unset(qmdm => qmdm.OwnedUntil)
                                                        .Set(qmm => qmm.SubmissionDate,
                                                             DateTime.UtcNow);

      await queueCollection.FindOneAndUpdateAsync(qmm => qmm.MessageId == id,
                                                  updateDefinition,
                                                  cancellationToken: cancellationToken);
    }

    /// <inheritdoc />
    public async Task MessageRejectedAsync(string id, CancellationToken cancellationToken)
    {
      using var _ = logger_.LogFunction(id);
      var queueCollection = await queueCollectionProvider_.GetAsync();

      await queueCollection.FindOneAndDeleteAsync(qmm => qmm.MessageId == id,
                                                  cancellationToken: cancellationToken);
    }

    /// <inheritdoc />
    public async Task ReleaseMessageAsync(string id, CancellationToken cancellationToken)
    {
      using var _ = logger_.LogFunction(id);
      var queueCollection = await queueCollectionProvider_.GetAsync();

      var updateDefinition = Builders<QueueMessageModel>.Update
                                                        .Unset(qmdm => qmdm.OwnedUntil);

      await queueCollection.FindOneAndUpdateAsync<QueueMessageModel>(qmdm => qmdm.MessageId == id &&
                                                                             qmdm.OwnerId == ownerId_,
                                                                     updateDefinition,
                                                                     new FindOneAndUpdateOptions<QueueMessageModel>
                                                                     {
                                                                       IsUpsert = false,
                                                                     },
                                                                     cancellationToken);
    }
  }
}
