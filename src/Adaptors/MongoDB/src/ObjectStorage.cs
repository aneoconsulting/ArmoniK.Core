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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Adapters.MongoDB.Common;
using ArmoniK.Adapters.MongoDB.Object;
using ArmoniK.Core;
using ArmoniK.Core.Storage;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

using MongoDB.Driver;
using MongoDB.Driver.Linq;

namespace ArmoniK.Adapters.MongoDB
{
  [PublicAPI]
  public class ObjectStorage : IObjectStorage
  {
    private readonly int                                      chunkSize_;
    private readonly ILogger<ObjectStorage>                   logger_;
    private readonly MongoCollectionProvider<ObjectDataModel> objectCollectionProvider_;
    private readonly SessionProvider                          sessionProvider_;

    public ObjectStorage(SessionProvider                          sessionProvider,
                         MongoCollectionProvider<ObjectDataModel> objectCollectionProvider,
                         ILogger<ObjectStorage>                   logger,
                         Options.ObjectStorage                    options)
    {
      sessionProvider_          = sessionProvider;
      objectCollectionProvider_ = objectCollectionProvider;
      chunkSize_                = options.ChunkSize;
      logger_                   = logger;
    }


    /// <inheritdoc />
    public async Task AddOrUpdateAsync(string key, byte[] value, CancellationToken cancellationToken = default)
    {
      using var _                = logger_.LogFunction(key);
      var       objectCollection = await objectCollectionProvider_.GetAsync();

      var taskList = new List<Task<ObjectDataModel>>();
      for (var (pos, idx) = (0, 0); pos < value.Length; idx += 1)
      {
        var chunkSize = Math.Min(value.Length - pos,
                                 chunkSize_);
        var chunk = new byte[chunkSize];
        Array.Copy(value,
                   pos,
                   chunk,
                   0,
                   chunkSize);
        pos += chunkSize;

        var updateDefinition = Builders<ObjectDataModel>.Update
                                                        .SetOnInsert(odm => odm.Chunk,
                                                                     chunk)
                                                        .SetOnInsert(odm => odm.ChunkIdx,
                                                                     idx)
                                                        .SetOnInsert(odm => odm.Key,
                                                                     key)
                                                        .SetOnInsert(odm => odm.Id,
                                                                     $"{key}{idx}");

        var localIdx = idx;
        taskList.Add(objectCollection.FindOneAndUpdateAsync<ObjectDataModel>(odm => odm.Key == key && odm.ChunkIdx == localIdx,
                                                                             updateDefinition,
                                                                             new FindOneAndUpdateOptions<ObjectDataModel>
                                                                             {
                                                                               ReturnDocument = ReturnDocument.After,
                                                                               IsUpsert       = true,
                                                                             },
                                                                             cancellationToken));
      }

      await Task.WhenAll(taskList);
      if (taskList.Any(task => task.Result is null))
      {
        logger_.LogError("Could not write value in DB for key {key}",
                         key);
        throw new InvalidOperationException("Could not write value in DB");
      }
    }

    /// <inheritdoc />
    public async Task<byte[]> TryGetValuesAsync(string key, CancellationToken cancellationToken = default)
    {
      using var _                = logger_.LogFunction(key);
      var       sessionHandle    = await sessionProvider_.GetAsync();
      var       objectCollection = await objectCollectionProvider_.GetAsync();

      var chunks = AsyncCursorSourceExt.ToAsyncEnumerable(objectCollection.AsQueryable(sessionHandle)
                                                                          .Where(odm => odm.Key == key)
                                                                          .OrderBy(odm => odm.ChunkIdx)
                                                                          .Select(odm => odm.Chunk));

      var buffer = new List<byte>(chunkSize_);
      await foreach (var chunk in chunks.WithCancellation(cancellationToken))
        buffer.AddRange(chunk);

      return buffer.ToArray();
    }

    /// <inheritdoc />
    public async Task<bool> TryDeleteAsync(string key, CancellationToken cancellationToken = default)
    {
      using var _                = logger_.LogFunction(key);
      var       objectCollection = await objectCollectionProvider_.GetAsync();

      var res = await objectCollection.DeleteManyAsync(odm => odm.Key == key,
                                                       cancellationToken);
      return res.DeletedCount > 0;
    }

    /// <inheritdoc />
    public async Task Init(CancellationToken cancellationToken)
    {
      if (!isInitialized_)
      {
        var session = sessionProvider_.GetAsync();
        await objectCollectionProvider_.GetAsync();
        await session;
        isInitialized_ = true;
      }
    }


    private bool isInitialized_ = false;

    /// <inheritdoc />
    public ValueTask<bool> Check(HealthCheckTag tag) => ValueTask.FromResult(isInitialized_);
  }
}