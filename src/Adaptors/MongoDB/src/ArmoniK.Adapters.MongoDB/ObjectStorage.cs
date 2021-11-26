// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

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
    private readonly IMongoCollection<ObjectDataModel> objectCollection_;
    private readonly IClientSessionHandle              sessionHandle_;
    private readonly int                               chunkSize_;
    private readonly ILogger<ObjectStorage>            logger_;

    public ObjectStorage(IClientSessionHandle              sessionHandle,
                         IMongoCollection<ObjectDataModel> objectCollection,
                         ILogger<ObjectStorage>            logger,
                         int                               chunkSize = 14500000)
    {
      sessionHandle_    = sessionHandle;
      objectCollection_ = objectCollection;
      chunkSize_        = chunkSize;
      logger_           = logger;
    }



    /// <inheritdoc />
    public async Task AddOrUpdateAsync(string key, byte[] value, CancellationToken cancellationToken = default)
    {
      logger_.LogFunction();
      var taskList = new List<Task<ObjectDataModel>>();
      for (var (pos, idx) = (0,0); pos < value.Length; idx+=1)
      {
        var chunkSize = Math.Min(value.Length - pos, chunkSize_);
        var chunk     = new byte[chunkSize];
        Array.Copy(value, pos, chunk, 0, chunkSize);
        pos += chunkSize;

        var updateDefinition = Builders<ObjectDataModel>.Update
                                                        .SetOnInsert(odm => odm.Chunk, chunk)
                                                        .SetOnInsert(odm => odm.ChunkIdx, idx)
                                                        .SetOnInsert(odm => odm.Key, key)
                                                        .SetOnInsert(odm => odm.Id, $"{key}{idx}");

        taskList.Add(objectCollection_.FindOneAndUpdateAsync<ObjectDataModel>
                       (
                        sessionHandle_,
                        odm => odm.Key == key && odm.ChunkIdx == idx,
                        updateDefinition,
                        new FindOneAndUpdateOptions<ObjectDataModel>()
                        {
                          ReturnDocument = ReturnDocument.After,
                          IsUpsert       = true,
                        },
                        cancellationToken
                       ));
      }

      await Task.WhenAll(taskList);
      if (taskList.Any(task => task.Result is null))
      {
        logger_.LogError("Could not write value in DB for key {key}", key);
        throw new InvalidOperationException("Could not write value in DB");
      }

    }

    /// <inheritdoc />
    public async Task<byte[]> TryGetValuesAsync(string key, CancellationToken cancellationToken = default)
    {
      var chunks = objectCollection_.AsQueryable(sessionHandle_)
                                    .Where(odm => odm.Key == key)
                                    .OrderBy(odm => odm.ChunkIdx)
                                    .Select(odm => odm.Chunk)
                                    .ToAsyncEnumerable();

      var buffer = new List<byte>(chunkSize_);
      await foreach (var chunk in chunks.WithCancellation(cancellationToken))
      {
        buffer.AddRange(chunk);
      }

      return buffer.ToArray();
    }

    /// <inheritdoc />
    public async Task<bool> TryDeleteAsync(string key, CancellationToken cancellationToken = default)
    {
      var res = await objectCollection_.DeleteManyAsync(sessionHandle_, 
                                                        odm => odm.Key == key, 
                                                        cancellationToken: cancellationToken);
      return res.DeletedCount > 0;
    }
  }
}
