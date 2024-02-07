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
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Object;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Utils;

using JetBrains.Annotations;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using MongoDB.Driver;
using MongoDB.Driver.Linq;

namespace ArmoniK.Core.Adapters.MongoDB;

[PublicAPI]
public class ObjectStorage : IObjectStorage
{
  private readonly ILogger<ObjectStorage>                                                  logger_;
  private readonly MongoCollectionProvider<ObjectDataModelMapping, ObjectDataModelMapping> objectCollectionProvider_;
  private readonly string                                                                  objectStorageName_;
  private readonly SessionProvider                                                         sessionProvider_;
  private          bool                                                                    isInitialized_;

  public ObjectStorage(SessionProvider                                                         sessionProvider,
                       MongoCollectionProvider<ObjectDataModelMapping, ObjectDataModelMapping> objectCollectionProvider,
                       ILogger<ObjectStorage>                                                  logger,
                       Options.ObjectStorage                                                   options)
  {
    if (options.ChunkSize == 0)
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"Minimum value for {nameof(Options.ObjectStorage.ChunkSize)} is 1.");
    }

    sessionProvider_          = sessionProvider;
    objectCollectionProvider_ = objectCollectionProvider;
    objectStorageName_        = "storage/";
    ChunkSize                 = options.ChunkSize;
    logger_                   = logger;
  }

  public int ChunkSize { get; }

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      await sessionProvider_.Init(cancellationToken)
                            .ConfigureAwait(false);
      sessionProvider_.Get();

      await objectCollectionProvider_.Init(cancellationToken)
                                     .ConfigureAwait(false);
      objectCollectionProvider_.Get();
    }

    isInitialized_ = true;
  }

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(isInitialized_
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy());

  /// <inheritdoc />
  public async Task<long> AddOrUpdateAsync(string                                 key,
                                           IAsyncEnumerable<ReadOnlyMemory<byte>> valueChunks,
                                           CancellationToken                      cancellationToken = default)
  {
    long      size             = 0;
    var       dbKey            = objectStorageName_ + key;
    using var _                = logger_.LogFunction(dbKey);
    var       objectCollection = objectCollectionProvider_.Get();

    var taskList = new List<Task>();

    var idx = 0;
    await foreach (var chunk in valueChunks.WithCancellation(cancellationToken)
                                           .ConfigureAwait(false))
    {
      size += chunk.Length;
      taskList.Add(objectCollection.InsertOneAsync(new ObjectDataModelMapping
                                                   {
                                                     Chunk    = chunk.ToArray(),
                                                     ChunkIdx = idx,
                                                     Key      = dbKey,
                                                   },
                                                   cancellationToken: cancellationToken));
      ++idx;
    }

    // If there was no chunks, add an empty chunk, just so that it could be found in the future
    if (idx == 0)
    {
      taskList.Add(objectCollection.InsertOneAsync(new ObjectDataModelMapping
                                                   {
                                                     Chunk    = Array.Empty<byte>(),
                                                     ChunkIdx = idx,
                                                     Key      = dbKey,
                                                   },
                                                   cancellationToken: cancellationToken));
    }

    await taskList.WhenAll()
                  .ConfigureAwait(false);

    return size;
  }

  /// <inheritdoc />
  async IAsyncEnumerable<byte[]> IObjectStorage.GetValuesAsync(string                                     key,
                                                               [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    using var _                = logger_.LogFunction(objectStorageName_ + key);
    var       sessionHandle    = sessionProvider_.Get();
    var       objectCollection = objectCollectionProvider_.Get();

    var throwException = true;
    await foreach (var chunk in objectCollection.AsQueryable(sessionHandle)
                                                .Where(odm => odm.Key == objectStorageName_ + key)
                                                .OrderBy(odm => odm.ChunkIdx)
                                                .Select(odm => odm.Chunk)
                                                .ToAsyncEnumerable(cancellationToken)
                                                .ConfigureAwait(false))
    {
      throwException = false;
      yield return chunk;
    }

    if (throwException)
    {
      throw new ObjectDataNotFoundException($"Result {key} not found");
    }
  }

  /// <inheritdoc />
  public async Task TryDeleteAsync(IEnumerable<string> keys,
                                   CancellationToken   cancellationToken = default)

  {
    using var _                = logger_.LogFunction(objectStorageName_);
    var       objectCollection = objectCollectionProvider_.Get();

    var names = keys.Select(key => objectStorageName_ + key);

    await objectCollection.DeleteManyAsync(odm => names.Contains(odm.Key),
                                           cancellationToken)
                          .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<string> ListKeysAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    using var _                = logger_.LogFunction();
    var       sessionHandle    = sessionProvider_.Get();
    var       objectCollection = objectCollectionProvider_.Get();

    await foreach (var key in objectCollection.AsQueryable(sessionHandle)
                                              .Where(odm => odm.ChunkIdx == 0)
                                              .Select(odm => odm.Key)
                                              .ToAsyncEnumerable(cancellationToken)
                                              .ConfigureAwait(false))
    {
      yield return key;
    }
  }
}
