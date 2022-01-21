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

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Object;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Storage;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

using MongoDB.Driver;
using MongoDB.Driver.Linq;

namespace ArmoniK.Core.Adapters.MongoDB;

[PublicAPI]
public class ObjectStorage : IObjectStorage
{
  public int ChunkSize { get; }

  private readonly ILogger<ObjectStorage>                   logger_;
  private readonly MongoCollectionProvider<ObjectDataModel> objectCollectionProvider_;
  private readonly SessionProvider                          sessionProvider_;

  public ObjectStorage(SessionProvider                          sessionProvider,
                       MongoCollectionProvider<ObjectDataModel> objectCollectionProvider,
                       ILogger<ObjectStorage>                   logger,
                       Options.ObjectStorage                    options)
  {
    if (options.ChunkSize == 0)
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"Minimum value for {nameof(Options.ObjectStorage.ChunkSize)} is 1.");

    sessionProvider_          = sessionProvider;
    objectCollectionProvider_ = objectCollectionProvider;
    ChunkSize                 = options.ChunkSize;
    logger_                   = logger;
  }


  /// <inheritdoc />
  public async Task AddOrUpdateAsync(string key, IAsyncEnumerable<byte[]> valueChunks, CancellationToken cancellationToken = default)
  {
    using var _                = logger_.LogFunction(key);
    var       objectCollection = await objectCollectionProvider_.GetAsync();

    var taskList = new List<Task>();

    var idx = 0;
    await foreach (var chunk in valueChunks.WithCancellation(cancellationToken))
    {
      taskList.Add(objectCollection.InsertOneAsync(new()
                                                   {
                                                     Chunk    = chunk,
                                                     ChunkIdx = idx,
                                                     Key      = key,
                                                   },
                                                   cancellationToken: cancellationToken));
      ++idx;
    }

    await Task.WhenAll(taskList);
  }

  /// <inheritdoc />
  public async Task AddOrUpdateAsync(string key, IAsyncEnumerable<ReadOnlyMemory<byte>> valueChunks, CancellationToken cancellationToken = default)
  {
    using var _                = logger_.LogFunction(key);
    var       objectCollection = await objectCollectionProvider_.GetAsync();

    var taskList = new List<Task>();

    var idx = 0;
    await foreach (var chunk in valueChunks.WithCancellation(cancellationToken))
    {
      taskList.Add(objectCollection.InsertOneAsync(new()
                                                   {
                                                     Chunk    = chunk.ToArray(),
                                                     ChunkIdx = idx,
                                                     Key      = key,
                                                   },
                                                   cancellationToken: cancellationToken));
      ++idx;
    }

    await Task.WhenAll(taskList);
  }

  /// <inheritdoc />
  async IAsyncEnumerable<byte[]> IObjectStorage.TryGetValuesAsync(string key, [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    using var _                = logger_.LogFunction(key);
    var       sessionHandle    = await sessionProvider_.GetAsync();
    var       objectCollection = await objectCollectionProvider_.GetAsync();



    await foreach (var chunk in objectCollection.AsQueryable(sessionHandle)
                                                .Where(odm => odm.Key == key)
                                                .OrderBy(odm => odm.ChunkIdx)
                                                .Select(odm => odm.Chunk)
                                                .ToAsyncEnumerable()
                                                .WithCancellation(cancellationToken))
      yield return chunk;
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
  public async IAsyncEnumerable<string> ListKeysAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    using var _                = logger_.LogFunction();
    var       sessionHandle    = await sessionProvider_.GetAsync();
    var       objectCollection = await objectCollectionProvider_.GetAsync();

    await foreach (var key in objectCollection.AsQueryable(sessionHandle)
                                              .Where(odm => odm.ChunkIdx == 0)
                                              .Select(odm => odm.Key)
                                              .ToAsyncEnumerable()
                                              .WithCancellation(cancellationToken))
      yield return key;

  }
}