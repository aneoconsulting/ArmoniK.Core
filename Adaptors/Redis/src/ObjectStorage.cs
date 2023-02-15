// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;

using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.Extensions.Logging;

using StackExchange.Redis;

namespace ArmoniK.Core.Adapters.Redis;

public class ObjectStorage : IObjectStorage
{
  private readonly ILogger<ObjectStorage> logger_;
  private readonly string                 objectStorageName_;
  private readonly IDatabaseAsync         redis_;

  /// <summary>
  ///   <see cref="IObjectStorage" /> implementation for Redis
  /// </summary>
  /// <param name="redis">Connection to redis database</param>
  /// <param name="objectStorageName">Name of the object storage used to differentiate them</param>
  /// <param name="logger">Logger used to print logs</param>
  public ObjectStorage(IDatabaseAsync         redis,
                       string                 objectStorageName,
                       ILogger<ObjectStorage> logger)
  {
    redis_             = redis;
    objectStorageName_ = objectStorageName;
    logger_            = logger;
  }

  /// <inheritdoc />
  public async Task AddOrUpdateAsync(string                   key,
                                     IAsyncEnumerable<byte[]> valueChunks,
                                     CancellationToken        cancellationToken = default)
  {
    var storageNameKey = objectStorageName_ + key;
    using var _ = logger_.LogFunction(storageNameKey);

    var taskList = new List<Task>();
    await foreach (var chunk in valueChunks.WithCancellation(cancellationToken)
                                           .ConfigureAwait(false))
    {
      taskList.Add(redis_.StringAppendAsync(storageNameKey, chunk));
    }
  }

  /// <inheritdoc />
  public async Task AddOrUpdateAsync(string                                 key,
                                     IAsyncEnumerable<ReadOnlyMemory<byte>> valueChunks,
                                     CancellationToken                      cancellationToken = default)
  {
    var storageNameKey = objectStorageName_ + key;

    using var _ = logger_.LogFunction(storageNameKey);

    var taskList = new List<Task>();
    await foreach (var chunk in valueChunks.WithCancellation(cancellationToken)
                                           .ConfigureAwait(false))
    {
      await redis_.StringAppendAsync(storageNameKey, chunk);
    }
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<byte[]> GetValuesAsync(string                                     key,
                                                       [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    var storageNameKey = objectStorageName_ + key;
    using var _ = logger_.LogFunction(storageNameKey);
    var value = await redis_.StringGetAsync(storageNameKey)
                            .ConfigureAwait(false);

    if (!value.HasValue)
    {
      throw new ObjectDataNotFoundException("Key not found");
    }

    var valuesCount = int.Parse(value!);

    yield return await redis_.StringGetAsync(storageNameKey);
  }

  /// <inheritdoc />
  public async Task<bool> TryDeleteAsync(string            key,
                                         CancellationToken cancellationToken = default)
  {
    return await redis_.KeyDeleteAsync(new RedisKey(objectStorageName_ + key))
                       .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public IAsyncEnumerable<string> ListKeysAsync(CancellationToken cancellationToken = default)
    => throw new NotImplementedException();
}
