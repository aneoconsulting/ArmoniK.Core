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

using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;

using Microsoft.Extensions.Logging;

using StackExchange.Redis;

namespace ArmoniK.Core.Adapters.Redis;

public class ObjectStorage : IObjectStorage
{
  private readonly ILogger<ObjectStorage> logger_;
  private readonly string                 objectStorageName_;
  private readonly IDatabaseAsync         redis_;

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
    using var _ = logger_.LogFunction(objectStorageName_ + key);

    var idx      = 0;
    var taskList = new List<Task>();
    await foreach (var chunk in valueChunks.WithCancellation(cancellationToken)
                                           .ConfigureAwait(false))
    {
      taskList.Add(redis_.StringSetAsync(objectStorageName_ + key + "_" + idx,
                                         chunk));
      ++idx;
    }

    if (idx == 0)
    {
      throw new ArmoniKException($"{nameof(valueChunks)} should contain at least one chunk");
    }

    await redis_.StringSetAsync(objectStorageName_ + key + "_count",
                                idx)
                .ConfigureAwait(false);
    await taskList.WhenAll()
                  .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task AddOrUpdateAsync(string                                 key,
                                     IAsyncEnumerable<ReadOnlyMemory<byte>> valueChunks,
                                     CancellationToken                      cancellationToken = default)
  {
    using var _ = logger_.LogFunction(objectStorageName_ + key);

    var idx      = 0;
    var taskList = new List<Task>();
    await foreach (var chunk in valueChunks.WithCancellation(cancellationToken)
                                           .ConfigureAwait(false))
    {
      taskList.Add(redis_.StringSetAsync(objectStorageName_ + key + "_" + idx,
                                         chunk));
      ++idx;
    }

    if (idx == 0)
    {
      throw new ArmoniKException($"{nameof(valueChunks)} should contain at least one chunk");
    }

    await redis_.StringSetAsync(objectStorageName_ + key + "_count",
                                idx)
                .ConfigureAwait(false);
    await taskList.WhenAll()
                  .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<byte[]> GetValuesAsync(string                                     key,
                                                       [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    using var _ = logger_.LogFunction(objectStorageName_ + key);
    var value = await redis_.StringGetAsync(objectStorageName_ + key + "_count")
                            .ConfigureAwait(false);

    if (!value.HasValue)
    {
      throw new ObjectDataNotFoundException($"Key not found");
    }

    var valuesCount = int.Parse(value);

    if (valuesCount == 0)
    {
      yield break;
    }

    foreach (var chunkTask in Enumerable.Range(0,
                                                valuesCount)
                                         .Select(index => redis_.StringGetAsync(objectStorageName_ + key + "_" + index))
                                         .ToList())
    {
      yield return await chunkTask.ConfigureAwait(false);
    }
  }

  /// <inheritdoc />
  public async Task<bool> TryDeleteAsync(string            key,
                                         CancellationToken cancellationToken = default)
  {
    using var _ = logger_.LogFunction(objectStorageName_ + key);
    return await redis_.KeyDeleteAsync(new RedisKey(objectStorageName_ + key))
                       .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public IAsyncEnumerable<string> ListKeysAsync(CancellationToken cancellationToken = default)
    => throw new NotImplementedException();
}
