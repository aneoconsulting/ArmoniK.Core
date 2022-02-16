// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
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

using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Logging;

using StackExchange.Redis;

namespace ArmoniK.Core.Adapters.Redis;

public class ObjectStorage : IObjectStorage
{
  private readonly IDatabaseAsync         redis_;
  private readonly string                 objectStorageName_;
  private readonly ILogger<ObjectStorage> logger_;

  public ObjectStorage(IDatabaseAsync redis, string objectStorageName, ILogger<ObjectStorage> logger)
  {
    redis_             = redis;
    objectStorageName_ = objectStorageName;
    logger_            = logger;
  }

  /// <inheritdoc />
  public async Task AddOrUpdateAsync(string key, IAsyncEnumerable<byte[]> valueChunks, CancellationToken cancellationToken = default)
  {
    using var _ = logger_.LogFunction(objectStorageName_ + key);
    await Task.WhenAll(await valueChunks.Select((chunk, i) =>
    {
      logger_.LogTrace("Add {key} {value}",
                       objectStorageName_ + key + i,
                       chunk);
      return redis_.SetAddAsync(objectStorageName_ + key,
                                chunk);
    }).ToListAsync(cancellationToken));
  }

  /// <inheritdoc />
  public async Task AddOrUpdateAsync(string key, IAsyncEnumerable<ReadOnlyMemory<byte>> valueChunks, CancellationToken cancellationToken = default)
  {
    using var _ = logger_.LogFunction(objectStorageName_ + key);
    await Task.WhenAll(await valueChunks.Select((chunk, i) =>
    {
      logger_.LogTrace("Add {key} {value}",
                       objectStorageName_ + key + i, chunk);
      return redis_.SetAddAsync(objectStorageName_ + key,
                                chunk);
    }).ToListAsync(cancellationToken));
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<byte[]> TryGetValuesAsync(string key, [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    using var _   = logger_.LogFunction(objectStorageName_ + key);
    var res = await redis_.SetMembersAsync(new RedisKey(objectStorageName_ + key));
    foreach (var redisValue in res)
    {
      cancellationToken.ThrowIfCancellationRequested();
      yield return redisValue;
    }
  }

  /// <inheritdoc />
  public async Task<bool> TryDeleteAsync(string key, CancellationToken cancellationToken = default)
  {
    using var _ = logger_.LogFunction(objectStorageName_ + key);
    return await redis_.KeyDeleteAsync(new RedisKey(objectStorageName_ + key));
  }

  /// <inheritdoc />
  public IAsyncEnumerable<string> ListKeysAsync(CancellationToken cancellationToken = default)
    => throw new NotImplementedException();
}