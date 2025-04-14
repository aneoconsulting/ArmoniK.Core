// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Base.Exceptions;
using ArmoniK.Utils;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using StackExchange.Redis;

namespace ArmoniK.Core.Adapters.Redis;

public class ObjectStorage : IObjectStorage
{
  private readonly ILogger<ObjectStorage> logger_;
  private readonly string                 objectStorageName_;
  private readonly IDatabaseAsync         redis_;
  private readonly Options.Redis          redisOptions_;
  private          bool                   isInitialized_;

  /// <summary>
  ///   <see cref="IObjectStorage" /> implementation for Redis
  /// </summary>
  /// <param name="redis">Connection to redis database</param>
  /// <param name="redisOptions">Redis object storage options</param>
  /// <param name="logger">Logger used to print logs</param>
  public ObjectStorage(IDatabaseAsync         redis,
                       Options.Redis          redisOptions,
                       ILogger<ObjectStorage> logger)
  {
    redis_             = redis;
    redisOptions_      = redisOptions;
    objectStorageName_ = "objectStorageName";
    logger_            = logger;
  }

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      await redis_.PingAsync()
                  .ConfigureAwait(false);
    }

    isInitialized_ = true;
  }

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => tag switch
       {
         HealthCheckTag.Startup or HealthCheckTag.Readiness => Task.FromResult(isInitialized_
                                                                                 ? HealthCheckResult.Healthy()
                                                                                 : HealthCheckResult.Unhealthy("Redis not initialized yet.")),
         HealthCheckTag.Liveness => Task.FromResult(isInitialized_ && redis_.Multiplexer.IsConnected
                                                      ? HealthCheckResult.Healthy()
                                                      : HealthCheckResult.Unhealthy("Redis not initialized or connection dropped.")),
         _ => throw new ArgumentOutOfRangeException(nameof(tag),
                                                    tag,
                                                    null),
       };

  /// <inheritdoc />
  public async Task<(byte[] id, long size)> AddOrUpdateAsync(ObjectData                             metaData,
                                                             IAsyncEnumerable<ReadOnlyMemory<byte>> valueChunks,
                                                             CancellationToken                      cancellationToken = default)
  {
    var key = Guid.NewGuid()
                  .ToString();
    var  storageNameKey = objectStorageName_ + key;
    long size           = 0;

    var idx      = 0;
    var taskList = new List<Task>();
    await foreach (var chunk in valueChunks.WithCancellation(cancellationToken)
                                           .ConfigureAwait(false))
    {
      size += chunk.Length;
      var storageNameKeyWithIndex = $"{storageNameKey}_{idx}";
      taskList.Add(PerformActionWithRetry(() => SetObjectAsync(storageNameKeyWithIndex,
                                                               chunk)));
      ++idx;
    }

    taskList.Add(PerformActionWithRetry(() => SetObjectAsync(storageNameKey + "_count",
                                                             idx)));
    await taskList.WhenAll()
                  .ConfigureAwait(false);

    return (Encoding.UTF8.GetBytes(key), size);
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<byte[]> GetValuesAsync(byte[]                                     id,
                                                       [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    var key = Encoding.UTF8.GetString(id);
    var value = await PerformActionWithRetry(() => redis_.StringGetAsync(objectStorageName_ + key + "_count"))
                  .ConfigureAwait(false);

    if (!value.HasValue)
    {
      throw new ObjectDataNotFoundException("Key not found");
    }

    var valuesCount = int.Parse(value!);

    if (valuesCount == 0)
    {
      yield break;
    }

    foreach (var chunkTask in Enumerable.Range(0,
                                               valuesCount)
                                        .Select(index => PerformActionWithRetry(() => redis_.StringGetAsync(objectStorageName_ + key + "_" + index)))
                                        .ToList())
    {
      yield return (await chunkTask.ConfigureAwait(false))!;
    }
  }

  /// <inheritdoc />
  public async Task TryDeleteAsync(IEnumerable<byte[]> ids,
                                   CancellationToken   cancellationToken = default)
    => await ids.ParallelForEach(id => TryDeleteAsync(id,
                                                      cancellationToken))
                .ConfigureAwait(false);

  /// <inheritdoc />
  public async Task<IDictionary<byte[], long?>> GetSizesAsync(IEnumerable<byte[]> ids,
                                                              CancellationToken   cancellationToken = default)
    => await ids.ParallelSelect(async id => (id, await ExistsAsync(id,
                                                                   cancellationToken)
                                                   .ConfigureAwait(false)))
                .ToDictionaryAsync(tuple => tuple.id,
                                   tuple => tuple.Item2,
                                   cancellationToken)
                .ConfigureAwait(false);

  private async Task<long?> ExistsAsync(byte[]            id,
                                        CancellationToken cancellationToken)
  {
    var key = Encoding.UTF8.GetString(id);


    var value = await PerformActionWithRetry(() => redis_.StringGetAsync(objectStorageName_ + key + "_count"))
                  .ConfigureAwait(false);

    if (!value.HasValue)
    {
      return null;
    }

    var valuesCount = int.Parse(value!);
    var keys = Enumerable.Range(0,
                                valuesCount)
                         .Select(index => new RedisKey(objectStorageName_ + key + "_" + index));
    long count = 0;

    foreach (var redisKey in keys)
    {
      count += await PerformActionWithRetry(() => redis_.StringLengthAsync(redisKey))
                 .ConfigureAwait(false);
    }

    return count;
  }

  private async Task TryDeleteAsync(byte[]            id,
                                    CancellationToken cancellationToken = default)
  {
    var key = Encoding.UTF8.GetString(id);

    var value = await PerformActionWithRetry(() => redis_.StringGetAsync(objectStorageName_ + key + "_count"))
                  .ConfigureAwait(false);

    if (!value.HasValue)
    {
      return;
    }

    var valuesCount = int.Parse(value!);
    var keyList = Enumerable.Range(0,
                                   valuesCount)
                            .Select(index => new RedisKey(objectStorageName_ + key + "_" + index))
                            .Concat(new[]
                                    {
                                      new RedisKey(objectStorageName_ + key + "_count"),
                                    })
                            .ToArray();

    await PerformActionWithRetry(() => redis_.KeyDeleteAsync(keyList))
      .ConfigureAwait(false);
    logger_.LogInformation("Deleted data with {resultId}",
                           key);
  }

  private async Task<T> PerformActionWithRetry<T>(Func<Task<T>> action)
  {
    for (var retryCount = 0; retryCount < redisOptions_.MaxRetry; retryCount++)
    {
      try
      {
        return await action()
                 .ConfigureAwait(false);
      }
      catch (Exception ex) when (ex is RedisTimeoutException or RedisConnectionException)
      {
        if (retryCount + 1 >= redisOptions_.MaxRetry)
        {
          logger_.LogError(ex,
                           "A RedisTimeoutException occurred {retryCount} times for the same action",
                           redisOptions_.MaxRetry);
          throw;
        }

        var retryDelay = (retryCount + 1) * (retryCount + 1) * redisOptions_.MsAfterRetry;
        logger_.LogWarning(ex,
                           "A RedisTimeoutException occurred {retryCount}/{MaxRetry}, retry in {retryDelay} ms",
                           retryCount,
                           redisOptions_.MaxRetry,
                           retryDelay);
        await Task.Delay(retryDelay)
                  .ConfigureAwait(false);
      }
    }

    throw new RedisTimeoutException("A RedisTimeoutException occurred",
                                    CommandStatus.Unknown);
  }

  private Task<bool> SetObjectAsync(string     key,
                                    RedisValue chunk)
  {
    if (redisOptions_.TtlTimeSpan <= TimeSpan.Zero || redisOptions_.TtlTimeSpan == TimeSpan.MaxValue)
    {
      return redis_.StringSetAsync(key,
                                   chunk);
    }

    return redis_.StringSetAsync(key,
                                 chunk,
                                 redisOptions_.TtlTimeSpan);
  }
}
