// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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
using ArmoniK.Core.Utils;
using ArmoniK.Core.Utils.Uuid;
using ArmoniK.Utils;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using StackExchange.Redis;

namespace ArmoniK.Core.Adapters.Redis;

/// <summary>
///   <see cref="IObjectStorage" /> implementation for Redis
/// </summary>
public class ObjectStorage : IObjectStorage
{
  private readonly ILogger<ObjectStorage> logger_;
  private readonly Options.Redis          redisOptions_;
  private readonly IDatabaseAsync         redis_;
  private readonly IUuidGenerator         uuidGenerator_;
  private          bool                   isInitialized_;

  /// <summary>
  ///   <see cref="IObjectStorage" /> implementation for Redis
  /// </summary>
  /// <param name="redis">Connection to redis database</param>
  /// <param name="redisOptions">Redis object storage options</param>
  /// <param name="uuidGenerator">Generator of UUIDs</param>
  /// <param name="logger">Logger used to print logs</param>
  public ObjectStorage(IDatabaseAsync redis,
                       Options.Redis  redisOptions,
                       [FromKeyedServices(UuidServiceKey.Uniform)]
                       IUuidGenerator uuidGenerator,
                       ILogger<ObjectStorage> logger)
  {
    redis_         = redis;
    redisOptions_  = redisOptions;
    uuidGenerator_ = uuidGenerator;
    logger_        = logger;
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
    var key = uuidGenerator_.GenerateUuid()
                            .ToString();
    var  storageNameKey = redisOptions_.KeyPrefix + key;
    long size           = 0;
    var  count          = 0;

    await using var cleanup = new Deferrer(Cleanup);

    var upload = valueChunks.Select((chunk, index) =>
                                    {
                                      count = index + 1;
                                      return (chunk, index);
                                    })
                            .ParallelSelect(new ParallelTaskOptions
                                            {
                                              ParallelismLimit  = redisOptions_.DegreeOfParallelism,
                                              CancellationToken = cancellationToken,
                                              Unordered         = true,
                                            },
                                            async indexedChunk =>
                                            {
                                              var (chunk, index) = indexedChunk;

                                              var storageNameKeyWithIndex = $"{storageNameKey}_{index}";

                                              await PerformActionWithRetry(() => SetObjectAsync(storageNameKeyWithIndex,
                                                                                                chunk),
                                                                           cancellationToken)
                                                .ConfigureAwait(false);

                                              return chunk.Length;
                                            });

    await foreach (var chunk in upload.WithCancellation(cancellationToken)
                                      .ConfigureAwait(false))
    {
      size += chunk;
    }

    await PerformActionWithRetry(() => SetObjectAsync(storageNameKey + "_count",
                                                      count),
                                 cancellationToken)
      .ConfigureAwait(false);

    // Disengage cleanup now that upload has been successfully completed
    cleanup.Reset();

    return (Encoding.UTF8.GetBytes(key), size);

    ValueTask Cleanup()
    {
      var keyList = Enumerable.Range(0,
                                     count)
                              .Select(index => new RedisKey($"{storageNameKey}_{index}"))
                              .Concat(new[]
                                      {
                                        new RedisKey($"{storageNameKey}_count"),
                                      })
                              .ToArray();

      return new ValueTask(PerformActionWithRetry(() => redis_.KeyDeleteAsync(keyList),
                                                  CancellationToken.None));
    }
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<byte[]> GetValuesAsync(byte[]                                     id,
                                                       [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    var key = Encoding.UTF8.GetString(id);
    var value = await PerformActionWithRetry(() => redis_.StringGetAsync(redisOptions_.KeyPrefix + key + "_count"),
                                             cancellationToken)
                  .ConfigureAwait(false);

    if (!value.HasValue)
    {
      throw new ObjectDataNotFoundException($"Header Key not found in Redis: `{key}`");
    }

    var valuesCount = int.Parse(value!);

    if (valuesCount == 0)
    {
      yield break;
    }

    var download = Enumerable.Range(0,
                                    valuesCount)
                             .ParallelSelect(new ParallelTaskOptions
                                             {
                                               ParallelismLimit  = redisOptions_.DegreeOfParallelism,
                                               CancellationToken = cancellationToken,
                                               Unordered         = false,
                                             },
                                             async index =>
                                             {
                                               var chunk = await PerformActionWithRetry(() => redis_.StringGetAsync(redisOptions_.KeyPrefix + key + "_" + index),
                                                                                        cancellationToken)
                                                             .ConfigureAwait(false);

                                               return (byte[]?)chunk switch
                                                      {
                                                        null      => throw new ObjectDataNotFoundException($"Chunk Key not found in Redis: `{key}_{index}`"),
                                                        var bytes => bytes,
                                                      };
                                             });

    await foreach (var chunk in download.WithCancellation(cancellationToken)
                                        .ConfigureAwait(false))
    {
      yield return chunk;
    }
  }

  /// <inheritdoc />
  public Task TryDeleteAsync(IEnumerable<byte[]> ids,
                             CancellationToken   cancellationToken = default)
    => ids.ParallelForEach(new ParallelTaskOptions
                           {
                             ParallelismLimit  = redisOptions_.DegreeOfParallelism,
                             CancellationToken = cancellationToken,
                             Unordered         = true,
                           },
                           id => TryDeleteAsync(id,
                                                cancellationToken));

  /// <inheritdoc />
  public Task<IDictionary<byte[], long?>> GetSizesAsync(IEnumerable<byte[]> ids,
                                                        CancellationToken   cancellationToken = default)
    => ids.ParallelSelect(new ParallelTaskOptions
                          {
                            ParallelismLimit  = redisOptions_.DegreeOfParallelism,
                            CancellationToken = cancellationToken,
                            Unordered         = true,
                          },
                          async id => (id, await ExistsAsync(id,
                                                             cancellationToken)
                                             .ConfigureAwait(false)))
          .ToDictionaryAsync(tuple => tuple.id,
                             tuple => tuple.Item2,
                             new ByteArrayComparer(),
                             cancellationToken)
          .AndThen(static IDictionary<byte[], long?> (dict) => dict)
          .AsTask();

  private async Task<long?> ExistsAsync(byte[]            id,
                                        CancellationToken cancellationToken)
  {
    var key = Encoding.UTF8.GetString(id);


    var value = await PerformActionWithRetry(() => redis_.StringGetAsync(redisOptions_.KeyPrefix + key + "_count"),
                                             cancellationToken)
                  .ConfigureAwait(false);

    if (!value.HasValue)
    {
      return null;
    }

    var valuesCount = int.Parse(value!);
    var keys = Enumerable.Range(0,
                                valuesCount)
                         .Select(index => new RedisKey(redisOptions_.KeyPrefix + key + "_" + index));
    long count = 0;

    foreach (var redisKey in keys)
    {
      count += await PerformActionWithRetry(() => redis_.StringLengthAsync(redisKey),
                                            cancellationToken)
                 .ConfigureAwait(false);
    }

    return count;
  }

  private async Task TryDeleteAsync(byte[]            id,
                                    CancellationToken cancellationToken = default)
  {
    var key = Encoding.UTF8.GetString(id);

    var value = await PerformActionWithRetry(() => redis_.StringGetAsync(redisOptions_.KeyPrefix + key + "_count"),
                                             cancellationToken)
                  .ConfigureAwait(false);

    if (!value.HasValue)
    {
      return;
    }

    var valuesCount = int.Parse(value!);
    var keyList = Enumerable.Range(0,
                                   valuesCount)
                            .Select(index => new RedisKey(redisOptions_.KeyPrefix + key + "_" + index))
                            .Concat(new[]
                                    {
                                      new RedisKey(redisOptions_.KeyPrefix + key + "_count"),
                                    })
                            .ToArray();

    await PerformActionWithRetry(() => redis_.KeyDeleteAsync(keyList),
                                 cancellationToken)
      .ConfigureAwait(false);
    logger_.LogInformation("Deleted data with {resultId}",
                           key);
  }

  private async Task<T> PerformActionWithRetry<T>(Func<Task<T>>     action,
                                                  CancellationToken cancellationToken)
  {
    for (var retryCount = 0; retryCount < redisOptions_.MaxRetry; retryCount++)
    {
      cancellationToken.ThrowIfCancellationRequested();
      try
      {
        return await action()
                     .WaitAsync(cancellationToken)
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
        await Task.Delay(retryDelay,
                         cancellationToken)
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
