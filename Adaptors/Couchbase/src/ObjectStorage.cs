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

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Base.Exceptions;
using ArmoniK.Utils;

using Couchbase;
using Couchbase.Core.IO.Transcoders;
using Couchbase.KeyValue;

using DnsClient.Internal;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using System.IO.Compression;
using System.Runtime.CompilerServices;
using System.Text;

namespace ArmoniK.Core.Adapters.Couchbase
{
  public class ObjectStorage : IObjectStorage
  {
    private readonly ILogger<ObjectStorage> logger_;
    private readonly ICluster couchbase_;
    private readonly Options.Couchbase couchbaseOptions_;
    private bool isInitialized_;


    private const int DefaultChunkDownloadSize = 8192; // 8KB
    /// <summary>
    ///   <see cref="IObjectStorage" /> implementation for Couchbase
    /// </summary>
    /// <param name="redis">Connection to Couchbase database</param>
    /// <param name="redisOptions">Couchbase object storage options</param>
    /// <param name="logger">Logger used to print logs</param>
    public ObjectStorage(ICluster couchbase,
                         Options.Couchbase couchbaseOptions,
                         ILogger<ObjectStorage> logger)
    {
      couchbase_ = couchbase;
      couchbaseOptions_ = couchbaseOptions;
      logger_ = logger;
    }

    public async Task<(byte[] id, long size)> AddOrUpdateAsync(ObjectData data, IAsyncEnumerable<ReadOnlyMemory<byte>> valueChunks, CancellationToken cancellationToken = default)
    {
      var key = Guid.NewGuid()
                  .ToString();
      long size = 0;
      
      var bucket = await couchbase_.BucketAsync(couchbaseOptions_.BucketName).ConfigureAwait(false);

      var scope = bucket.Scope(couchbaseOptions_.ScopeName);

      var collection = scope.Collection(couchbaseOptions_.CollectionName);

      // Create a wrapper that tracks size while passing through chunks
      async IAsyncEnumerable<ReadOnlyMemory<byte>> TrackSizeAndPassThrough([EnumeratorCancellation] CancellationToken ct = default)
      {
        await foreach (var chunk in valueChunks.WithCancellation(ct).ConfigureAwait(false))
        {
          size += chunk.Length;
          yield return chunk;
        }
      }
      
      logger_.LogInformation("AddOrUpdateAsync: Starting to process chunks for key {Key}", key);
      
      // Process and write the chunks with size tracking
      var processedChunks = ObjectHandler.ProcessStreamAsync(key, TrackSizeAndPassThrough(cancellationToken), cancellationToken: cancellationToken);
      
      var windowCount = await ObjectHandler.WriteStreamToCouchbaseAsync(collection, processedChunks, cancellationToken).ConfigureAwait(false);

      // Store the original size as metadata in a separate document
      var sizeMetadataKey = ObjectHandler.GetSizeMetadataKey(key);
      var sizeBytes = BitConverter.GetBytes(size);
      await collection.UpsertAsync(sizeMetadataKey, sizeBytes, options => options.Transcoder(new LegacyTranscoder())).ConfigureAwait(false);

      // Store the window count as metadata
      var windowCountMetadataKey = ObjectHandler.GetWindowCountMetadataKey(key);
      var windowCountBytes = BitConverter.GetBytes(windowCount);
      await collection.UpsertAsync(windowCountMetadataKey, windowCountBytes, options => options.Transcoder(new LegacyTranscoder())).ConfigureAwait(false);

      logger_.LogInformation("AddOrUpdateAsync: Completed for key {Key}, total uncompressed size = {Size}, window count = {WindowCount}", key, size, windowCount);

      return (Encoding.UTF8.GetBytes(key), size);
    }

    public Task<HealthCheckResult> Check(HealthCheckTag tag)
    =>
      tag switch
      {
        HealthCheckTag.Startup or HealthCheckTag.Readiness => Task.FromResult(isInitialized_
                                                                        ? HealthCheckResult.Healthy()
                                                                        : HealthCheckResult.Unhealthy("Couchbase not initialized yet.")),
        HealthCheckTag.Liveness => Task.FromResult(isInitialized_ ? HealthCheckResult.Healthy()
                                                     : HealthCheckResult.Unhealthy("Couchbase not initialized or connection dropped.")),
        _ => throw new ArgumentOutOfRangeException(nameof(tag),
                                                   tag,
                                                   null),
      };
    public async Task Init(CancellationToken cancellationToken)
    {
      if (!isInitialized_)
      {
        await couchbase_.PingAsync()
                    .ConfigureAwait(false);
      }

      isInitialized_ = true;
    }

    public async IAsyncEnumerable<byte[]> GetValuesAsync(byte[] id, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
      var bucket = await couchbase_.BucketAsync(couchbaseOptions_.BucketName);

      var scope = bucket.Scope(couchbaseOptions_.ScopeName);

      var collection = scope.Collection(couchbaseOptions_.CollectionName);

      var key = Encoding.UTF8.GetString(id);

      // Get window count metadata
      var windowCountMetadataKey = ObjectHandler.GetWindowCountMetadataKey(key);
      var windowCountResult = await collection.TryGetAsync(windowCountMetadataKey, options => options.Transcoder(new LegacyTranscoder())).ConfigureAwait(false);
      
      if (!windowCountResult.Exists)
      {
        throw new ObjectDataNotFoundException($"Window count metadata not found for key {key}");
      }
      
      // Read all windows sequentially
      var windowCountBytes = windowCountResult.ContentAs<byte[]>();
      var windowCount = BitConverter.ToInt32(windowCountBytes, 0);
      
      logger_.LogInformation("GetValuesAsync: Found {WindowCount} windows for key {Key}", windowCount, key);
      
      for (int windowIndex = 0; windowIndex < windowCount; windowIndex++)
      {
        var windowKey = $"{key}{ObjectHandler.ChunkSeparator}w{windowIndex}";
        var internalKey = ObjectHandler.GetInternalKey(windowKey);
        
        var result = await collection.TryGetAsync(internalKey, o => o.Transcoder(new LegacyTranscoder())).ConfigureAwait(false);
        
        if (!result.Exists)
        {
          logger_.LogWarning("GetValuesAsync: Window {WindowIndex} not found for key {Key}", windowIndex, key);
          throw new ObjectDataNotFoundException($"Window {windowIndex} not found for key {key}");
        }
        
        var mainChunk = result.ContentAs<byte[]>();
        var recombined = await ObjectHandler.RecombineAsync(collection, windowKey, mainChunk, cancellationToken).ConfigureAwait(false);
        
        // Decompress this window and yield its data
        await foreach (var chunk in ObjectHandler.DecompressWindowAsync(recombined, DefaultChunkDownloadSize, cancellationToken).ConfigureAwait(false))
        {
          yield return chunk;
        }
      }
    }

    public async Task TryDeleteAsync(IEnumerable<byte[]> ids, CancellationToken cancellationToken = default)
      => await ids.ParallelForEach(id => TryDeleteAsync(id, cancellationToken))
                  .ConfigureAwait(false);

    public async Task<IDictionary<byte[], long?>> GetSizesAsync(IEnumerable<byte[]> ids, CancellationToken cancellationToken = default)
      => await ids.ParallelSelect(async id => (id, await GetSizeAsync(id, cancellationToken)
                                                           .ConfigureAwait(false)))
                  .ToDictionaryAsync(tuple => tuple.id,
                                     tuple => tuple.Item2,
                                     cancellationToken)
                  .ConfigureAwait(false);

    private async Task<long?> GetSizeAsync(byte[] id, CancellationToken cancellationToken)
    {
      try
      {
        var key = Encoding.UTF8.GetString(id);
        var bucket = await couchbase_.BucketAsync(couchbaseOptions_.BucketName).ConfigureAwait(false);
        var scope = bucket.Scope(couchbaseOptions_.ScopeName);
        var collection = scope.Collection(couchbaseOptions_.CollectionName);
        
        // Get the size from metadata
        var sizeMetadataKey = ObjectHandler.GetSizeMetadataKey(key);
        var sizeResult = await collection.TryGetAsync(sizeMetadataKey, options => options.Transcoder(new LegacyTranscoder())).ConfigureAwait(false);
        
        if (!sizeResult.Exists)
        {
          logger_.LogWarning("GetSizeAsync: Size metadata not found for key {Key}", key);
          return null;
        }
        
        var sizeBytes = sizeResult.ContentAs<byte[]>();
        var sizeValue = BitConverter.ToInt64(sizeBytes, 0);
        logger_.LogInformation("GetSizeAsync: Retrieved size = {Size} for key {Key}", sizeValue, key);
        return sizeValue;
      }
      catch (Exception ex)
      {
        logger_.LogWarning(ex, "Error getting size for key, returning null");
        return null;
      }
    }

    private async Task TryDeleteAsync(byte[] id, CancellationToken cancellationToken = default)
    {
      try
      {
        var key = Encoding.UTF8.GetString(id);
        var bucket = await couchbase_.BucketAsync(couchbaseOptions_.BucketName).ConfigureAwait(false);
        var scope = bucket.Scope(couchbaseOptions_.ScopeName);
        var collection = scope.Collection(couchbaseOptions_.CollectionName);
        
        var deleteTasks = new List<Task>();
        
        // Try to get window count metadata
        var windowCountMetadataKey = ObjectHandler.GetWindowCountMetadataKey(key);
        var windowCountResult = await collection.TryGetAsync(windowCountMetadataKey, options => options.Transcoder(new LegacyTranscoder())).ConfigureAwait(false);
        
        if (windowCountResult.Exists)
        {
          // Multi-window approach: Delete all windows
          var windowCountBytes = windowCountResult.ContentAs<byte[]>();
          var windowCount = BitConverter.ToInt32(windowCountBytes, 0);
          
          logger_.LogInformation("TryDeleteAsync: Deleting {WindowCount} windows for key {Key}", windowCount, key);
          
          for (int windowIndex = 0; windowIndex < windowCount; windowIndex++)
          {
            var windowKey = $"{key}{ObjectHandler.ChunkSeparator}w{windowIndex}";
            var internalKey = ObjectHandler.GetInternalKey(windowKey);
            
            var result = await collection.TryGetAsync(internalKey, o => o.Transcoder(new LegacyTranscoder())).ConfigureAwait(false);
            
            if (result.Exists)
            {
              var mainChunk = result.ContentAs<byte[]>();
              
              // Delete main window chunk
              deleteTasks.Add(collection.RemoveAsync(internalKey));
              
              // Delete any additional chunks for this window (if data > 4MB within this window)
              if (mainChunk != null && mainChunk.Length > ObjectHandler.MaxChunkSize)
              {
                var chunkCount = BitConverter.ToInt32(mainChunk, mainChunk.Length - 4);
                for (int i = 0; i < chunkCount; i++)
                {
                  var chunkKey = $"{windowKey}{ObjectHandler.ChunkSeparator}{i}";
                  var chunkInternalKey = ObjectHandler.GetInternalKey(chunkKey);
                  deleteTasks.Add(collection.RemoveAsync(chunkInternalKey));
                }
              }
            }
          }
          
          // Delete metadata
          deleteTasks.Add(collection.RemoveAsync(windowCountMetadataKey));
          
          var sizeMetadataKey = ObjectHandler.GetSizeMetadataKey(key);
          var sizeResult = await collection.TryGetAsync(sizeMetadataKey, options => options.Transcoder(new LegacyTranscoder())).ConfigureAwait(false);
          if (sizeResult.Exists)
          {
            deleteTasks.Add(collection.RemoveAsync(sizeMetadataKey));
          }
        }
        else
        {
          // Legacy approach: Single window
          logger_.LogInformation("TryDeleteAsync: Using legacy single-window deletion for key {Key}", key);
          
          var internalKey = ObjectHandler.GetInternalKey(key);
          var result = await collection.TryGetAsync(internalKey, o => o.Transcoder(new LegacyTranscoder())).ConfigureAwait(false);
          
          if (!result.Exists)
          {
            logger_.LogInformation("Key {Key} does not exist, skipping delete", key);
            return;
          }

          var mainChunk = result.ContentAs<byte[]>();
          
          // Delete main chunk
          deleteTasks.Add(collection.RemoveAsync(internalKey));
          
          // Delete additional chunks (if data was split into 4MB chunks)
          if (mainChunk != null && mainChunk.Length > ObjectHandler.MaxChunkSize)
          {
            var chunkCount = BitConverter.ToInt32(mainChunk, mainChunk.Length - 4);
            for (int i = 0; i < chunkCount; i++)
            {
              var chunkKey = $"{key}{ObjectHandler.ChunkSeparator}{i}";
              var chunkInternalKey = ObjectHandler.GetInternalKey(chunkKey);
              deleteTasks.Add(collection.RemoveAsync(chunkInternalKey));
            }
          }
          
          // Delete size metadata if it exists
          var sizeMetadataKey = ObjectHandler.GetSizeMetadataKey(key);
          var sizeResult = await collection.TryGetAsync(sizeMetadataKey, options => options.Transcoder(new LegacyTranscoder())).ConfigureAwait(false);
          if (sizeResult.Exists)
          {
            deleteTasks.Add(collection.RemoveAsync(sizeMetadataKey));
          }
        }
        
        if (deleteTasks.Count > 0)
        {
          await Task.WhenAll(deleteTasks).ConfigureAwait(false);
        }
        
        logger_.LogInformation("Deleted data with key {Key}, total operations: {Count}", key, deleteTasks.Count);
      }
      catch (Exception ex)
      {
        logger_.LogWarning(ex, "Error deleting key, continuing");
      }
    }
  }
}
