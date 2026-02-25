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
// but WITHOUT ANY WARRANTY; without even the implied warranty of
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
using Couchbase.Extensions.DependencyInjection;
using Couchbase.KeyValue;

using DnsClient.Internal;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using System.IO.Compression;
using System.Runtime.CompilerServices;
using System.Text;

namespace ArmoniK.Core.Adapters.Couchbase
{
  public class CouchbaseStorage : IObjectStorage
  {
    private readonly ILogger<CouchbaseStorage> logger_;
    private readonly IClusterProvider clusterProvider_;
    private readonly Options.CouchbaseSettings couchbaseSettings_;
    private readonly Options.CouchbaseStorage couchbaseStorageOptions_;
    private readonly Lazy<Task<ICouchbaseCollection>> collectionLazy_;
    private bool isInitialized_;


    private const int DefaultChunkDownloadSize = 8192; // 8KB
    /// <summary>
    ///   <see cref="IObjectStorage" /> implementation for Couchbase
    /// </summary>
    /// <param name="clusterProvider">Couchbase cluster provider from DI</param>
    /// <param name="couchbaseSettings">Couchbase connection settings</param>
    /// <param name="couchbaseStorageOptions">Couchbase object storage options</param>
    /// <param name="logger">Logger used to print logs</param>
    public CouchbaseStorage(IClusterProvider clusterProvider,
                     Options.CouchbaseStorage couchbaseStorageOptions,
                     Options.CouchbaseSettings couchbaseSettings,
                     ILogger<CouchbaseStorage> logger)
    {
      clusterProvider_ = clusterProvider;
      couchbaseStorageOptions_ = couchbaseStorageOptions;
      couchbaseSettings_ = couchbaseSettings;
      logger_ = logger;
      
      collectionLazy_ = new Lazy<Task<ICouchbaseCollection>>(async () =>
      {
        logger_.LogDebug("Initializing Couchbase bucket and collection (lazy)");
        var cluster = await clusterProvider_.GetClusterAsync().ConfigureAwait(false);
        var bucket = await cluster.BucketAsync(couchbaseStorageOptions_.BucketName).ConfigureAwait(false);
        var scope = bucket.Scope(couchbaseStorageOptions_.ScopeName);
        var collection = scope.Collection(couchbaseStorageOptions_.CollectionName);
        logger_.LogDebug("Couchbase collection initialized: {Bucket}/{Scope}/{Collection}",
                        couchbaseStorageOptions_.BucketName,
                        couchbaseStorageOptions_.ScopeName,
                        couchbaseStorageOptions_.CollectionName);
        return collection;
      });
    }

    /// <summary>
    /// Gets the cached collection instance
    /// </summary>
    private Task<ICouchbaseCollection> GetCollectionAsync() => collectionLazy_.Value;

    public async Task<(byte[] id, long size)> AddOrUpdateAsync(ObjectData data, IAsyncEnumerable<ReadOnlyMemory<byte>> valueChunks, CancellationToken cancellationToken = default)
    {
      var key = Guid.NewGuid()
                  .ToString();
      long size = 0;
      
      var collection = await GetCollectionAsync().ConfigureAwait(false);

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
      var processedChunks = CouchbaseHelper.ProcessStreamAsync(key, TrackSizeAndPassThrough(cancellationToken), cancellationToken: cancellationToken);

      // Write to Couchbase with default concurrency
      var windowCount = await CouchbaseHelper.WriteStreamToCouchbaseAsync(collection, processedChunks, couchbaseStorageOptions_.DocumentTimeToLive, cancellationToken: cancellationToken).ConfigureAwait(false);

      // Store metadata using helper methods
      await CouchbaseHelper.StoreSizeMetadataAsync(collection, key, size, cancellationToken).ConfigureAwait(false);
      await CouchbaseHelper.StoreWindowCountMetadataAsync(collection, key, windowCount, cancellationToken).ConfigureAwait(false);

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
        logger_.LogInformation("Initializing Couchbase storage - getting cluster...");
        var cluster = await clusterProvider_.GetClusterAsync().ConfigureAwait(false);

        try
        {
          logger_.LogInformation("Opening bucket: {Bucket} and waiting until ready with timeout {BootstrapTimeout}",
                                couchbaseStorageOptions_.BucketName,
                                couchbaseSettings_.BootstrapTimeout);

          var bucket = await cluster.BucketAsync(couchbaseStorageOptions_.BucketName).ConfigureAwait(false);

          await bucket.WaitUntilReadyAsync(couchbaseSettings_.BootstrapTimeout).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
          logger_.LogError(ex, "Failed to open or wait for bucket ready. Bucket={Bucket}, BootstrapTimeout={BootstrapTimeout}",
                          couchbaseStorageOptions_.BucketName,
                          couchbaseSettings_.BootstrapTimeout);
          throw;
        }

        logger_.LogInformation("Initializing collection: {Bucket}/{Scope}/{Collection}",
                              couchbaseStorageOptions_.BucketName,
                              couchbaseStorageOptions_.ScopeName,
                              couchbaseStorageOptions_.CollectionName);
        _ = await GetCollectionAsync().ConfigureAwait(false);
        logger_.LogInformation("Collection initialized successfully");
      }

      isInitialized_ = true;
    }

    public async IAsyncEnumerable<byte[]> GetValuesAsync(byte[] id, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
      var collection = await GetCollectionAsync().ConfigureAwait(false);

      var key = Encoding.UTF8.GetString(id);

      // Get window count from metadata using helper method
      var windowCount = await CouchbaseHelper.GetWindowCountAsync(collection, key, cancellationToken).ConfigureAwait(false);
      
      logger_.LogInformation("GetValuesAsync: Found {WindowCount} windows for key {Key}", windowCount, key);
      
      // Fetch all windows in parallel with batching using helper method
      var windows = await CouchbaseHelper.FetchWindowsAsync(collection, key, windowCount, cancellationToken).ConfigureAwait(false);
      
      // Yield decompressed data from each window sequentially
      for (int windowIndex = 0; windowIndex < windowCount; windowIndex++)
      {
        if (windows.TryGetValue(windowIndex, out var recombined))
        {
          // Decompress this window and yield its data
          await foreach (var chunk in CouchbaseHelper.DecompressWindowAsync(recombined, DefaultChunkDownloadSize, cancellationToken).ConfigureAwait(false))
          {
            yield return chunk;
          }
        }
        else
        {
          logger_.LogWarning("GetValuesAsync: Window {WindowIndex} not found for key {Key}", windowIndex, key);
          throw new ObjectDataNotFoundException($"Window {windowIndex} not found for key {key}");
        }
      }
    }

    public async Task TryDeleteAsync(IEnumerable<byte[]> ids, CancellationToken cancellationToken = default)
      => await ids.ParallelForEach(id => TryDeleteAsync(id, cancellationToken))
                  .ConfigureAwait(false);

    public async Task<IDictionary<byte[], long?>> GetSizesAsync(IEnumerable<byte[]> ids, CancellationToken cancellationToken = default)
      => await ids.ParallelSelect(async id => KeyValuePair.Create(id, await GetSizeAsync(id, cancellationToken)
                                                                             .ConfigureAwait(false)))
                  .ToDictionaryAsync(kvp => kvp.Key,
                                     kvp => kvp.Value,
                                     cancellationToken)
                  .ConfigureAwait(false);

    private async Task<long?> GetSizeAsync(byte[] id, CancellationToken cancellationToken)
    {
      try
      {
        var key = Encoding.UTF8.GetString(id);
        var collection = await GetCollectionAsync().ConfigureAwait(false);
        
        // Get the size from metadata
        var sizeMetadataKey = CouchbaseHelper.GetSizeMetadataKey(key);
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
        var collection = await GetCollectionAsync().ConfigureAwait(false);
        
        // Get window count from metadata
        var windowCount = await CouchbaseHelper.GetWindowCountAsync(collection, key, cancellationToken).ConfigureAwait(false);
        
        logger_.LogInformation("TryDeleteAsync: Deleting {WindowCount} windows for key {Key}", windowCount, key);
        
        // Collect all deletion keys in parallel using helper method
        var allKeysToDelete = await CouchbaseHelper.CollectWindowDeletionKeysAsync(collection, key, windowCount, cancellationToken).ConfigureAwait(false);
        
        // Add metadata keys to deletion list
        var windowCountMetadataKey = CouchbaseHelper.GetWindowCountMetadataKey(key);
        allKeysToDelete.Add(windowCountMetadataKey);
        
        var sizeMetadataKey = CouchbaseHelper.GetSizeMetadataKey(key);
        var sizeResult = await collection.TryGetAsync(sizeMetadataKey, options => options.Transcoder(new LegacyTranscoder())).ConfigureAwait(false);
        if (sizeResult.Exists)
        {
          allKeysToDelete.Add(sizeMetadataKey);
        }
        
        // Execute deletions in parallel batches using helper method
        await CouchbaseHelper.ExecuteBatchDeletionsAsync(collection, allKeysToDelete, cancellationToken).ConfigureAwait(false);
        
        logger_.LogInformation("Deleted data with key {Key}, total operations: {Count}", key, allKeysToDelete.Count);
      }
      catch (Exception ex)
      {
        logger_.LogWarning(ex, "Error deleting key, continuing");
      }
    }
  }
}
