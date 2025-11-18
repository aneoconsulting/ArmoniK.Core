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

using Couchbase.Client.Transactions.Config;
using Couchbase.Core.IO.Transcoders;
using Couchbase.KeyValue;

using System.IO.Compression;
using System.Runtime.CompilerServices;

namespace ArmoniK.Core.Adapters.Couchbase
{
  internal class ObjectHandler
  {
    public const int MaxChunkSize = 4 * 1024 * 1023; // ~4MB
    public const char ChunkSeparator = '`';
    private const int DefaultCompressionThreshold = 1024;


    private const string InternalKeyPrefix = "ArmoniKObject";

    /// <summary>
    /// Processes a data stream in windows to avoid full materialization.
    /// </summary>
    /// <param name="key">Base key for storing data</param>
    /// <param name="dataStream">Stream of data chunks</param>
    /// <param name="compressionThreshold">Min size for compression (default: 1024)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Processed entries with window metadata</returns>
    public static async IAsyncEnumerable<(KeyValuePair<string, byte[]> Entry, int WindowIndex, bool IsLastWindow)> ProcessStreamAsync(
     string key,
     IAsyncEnumerable<ReadOnlyMemory<byte>> dataStream,
     int compressionThreshold = DefaultCompressionThreshold,
     [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
      // Process in windows to avoid full materialization
      var buffer = new MemoryStream();
      var windowSize = 2 * MaxChunkSize;
      var windowIndex = 0;
      var hasMore = true;

      await foreach (var chunk in dataStream.WithCancellation(cancellationToken).ConfigureAwait(false))
      {
        // Check if adding this chunk would exceed the window size
        if (buffer.Length > 0 && buffer.Length + chunk.Length >= windowSize)
        {
          // Process current buffer before adding the new chunk
          var windowKey = $"{key}{ChunkSeparator}w{windowIndex}";
          await foreach (var entry in ProcessWindowAsync(windowKey, buffer.ToArray(), compressionThreshold, cancellationToken))
          {
            yield return (entry, windowIndex, false);
          }
          buffer = new MemoryStream();
          windowIndex++;
        }

        // Add chunk to buffer
        await buffer.WriteAsync(chunk, cancellationToken).ConfigureAwait(false);
      }

      // Process any remaining data in the buffer (this is the last window)
      if (buffer.Length > 0)
      {
        var windowKey = $"{key}{ChunkSeparator}w{windowIndex}";
        var entries = new List<KeyValuePair<string, byte[]>>();
        
        // Collect all entries from the last window
        await foreach (var entry in ProcessWindowAsync(windowKey, buffer.ToArray(), compressionThreshold, cancellationToken))
        {
          entries.Add(entry);
        }
        
        // Yield all entries, marking only the LAST one as isLastWindow
        for (int i = 0; i < entries.Count; i++)
        {
          var isLastEntry = (i == entries.Count - 1);
          yield return (entries[i], windowIndex, isLastEntry);
        }
      }
    }

    /// <summary>
    /// Processes a single window of data by compressing and chunking it as needed.
    /// </summary>
    /// <param name="key">Key for this window (includes window index)</param>
    /// <param name="data">Raw data for this window (up to 8MB)</param>
    /// <param name="compressionThreshold">Minimum size to trigger compression</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Key-value pairs for storage in Couchbase</returns>
    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> ProcessWindowAsync(
        string key,
        byte[] data,
        int compressionThreshold,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
      // STEP 1: Optionally compress the data
      var compressed = CompressOptional(data, compressionThreshold);

      // STEP 2: Split into chunks if compressed data exceeds 4MB
      var chunks = ChunkData(key, compressed);

      // STEP 3: Yield results
      foreach (var chunk in chunks)
      {
        yield return new KeyValuePair<string, byte[]>(chunk.Key, chunk.Data);
      }
    }

    /// <summary>
    /// Optionally compresses data using GZip if it exceeds the threshold.
    /// </summary>
    /// <param name="data">Raw data to optionally compress</param>
    /// <param name="threshold">Minimum size to trigger compression</param>
    /// <returns>
    /// Compressed or uncompressed data with format:
    /// - Byte 0: Flag (0=uncompressed, 1=compressed)
    /// - Bytes 1-4: Original length (if compressed)
    /// - Remaining: Data (GZip compressed or raw)
    /// </returns>
    private static byte[] CompressOptional(byte[] data, int threshold)
    {
      if (data == null || data.Length == 0)
        return data;

      // Compress if exceeds threshold
      if (data.Length > threshold)
      {
        using var ms = new MemoryStream();
        ms.WriteByte(1); // Flag: compressed

        // Write original length
        ms.Write(BitConverter.GetBytes(data.Length), 0, 4);

        // GZip compress
        using (var gzip = new GZipStream(ms, CompressionLevel.Fastest, true))
        {
          gzip.Write(data, 0, data.Length);
        }

        return ms.ToArray();
      }

      // No compression - just flag
      var result = new byte[data.Length + 1];
      result[0] = 0; // Flag: uncompressed
      Array.Copy(data, 0, result, 1, data.Length);
      return result;
    }

    /// <summary>
    /// Splits compressed data into multiple chunks if it exceeds MaxChunkSize (~4MB).
    /// </summary>
    /// <param name="key">Base key for the chunks</param>
    /// <param name="compressedData">Compressed data to chunk</param>
    /// <returns>
    /// List of key-data tuples:
    /// - Single entry if data fits in MaxChunkSize
    /// - Multiple entries if chunking needed:
    ///   * Main chunk: {key} with first 4MB + chunk count (4 bytes)
    ///   * Extra chunks: {key}`{index} with remaining data
    /// </returns>
    private static List<(string Key, byte[] Data)> ChunkData(string key, byte[] compressedData)
    {
      var result = new List<(string, byte[])>();

      // No chunking needed
      if (compressedData == null || compressedData.Length <= MaxChunkSize)
      {
        result.Add((key, compressedData ?? Array.Empty<byte>()));
        return result;
      }

      var dataLength = compressedData.Length;
      var extraChunkCount = (dataLength - 1) / MaxChunkSize;

      // Main chunk: first MaxChunkSize + 4-byte chunk count
      var mainChunk = new byte[MaxChunkSize + 4];
      Array.Copy(compressedData, 0, mainChunk, 0, MaxChunkSize);
      Array.Copy(BitConverter.GetBytes(extraChunkCount), 0, mainChunk, MaxChunkSize, 4);
      result.Add((key, mainChunk));

      // Additional chunks with separator
      for (int i = 0; i < extraChunkCount; i++)
      {
        var offset = (i + 1) * MaxChunkSize;
        var chunkSize = Math.Min(MaxChunkSize, dataLength - offset);
        var chunkData = new byte[chunkSize];
        Array.Copy(compressedData, offset, chunkData, 0, chunkSize);
        result.Add(($"{key}{ChunkSeparator}{i}", chunkData));
      }

      return result;
    }

    public static string GetInternalKey(string key)
    {
      const string SEPARATOR = "-";
      return $"{InternalKeyPrefix}{SEPARATOR}{key}";
    }

    public static string GetSizeMetadataKey(string key)
    {
      return $"{key}_size";
    }

    public static string GetWindowCountMetadataKey(string key)
    {
      return $"{key}_windowcount";
    }

    /// <summary>
    /// Writes processed stream windows to Couchbase sequentially, tracking window count.
    /// </summary>
    /// <param name="collection">Couchbase collection to write to</param>
    /// <param name="processedStream">Stream of processed entries with window metadata</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>Total number of windows written</returns>
    public static async Task<int> WriteStreamToCouchbaseAsync(
        ICouchbaseCollection collection,
        IAsyncEnumerable<(KeyValuePair<string, byte[]> Entry, int WindowIndex, bool IsLastWindow)> processedStream,
        CancellationToken cancellationToken = default)
    {
      int maxWindowIndex = -1;  // Track highest window index seen

      await foreach (var (entry, windowIndex, isLastWindow) in processedStream.WithCancellation(cancellationToken).ConfigureAwait(false))
      {
        var internalKey = GetInternalKey(entry.Key);
        await collection.UpsertAsync(internalKey, entry.Value, options => options.Transcoder(new LegacyTranscoder())).ConfigureAwait(false);
        
        // Track the maximum window index we've seen
        if (windowIndex > maxWindowIndex)
        {
          maxWindowIndex = windowIndex;
        }
      }

      // Window count is max index + 1 (indices are 0-based)
      // Returns 0 if no entries were processed (maxWindowIndex stays -1)
      return maxWindowIndex + 1;
    }

    /// <summary>
    /// Recombines chunked data by fetching additional chunks and reassembling the original data.
    /// </summary>
    /// <param name="collection">Couchbase collection to fetch chunks from</param>
    /// <param name="key">Base key of the chunked data</param>
    /// <param name="main">Main chunk containing first 4MB and chunk count</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>
    /// Complete reassembled data:
    /// - Returns main data directly if no chunking (length &lt;= MaxChunkSize)
    /// - Fetches and combines all chunks if data was split
    /// </returns>
    public static async Task<byte[]> RecombineAsync(
        ICouchbaseCollection collection,
        string key,
        byte[]? main,
        CancellationToken ct)
    {
      if (main == null || main.Length <= MaxChunkSize) return main;

      var chunkCount = BitConverter.ToInt32(main, main.Length - 4);
      var chunkKeys = Enumerable.Range(0, chunkCount).Select(i => $"{key}{ChunkSeparator}{i}");
      var chunks = await FetchChunksAsync(collection, chunkKeys, ct).ConfigureAwait(false);

      var totalSize = main.Length - 4 + chunks.Sum(kvp => kvp.Value?.Length ?? 0);
      var result = new byte[totalSize];
      var pos = main.Length - 4;
      Array.Copy(main, 0, result, 0, pos);

      for (int i = 0; i < chunkCount; i++)
      {
        if (chunks.TryGetValue($"{key}{ChunkSeparator}{i}", out var chunk) && chunk != null)
        {
          Array.Copy(chunk, 0, result, pos, chunk.Length);
          pos += chunk.Length;
        }
      }
      return result;
    }

    /// <summary>
    /// Fetches multiple chunks from Couchbase in parallel and returns them as a dictionary.
    /// </summary>
    /// <param name="collection">Couchbase collection to fetch from</param>
    /// <param name="keys">Collection of chunk keys to fetch</param>
    /// <param name="ct">Cancellation token</param>
    /// <returns>
    /// Dictionary mapping chunk keys to their data:
    /// - Only includes chunks that exist
    /// - Missing/null chunks are omitted from result
    /// </returns>
    private static async Task<IDictionary<string, byte[]>> FetchChunksAsync(
        ICouchbaseCollection collection,
        IEnumerable<string> keys,
        CancellationToken ct)
    {
      var result = new Dictionary<string, byte[]>();
      var tasks = keys.Select(async k =>
      {
        var internalKey = GetInternalKey(k);
        var r = await collection.TryGetAsync(internalKey, o => o.Transcoder(new LegacyTranscoder())).ConfigureAwait(false);
        return new { Key = k, Data = r.Exists ? r.ContentAs<byte[]>() : null };
      });
      foreach (var t in await Task.WhenAll(tasks).ConfigureAwait(false))
        if (t.Data != null) result[t.Key] = t.Data;
      return result;
    }

    /// <summary>
    /// Decompresses a window of data that was compressed by CompressOptional, yielding chunks for streaming.
    /// </summary>
    /// <param name="recombined">Recombined window data (with compression flag and optional GZip compression)</param>
    /// <param name="chunkSize">Size of chunks to yield (default: 8KB for streaming)</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>
    /// Stream of decompressed data chunks:
    /// - Uncompressed data: Strips flag byte and yields in chunks
    /// - Compressed data: GZip decompresses and yields in chunks
    /// </returns>
    public static async IAsyncEnumerable<byte[]> DecompressWindowAsync(
        byte[] recombined,
        int chunkSize = 8192,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
      var isCompressed = recombined[0] == 1;

      if (!isCompressed)
      {
        // Uncompressed - yield in chunks
        var dataWithoutFlag = new byte[recombined.Length - 1];
        Array.Copy(recombined, 1, dataWithoutFlag, 0, dataWithoutFlag.Length);

        long totalRead = 0;
        long dataLength = dataWithoutFlag.Length;

        while (totalRead < dataLength)
        {
          var currentChunkSize = (int)Math.Min(dataLength - totalRead, chunkSize);
          var chunk = new byte[currentChunkSize];
          Array.Copy(dataWithoutFlag, totalRead, chunk, 0, currentChunkSize);
          totalRead += currentChunkSize;
          yield return chunk;
        }
        yield break;
      }

      // Compressed - stream GZip decompress
      using var ms = new MemoryStream(recombined, 1, recombined.Length - 1);
      var lengthBytes = new byte[4];
      await ms.ReadAsync(lengthBytes, 0, 4, cancellationToken).ConfigureAwait(false);
      var originalLength = BitConverter.ToInt32(lengthBytes, 0);

      using var gzipStream = new GZipStream(ms, CompressionMode.Decompress);
      long totalRead2 = 0;

      while (totalRead2 < originalLength)
      {
        var downloadChunkSize = (int)Math.Min(originalLength - totalRead2, chunkSize);
        var downloadChunk = new byte[downloadChunkSize];

        var bytesRead = await gzipStream.ReadAsync(downloadChunk, cancellationToken).ConfigureAwait(false);

        // Handle partial reads from GZip stream
        while (bytesRead != downloadChunkSize && totalRead2 + bytesRead < originalLength)
        {
          var remainingRead = await gzipStream.ReadAsync(downloadChunk, bytesRead, downloadChunkSize - bytesRead, cancellationToken).ConfigureAwait(false);
          if (remainingRead == 0) break;
          bytesRead += remainingRead;
        }

        if (bytesRead > 0)
        {
          if (bytesRead < downloadChunkSize)
          {
            var actualChunk = new byte[bytesRead];
            Array.Copy(downloadChunk, 0, actualChunk, 0, bytesRead);
            downloadChunk = actualChunk;
          }
          totalRead2 += bytesRead;
          yield return downloadChunk;
        }
        else break;
      }
    }
  }
}
