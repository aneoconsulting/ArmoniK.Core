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
using System.IO;
using System.IO.Pipelines;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Base.Exceptions;
using ArmoniK.Core.Utils;
using ArmoniK.Utils;

using Google;
using Google.Cloud.Storage.V1;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Gcs;

/// <summary>
///   <see cref="IObjectStorage" /> implementation for Google Cloud Storage
/// </summary>
public class ObjectStorage : IObjectStorage
{
  private readonly StorageClient          client_;
  private readonly ILogger<ObjectStorage> logger_;
  private readonly string                 objectStorageName_;
  private readonly Options.Gcs            options_;
  private          bool                   isInitialized_;

  /// <summary>
  ///   <see cref="IObjectStorage" /> implementation for Google Cloud Storage
  /// </summary>
  /// <param name="client">Connection to Google Cloud Storage</param>
  /// <param name="options">Gcs object storage options</param>
  /// <param name="logger">Logger used to print logs</param>
  public ObjectStorage(StorageClient          client,
                       Options.Gcs            options,
                       ILogger<ObjectStorage> logger)
  {
    client_            = client;
    objectStorageName_ = "objectStorageName";
    options_           = options;
    logger_            = logger;
  }

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      await client_.GetBucketAsync(options_.BucketName,
                                   null,
                                   cancellationToken)
                   .ConfigureAwait(false);
    }

    logger_.LogInformation("ObjectStorage has correctly been initialized with options {@Options}",
                           options_.Confidential());
    isInitialized_ = true;
  }

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => tag switch
       {
         HealthCheckTag.Startup or HealthCheckTag.Readiness => Task.FromResult(isInitialized_
                                                                                 ? HealthCheckResult.Healthy()
                                                                                 : HealthCheckResult.Unhealthy("Gcs not initialized yet.")),
         HealthCheckTag.Liveness => Task.FromResult(isInitialized_
                                                      ? HealthCheckResult.Healthy()
                                                      : HealthCheckResult.Unhealthy("Gcs not initialized or connection dropped.")),
         _ => throw new ArgumentOutOfRangeException(nameof(tag),
                                                    tag,
                                                    null),
       };

  /// <inheritdoc />
  public async Task<(byte[] id, long size)> AddOrUpdateAsync(ObjectData                             metaData,
                                                             IAsyncEnumerable<ReadOnlyMemory<byte>> valueChunks,
                                                             CancellationToken                      cancellationToken = default)
  {
    var key                   = Guid.NewGuid()
                                    .ToString();
    var objectStorageFullName = $"{objectStorageName_}{key}";

    logger_.LogDebug("Upload object {Key} to bucket {Bucket}",
                     objectStorageFullName,
                     options_.BucketName);

    await using var source = new AsyncChunkReadStream(valueChunks,
                                                      cancellationToken);

    await client_.UploadObjectAsync(options_.BucketName,
                                    objectStorageFullName,
                                    contentType: null,
                                    source: source,
                                    options: null,
                                    cancellationToken: cancellationToken)
                 .ConfigureAwait(false);

    return (Encoding.UTF8.GetBytes(key), source.TotalBytes);
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<byte[]> GetValuesAsync(byte[]                                     id,
                                                       [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    var key                   = Encoding.UTF8.GetString(id);
    var objectStorageFullName = $"{objectStorageName_}{key}";

    var pipe = new Pipe();

    var downloadTask = Task.Run(async () =>
                                {
                                  try
                                  {
                                    // leaveOpen=true: disposing the stream must NOT complete the writer, otherwise a
                                    // failure that happens after the using-block scope wouldn't surface as CompleteAsync(ex).
                                    await using var stream = pipe.Writer.AsStream(true);
                                    await client_.DownloadObjectAsync(options_.BucketName,
                                                                      objectStorageFullName,
                                                                      stream,
                                                                      options: null,
                                                                      cancellationToken: cancellationToken)
                                                 .ConfigureAwait(false);
                                    await pipe.Writer.CompleteAsync()
                                              .ConfigureAwait(false);
                                  }
                                  catch (Exception ex)
                                  {
                                    await pipe.Writer.CompleteAsync(ex)
                                              .ConfigureAwait(false);
                                  }
                                },
                                cancellationToken);

    try
    {
      while (true)
      {
        ReadResult result;
        try
        {
          result = await pipe.Reader.ReadAsync(cancellationToken)
                             .ConfigureAwait(false);
        }
        catch (GoogleApiException ex) when (ex.HttpStatusCode == HttpStatusCode.NotFound)
        {
          logger_.LogError("The key {Key} was not found",
                           objectStorageFullName);
          throw new ObjectDataNotFoundException("Key not found",
                                                ex);
        }

        foreach (var segment in result.Buffer)
        {
          if (!segment.IsEmpty)
          {
            yield return segment.ToArray();
          }
        }

        pipe.Reader.AdvanceTo(result.Buffer.End);

        if (result.IsCompleted)
        {
          break;
        }
      }
    }
    finally
    {
      await pipe.Reader.CompleteAsync()
                .ConfigureAwait(false);
      await downloadTask.ConfigureAwait(false);
    }
  }

  /// <inheritdoc />
  public async Task TryDeleteAsync(IEnumerable<byte[]> ids,
                                   CancellationToken   cancellationToken = default)
    => await ids.ParallelForEach(new ParallelTaskOptions(options_.DegreeOfParallelism),
                                 id => TryDeleteAsync(id,
                                                      cancellationToken))
                .ConfigureAwait(false);

  /// <inheritdoc />
  public async Task<IDictionary<byte[], long?>> GetSizesAsync(IEnumerable<byte[]> ids,
                                                              CancellationToken   cancellationToken = default)
    => await ids.ParallelSelect(new ParallelTaskOptions(options_.DegreeOfParallelism),
                                async id => (id, await GetSizeAsync(id,
                                                                    cancellationToken)
                                                   .ConfigureAwait(false)))
                .ToDictionaryAsync(tuple => tuple.id,
                                   tuple => tuple.Item2,
                                   new ByteArrayComparer(),
                                   cancellationToken)
                .ConfigureAwait(false);

  private async Task<long?> GetSizeAsync(byte[]            id,
                                         CancellationToken cancellationToken)
  {
    try
    {
      var key                   = Encoding.UTF8.GetString(id);
      var objectStorageFullName = $"{objectStorageName_}{key}";

      var obj = await client_.GetObjectAsync(options_.BucketName,
                                             objectStorageFullName,
                                             options: null,
                                             cancellationToken: cancellationToken)
                             .ConfigureAwait(false);

      return (long?)obj.Size;
    }
    catch (GoogleApiException e) when (e.HttpStatusCode == HttpStatusCode.NotFound)
    {
      return null;
    }
  }

  private async Task TryDeleteAsync(byte[]            id,
                                    CancellationToken cancellationToken = default)
  {
    var key                   = Encoding.UTF8.GetString(id);
    var objectStorageFullName = $"{objectStorageName_}{key}";

    try
    {
      await client_.DeleteObjectAsync(options_.BucketName,
                                      objectStorageFullName,
                                      options: null,
                                      cancellationToken: cancellationToken)
                   .ConfigureAwait(false);
      logger_.LogInformation("Deleted data with {resultId}",
                             key);
    }
    catch (GoogleApiException e) when (e.HttpStatusCode == HttpStatusCode.NotFound)
    {
      logger_.LogDebug("Delete is a no-op: key {Key} not found",
                       objectStorageFullName);
    }
  }

  /// <summary>
  ///   Adapts an <see cref="IAsyncEnumerable{T}" /> of byte chunks into a forward-only read <see cref="Stream" />.
  ///   The GCS client expects a <see cref="Stream" /> source for uploads; this wrapper drains the async enumerable
  ///   on demand without buffering the entire payload in memory.
  /// </summary>
  private sealed class AsyncChunkReadStream : Stream
  {
    private readonly IAsyncEnumerator<ReadOnlyMemory<byte>> source_;
    private          ReadOnlyMemory<byte>                   current_ = ReadOnlyMemory<byte>.Empty;
    private          bool                                   finished_;
    private          long                                   totalBytes_;

    public AsyncChunkReadStream(IAsyncEnumerable<ReadOnlyMemory<byte>> source,
                                CancellationToken                      cancellationToken)
      => source_ = source.GetAsyncEnumerator(cancellationToken);

    public long TotalBytes
      => totalBytes_;

    public override bool CanRead
      => true;

    public override bool CanSeek
      => false;

    public override bool CanWrite
      => false;

    public override long Length
      => throw new NotSupportedException();

    public override long Position
    {
      get => throw new NotSupportedException();
      set => throw new NotSupportedException();
    }

    public override void Flush()
    {
    }

    public override long Seek(long       offset,
                              SeekOrigin origin)
      => throw new NotSupportedException();

    public override void SetLength(long value)
      => throw new NotSupportedException();

    public override void Write(byte[] buffer,
                               int    offset,
                               int    count)
      => throw new NotSupportedException();

    public override int Read(byte[] buffer,
                             int    offset,
                             int    count)
      => ReadAsync(buffer.AsMemory(offset,
                                   count))
        .AsTask()
        .GetAwaiter()
        .GetResult();

    public override async ValueTask<int> ReadAsync(Memory<byte>      buffer,
                                                   CancellationToken cancellationToken = default)
    {
      while (current_.IsEmpty)
      {
        if (finished_)
        {
          return 0;
        }

        if (!await source_.MoveNextAsync()
                          .ConfigureAwait(false))
        {
          finished_ = true;
          return 0;
        }

        current_ = source_.Current;
      }

      var toCopy = Math.Min(buffer.Length,
                            current_.Length);
      current_.Slice(0,
                     toCopy)
              .CopyTo(buffer);
      current_    =  current_.Slice(toCopy);
      totalBytes_ += toCopy;
      return toCopy;
    }

    public override Task<int> ReadAsync(byte[]            buffer,
                                        int               offset,
                                        int               count,
                                        CancellationToken cancellationToken)
      => ReadAsync(buffer.AsMemory(offset,
                                   count),
                   cancellationToken)
        .AsTask();

    public override async ValueTask DisposeAsync()
    {
      await source_.DisposeAsync()
                   .ConfigureAwait(false);
      await base.DisposeAsync()
                .ConfigureAwait(false);
    }

    protected override void Dispose(bool disposing)
    {
      if (disposing)
      {
        source_.DisposeAsync()
               .AsTask()
               .GetAwaiter()
               .GetResult();
      }

      base.Dispose(disposing);
    }
  }
}
