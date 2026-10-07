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

namespace ArmoniK.Core.Adapters.LocalStorage;

public class ObjectStorage : IObjectStorage
{
  private readonly int                    chunkSize_;
  private readonly ILogger<ObjectStorage> logger_;
  private readonly string                 path_;
  private readonly int                    splitPathAt_;
  private readonly IUuidGenerator         uuidGenerator_;
  private          bool                   isInitialized_;

  /// <summary>
  ///   <see cref="IObjectStorage" /> implementation for LocalStorage
  /// </summary>
  /// <param name="options">Options for the local storage</param>
  /// <param name="uuidGenerator">Generator of UUIDs</param>
  /// <param name="logger">Logger used to print logs</param>
  public ObjectStorage(Options.LocalStorage options,
                       [FromKeyedServices(UuidServiceKey.Uniform)]
                       IUuidGenerator uuidGenerator,
                       ILogger<ObjectStorage> logger)
  {
    // Empty is replaced by the default
    path_ = string.IsNullOrEmpty(options.Path)
              ? Options.LocalStorage.Default.Path
              : options.Path;

    // 0 is replaced by the default
    chunkSize_ = options.ChunkSize == 0
                   ? Options.LocalStorage.Default.ChunkSize
                   : options.ChunkSize;

    // 0 disables path splitting
    splitPathAt_ = options.SplitPathAt <= 0
                     ? int.MaxValue
                     : options.SplitPathAt;
    uuidGenerator_ = uuidGenerator;


    logger_ = logger;

    logger.LogDebug("Creating Local ObjectStorage with options {@Options}",
                    options);

    Directory.CreateDirectory(path_);
  }

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
  {
    _ = cancellationToken;
    logger_.LogDebug("Initializing Local ObjectStorageFactory at path {path}, chunked by {chunkSize}",
                     path_,
                     chunkSize_);
    // This creates all intermediate directories and does not fail if it already exists
    Directory.CreateDirectory(path_);
    isInitialized_ = true;
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => tag switch
       {
         HealthCheckTag.Startup or HealthCheckTag.Readiness => Task.FromResult(isInitialized_
                                                                                 ? HealthCheckResult.Healthy()
                                                                                 : HealthCheckResult.Unhealthy("Local storage not initialized yet.")),
         HealthCheckTag.Liveness => Task.FromResult(isInitialized_ && Directory.Exists(path_)
                                                      ? HealthCheckResult.Healthy()
                                                      : HealthCheckResult.Unhealthy("Local storage not initialized or folder has been deleted.")),
         _ => throw new ArgumentOutOfRangeException(nameof(tag),
                                                    tag,
                                                    null),
       };

  /// <inheritdoc />
  public async Task<(byte[] id, long size)> AddOrUpdateAsync(ObjectData                             metaData,
                                                             IAsyncEnumerable<ReadOnlyMemory<byte>> valueChunks,
                                                             CancellationToken                      cancellationToken = default)
  {
    long size = 0;
    var key = uuidGenerator_.GenerateUuid()
                            .ToString();
    var filename = GetPath(key);

    // Write to temporary file, with deletion in case of error
    await using var fileCleaner = new Deferrer(() => File.Delete(filename));
    await using var file        = OpenForWriting(filename);

    await using var enumerator = valueChunks.GetAsyncEnumerator(cancellationToken);

    // Prepare overlapped read and write
    var readTask = enumerator.MoveNextAsync()
                             .ConfigureAwait(false);
    var writeTask = ValueTask.CompletedTask.ConfigureAwait(false);

    while (await readTask)
    {
      var chunk = enumerator.Current;
      size += chunk.Length;

      readTask = enumerator.MoveNextAsync()
                           .ConfigureAwait(false);
      await writeTask;
      writeTask = file.WriteAsync(chunk,
                                  cancellationToken)
                      .ConfigureAwait(false);
    }

    // Last write must be complete before flushing
    await writeTask;

    await file.FlushAsync(cancellationToken)
              .ConfigureAwait(false);

    // File has been successfully written, so deletion should be withdrawn.
    fileCleaner.Reset();

    return (Encoding.UTF8.GetBytes(key), size);
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<byte[]> GetValuesAsync(byte[]                                     id,
                                                       [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    var key = Encoding.UTF8.GetString(id);

    // If opening fails, the exception is wrapped in a ObjectDataNotFoundException
    await using var file = OpenForReading(key);

    // Task is not awaited here in order to overlap reading and yielding
    var buffer = new byte[chunkSize_];
    var readTask = file.ReadAsync(buffer,
                                  cancellationToken)
                       .ConfigureAwait(false);

    int read;

    // While chunk is not empty
    while ((read = await readTask) > 0)
    {
      var readBuffer = buffer;

      // Start reading new chunk
      buffer = new byte[chunkSize_];
      readTask = file.ReadAsync(buffer,
                                cancellationToken)
                     .ConfigureAwait(false);

      // Partial chunk requires a resize
      if (read < chunkSize_)
      {
        Array.Resize(ref readBuffer,
                     read);
      }

      yield return readBuffer;
    }
  }

  /// <inheritdoc />
  public Task TryDeleteAsync(IEnumerable<byte[]> ids,
                             CancellationToken   cancellationToken = default)
  {
    if (cancellationToken.IsCancellationRequested)
    {
      return Task.FromCanceled(cancellationToken);
    }

    try
    {
      foreach (var id in ids)
      {
        var key      = Encoding.UTF8.GetString(id);
        var filename = GetPath(key);
        File.Delete(filename);
      }
    }
    catch (Exception e)
    {
      return Task.FromException(e);
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task<IDictionary<byte[], long?>> GetSizesAsync(IEnumerable<byte[]> ids,
                                                        CancellationToken   cancellationToken = default)
    => Task.FromResult<IDictionary<byte[], long?>>(ids.ToDictionary(id => id,
                                                                    GetSize,
                                                                    new ByteArrayComparer()));

  /// <summary>
  ///   Get the size of the given object.
  /// </summary>
  /// <param name="key">The key of the object.</param>
  /// <returns>The size of the backing file, or null if it does not exist.</returns>
  private long? GetSize(byte[] key)
  {
    var filename = GetPath(Encoding.UTF8.GetString(key));

    try
    {
      return new FileInfo(filename).Length;
    }
    catch (FileNotFoundException)
    {
      return null;
    }
  }

  /// <summary>
  ///   Get full path for the given <paramref name="key" />.
  ///   The first SplitPathAt characters of the key are materialized as a folder prefix.
  /// </summary>
  /// <param name="key">Key of the object to access.</param>
  /// <returns>The path to the object.</returns>
  /// <exception cref="ArgumentNullException"><paramref name="key" /> is null.</exception>
  /// <exception cref="ArgumentException"><paramref name="key" /> could not be split.</exception>
  private string GetPath(string key)
  {
    ArgumentNullException.ThrowIfNull(key);

    // The key is too small to be split
    if (key.Length <= splitPathAt_)
    {
      return Path.Combine(path_,
                          key);
    }

    // If the first character after the split is the low part of a surrogate pair,
    // either the string was not valid UTF-16, or we split in the middle of the pair,
    // creating an invalid UTF-16 string. In both cases, the result is not valid UTF-16 string.
    if (char.IsLowSurrogate(key[splitPathAt_]))
    {
      throw new ArgumentException($"The key `{key}` is invalid as it contains a surrogate pair at split location ({splitPathAt_}).",
                                  nameof(key));
    }

    var span = key.AsSpan();
    return Path.Join(path_.AsSpan(),
                     span[..splitPathAt_],
                     span[splitPathAt_..]);
  }

  /// <summary>
  ///   Create and open a new for file for the given key.
  /// </summary>
  /// <param name="filename">Path of the object to write.</param>
  /// <returns>The Stream of the opened file.</returns>
  /// <exception cref="ArgumentNullException"><paramref name="filename" /> is null.</exception>
  /// <exception cref="ArgumentException"><paramref name="filename" /> could not be split.</exception>
  /// <exception cref="IOException">The file could not be created.</exception>
  private static FileStream OpenForWriting(string filename)
  {
    try
    {
      return File.Open(filename,
                       FileMode.OpenOrCreate,
                       FileAccess.Write,
                       FileShare.ReadWrite | FileShare.Delete);
    }
    // If the file creation failed and the path has been split,
    // we need to create the prefix folder and retry file creation
    catch (DirectoryNotFoundException)
    {
      var dir = Path.GetDirectoryName(filename);

      if (string.IsNullOrEmpty(dir))
      {
        throw;
      }

      // This creates all intermediate directories and does not fail if it already exists.
      // The directory can be created by another agent in the meantime.
      Directory.CreateDirectory(dir);

      return File.Open(filename,
                       FileMode.OpenOrCreate,
                       FileAccess.Write,
                       FileShare.ReadWrite | FileShare.Delete);
    }
  }

  /// <summary>
  ///   Open an object.
  /// </summary>
  /// <param name="key">Key of the object to read.</param>
  /// <returns>The <see cref="FileStream" /> to the object file.</returns>
  /// <exception cref="ObjectDataNotFoundException">The object does not exist.</exception>
  private FileStream OpenForReading(string key)
  {
    var filename = GetPath(key);
    try
    {
      return File.Open(filename,
                       FileMode.Open,
                       FileAccess.Read,
                       FileShare.ReadWrite | FileShare.Delete);
    }
    catch (IOException e) when (e is FileNotFoundException or DirectoryNotFoundException)
    {
      throw new ObjectDataNotFoundException($"The object {key} has not been found in {path_}",
                                            e);
    }
  }
}
