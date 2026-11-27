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
using System.Buffers;
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
using Microsoft.Win32.SafeHandles;

namespace ArmoniK.Core.Adapters.LocalStorage;

public class ObjectStorage : IObjectStorage
{
  /// <summary>
  ///   Valid key authorized characters
  /// </summary>
  private static readonly SearchValues<char> ValidKeyChars = SearchValues.Create("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-");

  private readonly int                    chunkParallelism_;
  private readonly int                    chunkSize_;
  private readonly ILogger<ObjectStorage> logger_;
  private readonly int                    parallelism_;
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
    parallelism_      = options.DegreeOfParallelism;
    chunkParallelism_ = options.ChunkParallelism;
    uuidGenerator_    = uuidGenerator;


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
    var key = uuidGenerator_.GenerateUuid()
                            .ToString();
    var filename = GetPath(key);

    // Write to temporary file, with deletion in case of error
    await using var fileCleaner = new Deferrer(() => TryDelete(key));

    long size;
    using (var file = OpenForWriting(filename))
    {
      // Small chunks are merged up to ChunkSize. Each chunk is written at its offset, computed in order, so that
      // ChunkParallelism chunks can be written concurrently.
      long offset = 0;
      size = await valueChunks.Rechunk(chunkSize_,
                                       int.MaxValue)
                              .Select(chunk =>
                                      {
                                        var at = offset;
                                        offset += chunk.Length;
                                        return (chunk, at);
                                      })
                              .ParallelSelect(new ParallelTaskOptions
                                              {
                                                CancellationToken = cancellationToken,
                                                ParallelismLimit  = chunkParallelism_,
                                                Unordered         = true,
                                              },
                                              async (c,
                                                     token) =>
                                              {
                                                await RandomAccess.WriteAsync(file,
                                                                              c.chunk,
                                                                              c.at,
                                                                              token)
                                                                  .ConfigureAwait(false);
                                                return (long)c.chunk.Length;
                                              })
                              .SumAsync(cancellationToken)
                              .ConfigureAwait(false);
    }

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
    using var file   = OpenForReading(key);
    var       length = RandomAccess.GetLength(file);

    // ChunkParallelism chunks are read concurrently at their offsets, and returned in order
    var offsets = Enumerable.Range(0,
                                   (int)((length + chunkSize_ - 1) / chunkSize_))
                            .Select(i => (long)i * chunkSize_);
    await foreach (var chunk in offsets.ParallelSelect(new ParallelTaskOptions
                                                       {
                                                         CancellationToken = cancellationToken,
                                                         ParallelismLimit  = chunkParallelism_,
                                                       },
                                                       (offset,
                                                        token) => ReadChunkAsync(file,
                                                                                 offset,
                                                                                 (int)Math.Min(chunkSize_,
                                                                                               length - offset),
                                                                                 token))
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
                             CancellationToken = cancellationToken,
                             ParallelismLimit  = parallelism_,
                             Unordered         = true,
                           },
                           id =>
                           {
                             TryDelete(Encoding.UTF8.GetString(id));
                             return Task.CompletedTask;
                           });

  /// <inheritdoc />
  public Task<IDictionary<byte[], long?>> GetSizesAsync(IEnumerable<byte[]> ids,
                                                        CancellationToken   cancellationToken = default)
    => ids.ParallelSelect(new ParallelTaskOptions
                          {
                            CancellationToken = cancellationToken,
                            ParallelismLimit  = parallelism_,
                            Unordered         = true,
                          },
                          id => Task.FromResult((id, GetSize(id))))
          .ToDictionaryAsync(new ByteArrayComparer(),
                             cancellationToken)
          .AndThen(static IDictionary<byte[], long?> (dict) => dict)
          .AsTask();

  /// <summary>
  ///   Read a chunk of a file.
  /// </summary>
  /// <param name="file">The file.</param>
  /// <param name="offset">Offset of the chunk in the file.</param>
  /// <param name="length">Length of the chunk.</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>The chunk, shorter if the file ends before.</returns>
  private static async Task<byte[]> ReadChunkAsync(SafeFileHandle    file,
                                                   long              offset,
                                                   int               length,
                                                   CancellationToken cancellationToken)
  {
    var buffer = new byte[length];
    var read   = 0;
    while (read < length)
    {
      var n = await RandomAccess.ReadAsync(file,
                                           buffer.AsMemory(read),
                                           offset + read,
                                           cancellationToken)
                                .ConfigureAwait(false);
      if (n == 0)
      {
        Array.Resize(ref buffer,
                     read);
        break;
      }

      read += n;
    }

    return buffer;
  }

  /// <summary>
  ///   Delete a given object.
  /// </summary>
  /// <param name="key">The key of the object.</param>
  private void TryDelete(string key)
  {
    string filename;
    try
    {
      filename = GetPath(key);
    }
    catch (ArgumentException)
    {
      return;
    }

    try
    {
      File.Delete(filename);
    }
    catch (DirectoryNotFoundException)
    {
      // Ignore non-existent parent directory
    }
  }

  /// <summary>
  ///   Get the size of the given object.
  /// </summary>
  /// <param name="key">The key of the object.</param>
  /// <returns>The size of the backing file, or null if it does not exist.</returns>
  private long? GetSize(byte[] key)
  {
    string filename;
    try
    {
      filename = GetPath(Encoding.UTF8.GetString(key));
    }
    catch (ArgumentException)
    {
      return null;
    }

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
  /// <exception cref="ArgumentException"><paramref name="key" /> has invalid characters.</exception>
  private string GetPath(string key)
  {
    ArgumentNullException.ThrowIfNull(key);

    if (key.Length == 0 || key.ContainsAnyExcept(ValidKeyChars))
    {
      throw new ArgumentException($"The key `{key}` contains invalid character",
                                  nameof(key));
    }

    // The key is too small to be split
    if (key.Length <= splitPathAt_)
    {
      return Path.Combine(path_,
                          key);
    }

    // Split cannot make invalid UTF-16 strings as the string has been first validated
    // to contain only letters, numbers, dashes and underscores.
    // The prefix folder name is suffixed with `.d` to ensure no filename could have
    // the same name, because the folder name is larger than SplitPathAt.
    var span = key.AsSpan();
    return Path.Join(path_,
                     $"{span[..splitPathAt_]}.d",
                     span[splitPathAt_..]);
  }

  /// <summary>
  ///   Create and open a new file for the given key.
  /// </summary>
  /// <param name="filename">Path of the object to write.</param>
  /// <returns>The handle of the opened file.</returns>
  /// <exception cref="ArgumentNullException"><paramref name="filename" /> is null.</exception>
  /// <exception cref="IOException">The file could not be created.</exception>
  private static SafeFileHandle OpenForWriting(string filename)
  {
    try
    {
      // File is allowed to be shared with others in read/write/delete mode without locking.
      // ArmoniK ensures that those operation can only happen after the file has been closed.
      return File.OpenHandle(filename,
                             FileMode.Create,
                             FileAccess.Write,
                             FileShare.ReadWrite | FileShare.Delete,
                             FileOptions.Asynchronous);
    }
    // If the file creation failed and the path has been split,
    // we need to create the prefix folder and retry file creation.
    // A missing prefix folder is not always reported as DirectoryNotFoundException: on Unix, .NET maps ENOENT to
    // FileNotFoundException when the parent folder exists at the time the exception is built, which happens when
    // another writer creates the folder between the failed open and that check.
    catch (IOException)
    {
      var dir = Path.GetDirectoryName(filename);

      if (string.IsNullOrEmpty(dir))
      {
        throw;
      }

      // This creates all intermediate directories and does not fail if it already exists.
      // The directory can be created by another agent in the meantime.
      Directory.CreateDirectory(dir);

      return File.OpenHandle(filename,
                             FileMode.Create,
                             FileAccess.Write,
                             FileShare.ReadWrite | FileShare.Delete,
                             FileOptions.Asynchronous);
    }
  }

  /// <summary>
  ///   Open an object.
  /// </summary>
  /// <param name="key">Key of the object to read.</param>
  /// <returns>The handle of the object file.</returns>
  /// <exception cref="ObjectDataNotFoundException">The object does not exist.</exception>
  private SafeFileHandle OpenForReading(string key)
  {
    string filename;
    try
    {
      filename = GetPath(key);
    }
    catch (ArgumentException e)
    {
      throw new ObjectDataNotFoundException($"The object {key} has not been found in {path_}",
                                            e);
    }

    try
    {
      // File is allowed to be shared with others in read/write/delete mode without locking.
      // Deleting keep the file alive without a Path and let all the readers to finish their reads.
      return File.OpenHandle(filename,
                             FileMode.Open,
                             FileAccess.Read,
                             FileShare.ReadWrite | FileShare.Delete,
                             FileOptions.Asynchronous);
    }
    catch (IOException e) when (e is FileNotFoundException or DirectoryNotFoundException)
    {
      throw new ObjectDataNotFoundException($"The object {key} has not been found in {path_}",
                                            e);
    }
  }
}
