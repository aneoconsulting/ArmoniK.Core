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
using System.Globalization;
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
  /// <summary>
  ///   Separates the key of a striped object from its layout: "{uuid}_{count}x{size}"
  /// </summary>
  private const char StripeSeparator = '_';

  /// <summary>
  ///   Valid key authorized characters
  /// </summary>
  private static readonly SearchValues<char> ValidKeyChars = SearchValues.Create("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-");

  private readonly int                    chunkSize_;
  private readonly ILogger<ObjectStorage> logger_;
  private readonly int                    parallelism_;
  private readonly string                 path_;
  private readonly int                    splitPathAt_;
  private readonly int                    stripeCount_;
  private readonly int                    stripeSize_;
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
    parallelism_ = options.DegreeOfParallelism;

    // 1 or less keeps one file per object
    stripeCount_ = Math.Max(options.StripeCount,
                            1);

    // 0 or less is replaced by the default
    stripeSize_ = options.StripeSize <= 0
                    ? Options.LocalStorage.Default.StripeSize
                    : options.StripeSize;
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
    var key = uuidGenerator_.GenerateUuid()
                            .ToString();

    // The layout of a striped object is part of its key, so that it is read the same way whatever the settings of
    // the reader
    if (stripeCount_ > 1)
    {
      key = $"{key}{StripeSeparator}{stripeCount_}x{stripeSize_}";
    }

    var (paths, stripeSize) = GetLayout(key);

    // Write to temporary file, with deletion in case of error
    await using var fileCleaner = new Deferrer(() => TryDelete(key));

    var size = paths.Length == 1
                 ? await WriteFileAsync(paths[0],
                                        valueChunks,
                                        cancellationToken)
                     .ConfigureAwait(false)
                 : await WriteStripesAsync(paths,
                                           stripeSize,
                                           valueChunks,
                                           cancellationToken)
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

    string[] paths;
    int      stripeSize;
    try
    {
      (paths, stripeSize) = GetLayout(key);
    }
    catch (ArgumentException e)
    {
      throw new ObjectDataNotFoundException($"The object {key} has not been found in {path_}",
                                            e);
    }

    if (paths.Length > 1)
    {
      await foreach (var chunk in ReadStripesAsync(key,
                                                   paths,
                                                   stripeSize,
                                                   cancellationToken)
                       .ConfigureAwait(false))
      {
        yield return chunk;
      }

      yield break;
    }

    // If opening fails, the exception is wrapped in a ObjectDataNotFoundException
    await using var file = OpenForReading(key,
                                          paths[0]);

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
  ///   Write the chunks of an object into one file.
  /// </summary>
  /// <param name="filename">Path of the file.</param>
  /// <param name="valueChunks">Chunks of the object.</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>The size of the object.</returns>
  private static async Task<long> WriteFileAsync(string                                 filename,
                                                 IAsyncEnumerable<ReadOnlyMemory<byte>> valueChunks,
                                                 CancellationToken                      cancellationToken)
  {
    long            size = 0;
    await using var file = OpenForWriting(filename);

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

    return size;
  }

  /// <summary>
  ///   Write the chunks of an object as stripes dealt round-robin to several files, which are written concurrently.
  /// </summary>
  /// <param name="paths">Paths of the files, stripe i goes to file i modulo their number.</param>
  /// <param name="stripeSize">Size of the stripes; only the last stripe of the object can be smaller.</param>
  /// <param name="valueChunks">Chunks of the object.</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>The size of the object.</returns>
  private static async Task<long> WriteStripesAsync(string[]                               paths,
                                                    int                                    stripeSize,
                                                    IAsyncEnumerable<ReadOnlyMemory<byte>> valueChunks,
                                                    CancellationToken                      cancellationToken)
  {
    var files = new FileStream?[paths.Length];
    // write in flight on each file, and the pooled buffer it writes
    var writes  = new Task[paths.Length];
    var buffers = new byte[]?[paths.Length];
    Array.Fill(writes,
               Task.CompletedTask);

    long size   = 0;
    var  stripe = 0;
    var  filled = 0;
    var  buffer = ArrayPool<byte>.Shared.Rent(stripeSize);

    // Waits for the previous write of the file of the current stripe, then starts writing the stripe: up to one write
    // in flight per file
    async Task WriteStripeAsync()
    {
      var file = stripe % paths.Length;
      await writes[file]
        .ConfigureAwait(false);
      var write = files[file]!.WriteAsync(buffer.AsMemory(0,
                                                          filled),
                                          cancellationToken)
                              .AsTask();
      if (buffers[file] is not null)
      {
        ArrayPool<byte>.Shared.Return(buffers[file]!);
      }

      buffers[file] = buffer;
      writes[file]  = write;
      buffer        = ArrayPool<byte>.Shared.Rent(stripeSize);
      filled        = 0;
      stripe++;
    }

    try
    {
      for (var i = 0; i < paths.Length; i++)
      {
        files[i] = OpenForWriting(paths[i]);
      }

      await foreach (var chunk in valueChunks.WithCancellation(cancellationToken)
                                             .ConfigureAwait(false))
      {
        size += chunk.Length;
        var remaining = chunk;
        while (!remaining.IsEmpty)
        {
          var length = Math.Min(remaining.Length,
                                stripeSize - filled);
          remaining[..length]
            .CopyTo(buffer.AsMemory(filled));
          filled    += length;
          remaining =  remaining[length..];

          if (filled == stripeSize)
          {
            await WriteStripeAsync()
              .ConfigureAwait(false);
          }
        }
      }

      if (filled > 0)
      {
        await WriteStripeAsync()
          .ConfigureAwait(false);
      }

      await Task.WhenAll(writes)
                .ConfigureAwait(false);

      foreach (var file in files)
      {
        await file!.FlushAsync(cancellationToken)
                   .ConfigureAwait(false);
      }

      return size;
    }
    finally
    {
      // On failure, the writes still in flight must end before their buffers go back to the pool
      try
      {
        await Task.WhenAll(writes)
                  .ConfigureAwait(false);
      }
      catch
      {
        // The error has already been thrown by the main path
      }

      ArrayPool<byte>.Shared.Return(buffer);
      foreach (var b in buffers)
      {
        if (b is not null)
        {
          ArrayPool<byte>.Shared.Return(b);
        }
      }

      foreach (var file in files)
      {
        if (file is not null)
        {
          await file.DisposeAsync()
                    .ConfigureAwait(false);
        }
      }
    }
  }

  /// <summary>
  ///   Read a striped object: the files are read concurrently, one stripe ahead each, and the stripes are returned in
  ///   order as chunks of at most ChunkSize bytes.
  /// </summary>
  /// <param name="key">Key of the object.</param>
  /// <param name="paths">Paths of the files, stripe i is in file i modulo their number.</param>
  /// <param name="stripeSize">Size of the stripes.</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>The chunks of the object.</returns>
  /// <exception cref="ObjectDataNotFoundException">A file of the object does not exist.</exception>
  private async IAsyncEnumerable<byte[]> ReadStripesAsync(string                                     key,
                                                          string[]                                   paths,
                                                          int                                        stripeSize,
                                                          [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    var       files = new FileStream?[paths.Length];
    var       reads = new Task<List<byte[]>>?[paths.Length];
    using var cts   = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

    try
    {
      for (var i = 0; i < paths.Length; i++)
      {
        files[i] = OpenForReading(key,
                                  paths[i]);
      }

      for (var i = 0; i < paths.Length; i++)
      {
        reads[i] = ReadStripeAsync(files[i]!,
                                   stripeSize,
                                   chunkSize_,
                                   cts.Token);
      }

      // Every stripe but the last one is full: the object ends with the first stripe that is not
      for (var stripe = 0;; stripe++)
      {
        var file   = stripe % paths.Length;
        var chunks = await reads[file]!.ConfigureAwait(false);
        var length = chunks.Sum(chunk => (long)chunk.Length);

        reads[file] = length == stripeSize
                        ? ReadStripeAsync(files[file]!,
                                          stripeSize,
                                          chunkSize_,
                                          cts.Token)
                        : null;

        foreach (var chunk in chunks)
        {
          yield return chunk;
        }

        if (length < stripeSize)
        {
          yield break;
        }
      }
    }
    finally
    {
      // The reads still in flight (the consumer stopped early, or a read failed) must end before closing the files
      await cts.CancelAsync()
               .ConfigureAwait(false);
      foreach (var read in reads)
      {
        if (read is null)
        {
          continue;
        }

        try
        {
          await read.ConfigureAwait(false);
        }
        catch
        {
          // Cancelled reads, or an error already thrown by the main path
        }
      }

      foreach (var file in files)
      {
        if (file is not null)
        {
          await file.DisposeAsync()
                    .ConfigureAwait(false);
        }
      }
    }
  }

  /// <summary>
  ///   Read the next stripe of a file.
  /// </summary>
  /// <param name="file">File positioned at the start of the stripe.</param>
  /// <param name="stripeSize">Size of the stripes.</param>
  /// <param name="chunkSize">Maximum size of the returned chunks.</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>The stripe as chunks of at most <paramref name="chunkSize" /> bytes, shorter at the end of the file.</returns>
  private static async Task<List<byte[]>> ReadStripeAsync(FileStream        file,
                                                          int               stripeSize,
                                                          int               chunkSize,
                                                          CancellationToken cancellationToken)
  {
    var chunks    = new List<byte[]>();
    var remaining = stripeSize;
    while (remaining > 0)
    {
      var buffer = new byte[Math.Min(chunkSize,
                                     remaining)];
      var read = await file.ReadAtLeastAsync(buffer,
                                             buffer.Length,
                                             false,
                                             cancellationToken)
                           .ConfigureAwait(false);
      if (read == 0)
      {
        break;
      }

      remaining -= read;
      if (read < buffer.Length)
      {
        // End of the file
        Array.Resize(ref buffer,
                     read);
        chunks.Add(buffer);
        break;
      }

      chunks.Add(buffer);
    }

    return chunks;
  }

  /// <summary>
  ///   Delete a given object.
  /// </summary>
  /// <param name="key">The key of the object.</param>
  private void TryDelete(string key)
  {
    string[] paths;
    try
    {
      (paths, _) = GetLayout(key);
    }
    catch (ArgumentException)
    {
      return;
    }

    foreach (var filename in paths)
    {
      try
      {
        File.Delete(filename);
      }
      catch (DirectoryNotFoundException)
      {
        // Ignore non-existent parent directory
      }
    }
  }

  /// <summary>
  ///   Get the size of the given object.
  /// </summary>
  /// <param name="key">The key of the object.</param>
  /// <returns>The size of the backing files, or null if one of them does not exist.</returns>
  private long? GetSize(byte[] key)
  {
    string[] paths;
    try
    {
      (paths, _) = GetLayout(Encoding.UTF8.GetString(key));
    }
    catch (ArgumentException)
    {
      return null;
    }

    try
    {
      return paths.Sum(filename => new FileInfo(filename).Length);
    }
    catch (FileNotFoundException)
    {
      return null;
    }
  }

  /// <summary>
  ///   Get the files of the object of the given <paramref name="key" />: one file named after the key, or for a striped
  ///   object (key suffixed by "_{count}x{size}"), the files "key.0" to "key.{count - 1}".
  /// </summary>
  /// <param name="key">Key of the object to access.</param>
  /// <returns>The paths of the files of the object and the size of its stripes (0 when it is not striped).</returns>
  /// <exception cref="ArgumentNullException"><paramref name="key" /> is null.</exception>
  /// <exception cref="ArgumentException"><paramref name="key" /> has invalid characters.</exception>
  private (string[] paths, int stripeSize) GetLayout(string key)
  {
    var path = GetPath(key);

    var separator = key.LastIndexOf(StripeSeparator);
    if (separator < 0)
    {
      return ([path], 0);
    }

    var layout = key.AsSpan(separator + 1);
    var times  = layout.IndexOf('x');
    if (times <= 0 || !int.TryParse(layout[..times],
                                    NumberStyles.None,
                                    CultureInfo.InvariantCulture,
                                    out var count) || !int.TryParse(layout[(times + 1)..],
                                                                    NumberStyles.None,
                                                                    CultureInfo.InvariantCulture,
                                                                    out var stripeSize) || count < 2 || stripeSize < 1)
    {
      return ([path], 0);
    }

    var paths = new string[count];
    for (var i = 0; i < count; i++)
    {
      paths[i] = $"{path}.{i}";
    }

    return (paths, stripeSize);
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
  /// <returns>The Stream of the opened file.</returns>
  /// <exception cref="ArgumentNullException"><paramref name="filename" /> is null.</exception>
  /// <exception cref="IOException">The file could not be created.</exception>
  private static FileStream OpenForWriting(string filename)
  {
    try
    {
      // File is allowed to be shared with others in read/write/delete mode without locking.
      // ArmoniK ensures that those operation can only happen after the file has been closed.
      return File.Open(filename,
                       FileMode.Create,
                       FileAccess.Write,
                       FileShare.ReadWrite | FileShare.Delete);
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

      return File.Open(filename,
                       FileMode.Create,
                       FileAccess.Write,
                       FileShare.ReadWrite | FileShare.Delete);
    }
  }

  /// <summary>
  ///   Open a file of an object.
  /// </summary>
  /// <param name="key">Key of the object to read.</param>
  /// <param name="filename">Path of the file.</param>
  /// <returns>The <see cref="FileStream" /> to the object file.</returns>
  /// <exception cref="ObjectDataNotFoundException">The object does not exist.</exception>
  private FileStream OpenForReading(string key,
                                    string filename)
  {
    try
    {
      // File is allowed to be shared with others in read/write/delete mode without locking.
      // Deleting keep the file alive without a Path and let all the readers to finish their reads.
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
