// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.LocalStorage;

public class ObjectStorage : IObjectStorage
{
  private readonly int                    chunkSize_;
  private readonly ILogger<ObjectStorage> logger_;
  private readonly string                 path_;

  /// <summary>
  ///   <see cref="IObjectStorage" /> implementation for LocalStorage
  /// </summary>
  /// <param name="path">Path where the objects are stored</param>
  /// <param name="chunkSize">Size of the chunks when reading</param>
  /// <param name="logger">Logger used to print logs</param>
  public ObjectStorage(string                 path,
                       int                    chunkSize,
                       ILogger<ObjectStorage> logger)
  {
    path_      = path;
    logger_    = logger;
    chunkSize_ = chunkSize;

    logger.LogDebug("Creating Local ObjectStorage at {path}",
                    path);

    Directory.CreateDirectory(path);
  }

  /// <inheritdoc />
  public Task AddOrUpdateAsync(string                   key,
                               IAsyncEnumerable<byte[]> valueChunks,
                               CancellationToken        cancellationToken = default)
    => AddOrUpdateAsync(key,
                        valueChunks.Select(chunk => (ReadOnlyMemory<byte>)chunk.AsMemory()),
                        cancellationToken);

  /// <inheritdoc />
  public async Task AddOrUpdateAsync(string                                 key,
                                     IAsyncEnumerable<ReadOnlyMemory<byte>> valueChunks,
                                     CancellationToken                      cancellationToken = default)
  {
    var filename = Path.Combine(path_,
                                key);

    using var _ = logger_.LogFunction(filename);

    // Write to temporary file
    await using var file = File.Open(filename,
                                     FileMode.OpenOrCreate,
                                     FileAccess.Write);

    await using var enumerator = valueChunks.GetAsyncEnumerator(cancellationToken);

    // Prepare overlapped read and write
    var readTask = enumerator.MoveNextAsync()
                             .ConfigureAwait(false);
    var writeTask = ValueTask.CompletedTask.ConfigureAwait(false);

    while (await readTask)
    {
      var chunk = enumerator.Current;

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
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<byte[]> GetValuesAsync(string                                     key,
                                                       [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    var filename = Path.Combine(path_,
                                key);

    using var _ = logger_.LogFunction(filename);

    if (!File.Exists(filename))
    {
      throw new ObjectDataNotFoundException($"The object {key} has not been found in {path_}");
    }

    await using var file = File.Open(filename,
                                     FileMode.Open,
                                     FileAccess.Read);

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
  public Task<bool> TryDeleteAsync(string            key,
                                   CancellationToken cancellationToken = default)
  {
    var filename = Path.Combine(path_,
                                key);

    using var _ = logger_.LogFunction(filename);

    File.Delete(filename);

    return Task.FromResult(true);
  }

  /// <inheritdoc />
  public IAsyncEnumerable<string> ListKeysAsync(CancellationToken cancellationToken = default)
    => throw new NotImplementedException();
}
