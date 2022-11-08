// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
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
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.LocalStorage;

public class ObjectStorage : IObjectStorage
{
  private readonly int                    chunkSize_;
  private readonly ILogger<ObjectStorage> logger_;
  private readonly string                 path_;
  private readonly int                    pid_;

  /// <summary>
  ///   <see cref="IObjectStorage" /> implementation for Redis
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
    pid_       = Environment.ProcessId;
    chunkSize_ = chunkSize;
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
    var tid = Environment.CurrentManagedThreadId;
    var filename = Path.Combine(path_,
                                key);
    var tmpFilename = $"{filename}.{pid_}.{tid}";

    using var _ = logger_.LogFunction(filename);

    {
      // Write to temporary file
      await using var file = File.Open(tmpFilename,
                                       FileMode.Create,
                                       FileAccess.Write);

      // TODO: Overlap chunk reading and chunk writing
      await foreach (var chunk in valueChunks.WithCancellation(cancellationToken)
                                             .ConfigureAwait(false))
      {
        await file.WriteAsync(chunk,
                              cancellationToken)
                  .ConfigureAwait(false);
      }

      await file.FlushAsync(cancellationToken)
                .ConfigureAwait(false);
    }

    // Atomically update destination file
    File.Move(tmpFilename,
              filename);
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<byte[]> GetValuesAsync(string                                     key,
                                                       [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    var filename = Path.Combine(path_,
                                key);

    using var _ = logger_.LogFunction(filename);

    await using var file = File.Open(filename,
                                     FileMode.Open,
                                     FileAccess.Read);
    // TODO: overlap read and yield
    while (true)
    {
      var buffer = new byte[chunkSize_];
      var read = await file.ReadAsync(buffer,
                                      cancellationToken)
                           .ConfigureAwait(false);

      if (read != buffer.Length)
      {
        if (read == 0)
        {
          yield break;
        }

        Array.Resize(ref buffer,
                     read);
      }

      yield return buffer;
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
