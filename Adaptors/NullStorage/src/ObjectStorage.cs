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
using ArmoniK.Core.Utils;

using JetBrains.Annotations;

using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ArmoniK.Core.Adapters.NullStorage;

[UsedImplicitly]
public class ObjectStorage : IObjectStorage
{
  /// <summary>
  ///   <see cref="IObjectStorage" /> implementation for NullStorage
  /// </summary>
  public ObjectStorage()
  {
  }

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
    => Task.CompletedTask;

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(HealthCheckResult.Healthy());

  /// <inheritdoc />
  public async Task<(byte[] id, long size)> AddOrUpdateAsync(ObjectData                             metaData,
                                                             IAsyncEnumerable<ReadOnlyMemory<byte>> valueChunks,
                                                             CancellationToken                      cancellationToken = default)
  {
    long size      = 0;
    long chunkSize = 0;
    await foreach (var chunk in valueChunks.WithCancellation(cancellationToken)
                                           .ConfigureAwait(false))
    {
      size += chunk.Length;
      chunkSize = long.Max(chunkSize,
                           chunk.Length);
    }

    return (Encode(chunkSize,
                   size), size);
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<byte[]> GetValuesAsync(byte[]                                     id,
                                                       [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    await Task.Yield();

    var (chunkSize, size) = Decode(id);

    while (size >= chunkSize)
    {
      cancellationToken.ThrowIfCancellationRequested();
      yield return new byte[chunkSize];
      size -= chunkSize;
    }

    if (size > 0)
    {
      cancellationToken.ThrowIfCancellationRequested();
      yield return new byte[size];
    }
  }

  /// <inheritdoc />
  public Task TryDeleteAsync(IEnumerable<byte[]> ids,
                             CancellationToken   cancellationToken = default)
    => Task.CompletedTask;

  /// <inheritdoc />
  public Task<IDictionary<byte[], long?>> GetSizesAsync(IEnumerable<byte[]> ids,
                                                        CancellationToken   cancellationToken = default)
    => Task.FromResult<IDictionary<byte[], long?>>(ids.ToDictionary(id => id,
                                                                    id => (long?)Decode(id)
                                                                      .size,
                                                                    new ByteArrayComparer()));

  private static byte[] Encode(long chunkSize,
                               long size)
    => Encoding.ASCII.GetBytes($"{chunkSize}:{size}");

  private static (long chunkSize, long size) Decode(byte[] id)
  {
    var elements = Encoding.ASCII.GetString(id)
                           .Split(":",
                                  2);

    return (long.Parse(elements[0]), long.Parse(elements[1]));
  }
}
