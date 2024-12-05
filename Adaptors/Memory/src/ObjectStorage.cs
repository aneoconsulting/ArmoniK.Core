// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Base.Exceptions;
using ArmoniK.Utils;

using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ArmoniK.Core.Adapters.Memory;

public class ObjectStorage : IObjectStorage
{
  private readonly ConcurrentDictionary<string, byte[]> store_ = new();
  private          bool                                 isInitialized_;

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
  {
    isInitialized_ = true;
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(isInitialized_
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy());

  public async Task<(byte[] id, long size)> AddOrUpdateAsync(ObjectData                             metaData,
                                                             IAsyncEnumerable<ReadOnlyMemory<byte>> valueChunks,
                                                             CancellationToken                      cancellationToken = default)
  {
    var array = new List<byte>();

    var key = Guid.NewGuid()
                  .ToString();

    await foreach (var val in valueChunks.WithCancellation(cancellationToken)
                                         .ConfigureAwait(false))
    {
      array.AddRange(val.ToArray());
    }

    store_[key] = array.ToArray();

    return (Encoding.UTF8.GetBytes(key), array.Count);
  }

#pragma warning disable CS1998
  public async IAsyncEnumerable<byte[]> GetValuesAsync(byte[] id,
#pragma warning restore CS1998
                                                       [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    var key = Encoding.UTF8.GetString(id);
    if (!store_.TryGetValue(key,
                            out var value))
    {
      throw new ObjectDataNotFoundException();
    }

    foreach (var chunk in value.ToChunks(100))
    {
      yield return chunk;
    }
  }

  public Task TryDeleteAsync(IEnumerable<byte[]> ids,
                             CancellationToken   cancellationToken = default)
  {
    foreach (var id in ids)
    {
      var key = Encoding.UTF8.GetString(id);

      store_.TryRemove(key,
                       out _);
    }

    return Task.CompletedTask;
  }
}
