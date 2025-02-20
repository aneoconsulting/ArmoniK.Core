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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;

using JetBrains.Annotations;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Embed;

[UsedImplicitly]
public class ObjectStorage : IObjectStorage
{
  private readonly ILogger<ObjectStorage> logger_;
  private          bool                   isInitialized_;

  /// <summary>
  ///   <see cref="IObjectStorage" /> implementation for Redis
  /// </summary>
  /// <param name="logger">Logger used to print logs</param>
  public ObjectStorage(ILogger<ObjectStorage> logger)
    => logger_ = logger;

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
                         : HealthCheckResult.Unhealthy("Object storage not initialized yet."));

  /// <inheritdoc />
  public async Task<(byte[] id, long size)> AddOrUpdateAsync(ObjectData                             metaData,
                                                             IAsyncEnumerable<ReadOnlyMemory<byte>> valueChunks,
                                                             CancellationToken                      cancellationToken = default)
  {
    var array = new List<byte>();

    await foreach (var val in valueChunks.WithCancellation(cancellationToken)
                                         .ConfigureAwait(false))
    {
      array.AddRange(val.Span);
    }

    return (array.ToArray(), array.Count);
  }

  /// <inheritdoc />
  public IAsyncEnumerable<byte[]> GetValuesAsync(byte[]            id,
                                                 CancellationToken cancellationToken = default)
    => AsyncEnumerable.Repeat(id,
                              1);

  /// <inheritdoc />
  public Task TryDeleteAsync(IEnumerable<byte[]> ids,
                             CancellationToken   cancellationToken = default)
    => Task.CompletedTask;

  /// <inheritdoc />
  public Task<IDictionary<byte[], long?>> GetSizesAsync(IEnumerable<byte[]> ids,
                                                        CancellationToken   cancellationToken = default)
    => Task.FromResult<IDictionary<byte[], long?>>(ids.ToDictionary(id => id,
                                                                    id => (long?)id.LongLength));
}
