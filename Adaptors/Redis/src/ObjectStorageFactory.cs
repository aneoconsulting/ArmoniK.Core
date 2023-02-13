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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using StackExchange.Redis;

namespace ArmoniK.Core.Adapters.Redis;

public class ObjectStorageFactory : IObjectStorageFactory
{
  private readonly ILoggerFactory loggerFactory_;
  private readonly IDatabaseAsync redis_;


  private bool isInitialized_;

  public ObjectStorageFactory(IDatabaseAsync redis,
                              ILoggerFactory loggerFactory)
  {
    redis_         = redis;
    loggerFactory_ = loggerFactory;
  }

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      await redis_.PingAsync()
                  .ConfigureAwait(false);
    }

    isInitialized_ = true;
  }

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
  {
    switch (tag)
    {
      case HealthCheckTag.Startup:
      case HealthCheckTag.Readiness:
        return Task.FromResult(isInitialized_
                                 ? HealthCheckResult.Healthy()
                                 : HealthCheckResult.Unhealthy("Redis not initialized yet."));
      case HealthCheckTag.Liveness:
        return Task.FromResult(isInitialized_ && redis_.Multiplexer.IsConnected
                                 ? HealthCheckResult.Healthy()
                                 : HealthCheckResult.Unhealthy("Redis not initialized or connection dropped."));
      default:
        throw new ArgumentOutOfRangeException(nameof(tag),
                                              tag,
                                              null);
    }
  }

  public IObjectStorage CreateObjectStorage(string objectStorageName)
    => new ObjectStorage(redis_,
                         objectStorageName,
                         loggerFactory_.CreateLogger<ObjectStorage>());
}
