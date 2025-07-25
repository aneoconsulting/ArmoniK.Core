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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;

using JetBrains.Annotations;

using Microsoft.Extensions.Diagnostics.HealthChecks;

using MongoDB.Driver;
using MongoDB.Driver.Core.Clusters;

namespace ArmoniK.Core.Adapters.MongoDB.Common;

public class SessionProvider : IInitializable
{
  private readonly IMongoClient          client_;
  private readonly object                lockObj_ = new();
  private          IClientSessionHandle? clientSessionHandle_;

  [UsedImplicitly]
  public SessionProvider(IMongoClient client)
    => client_ = client;

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => tag switch
       {
         HealthCheckTag.Readiness or HealthCheckTag.Startup => Task.FromResult(clientSessionHandle_ is not null
                                                                                 ? HealthCheckResult.Healthy()
                                                                                 : HealthCheckResult.Degraded($"{nameof(clientSessionHandle_)} is still null")),
         HealthCheckTag.Liveness => Task.FromResult(clientSessionHandle_ is not null && client_.Cluster.Description.State == ClusterState.Connected
                                                      ? HealthCheckResult.Healthy()
                                                      : HealthCheckResult.Unhealthy("Connection to MongoDB cluster dropped.")),
         _ => throw new ArgumentOutOfRangeException(nameof(tag),
                                                    tag,
                                                    null),
       };

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
  {
    if (clientSessionHandle_ is not null)
    {
      return Task.CompletedTask;
    }

    lock (lockObj_)
    {
      clientSessionHandle_ ??= client_.StartSession(cancellationToken: cancellationToken);
    }

    return Task.CompletedTask;
  }

  public IClientSessionHandle Get()
  {
    if (clientSessionHandle_ is null)
    {
      throw new NullReferenceException($"{nameof(clientSessionHandle_)} not initialized, call the Init function beforehand.");
    }

    return clientSessionHandle_;
  }
}
