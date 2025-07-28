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

/// <summary>
///   Provides a MongoDB session handle for database operations, implementing initialization and health checks.
/// </summary>
public class SessionProvider : IInitializable
{
  private readonly IMongoClient          client_;
  private readonly object                lockObj_ = new();
  private          IClientSessionHandle? clientSessionHandle_;

  /// <summary>
  ///   Initializes a new instance of the <see cref="SessionProvider" /> class.
  /// </summary>
  /// <param name="client">The MongoDB client used to create sessions.</param>
  [UsedImplicitly]
  public SessionProvider(IMongoClient client)
    => client_ = client;

  /// <summary>
  ///   Checks the health of the MongoDB session provider.
  /// </summary>
  /// <param name="tag">The health check tag indicating the type of health check.</param>
  /// <returns>A task that represents the asynchronous health check operation, containing the result of the health check.</returns>
  /// <exception cref="ArgumentOutOfRangeException">
  ///   Thrown when the provided health check tag is not recognized.
  /// </exception>
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

  /// <summary>
  ///   Initializes the MongoDB session handle.
  /// </summary>
  /// <param name="cancellationToken">A cancellation token to observe while waiting for the initialization to complete.</param>
  /// <returns>A task that represents the asynchronous initialization operation.</returns>
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

  /// <summary>
  ///   Retrieves the current MongoDB session handle.
  /// </summary>
  /// <returns>The current <see cref="IClientSessionHandle" /> for MongoDB operations.</returns>
  /// <exception cref="NullReferenceException">
  ///   Thrown when the session handle has not been initialized.
  ///   Ensure that the <see cref="Init" /> method has been called before accessing the session handle.
  /// </exception>
  public IClientSessionHandle Get()
  {
    if (clientSessionHandle_ is null)
    {
      throw new NullReferenceException($"{nameof(clientSessionHandle_)} not initialized, call the Init function beforehand.");
    }

    return clientSessionHandle_;
  }
}
