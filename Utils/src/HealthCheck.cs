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

using ArmoniK.Core.Base;

using JetBrains.Annotations;

using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ArmoniK.Core.Utils;

/// <summary>
///   Implementation of <see cref="IHealthCheck" /> that manages FailureStatus and simplifies the implementation of Health
///   Checks
/// </summary>
[PublicAPI]
public class HealthCheck : IHealthCheck
{
  private readonly IHealthCheckProvider healthCheckProvider_;
  private readonly HealthCheckTag       tag_;

  /// <summary>
  ///   Constructor that creates a HealthCheck from a <see cref="IHealthCheckProvider" />
  /// </summary>
  /// <param name="healthCheckProvider">Interface for classes that exposes a simplified Health Check</param>
  /// <param name="tag">Tag to filter which health check is executed</param>
  public HealthCheck(IHealthCheckProvider healthCheckProvider,
                     HealthCheckTag       tag)
  {
    healthCheckProvider_ = healthCheckProvider;
    tag_                 = tag;
  }


  /// <exception cref="ArgumentOutOfRangeException"></exception>
  /// <inheritdoc />
  public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context,
                                                        CancellationToken  cancellationToken = default)
  {
    var result = await healthCheckProvider_.Check(tag_)
                                           .ConfigureAwait(false);

    if (result.Status is HealthStatus.Healthy or HealthStatus.Degraded)
    {
      return result;
    }

    return context.Registration.FailureStatus switch
           {
             HealthStatus.Unhealthy => HealthCheckResult.Unhealthy(result.Description,
                                                                   result.Exception,
                                                                   result.Data),
             HealthStatus.Degraded => HealthCheckResult.Degraded(result.Description,
                                                                 result.Exception,
                                                                 result.Data),
             HealthStatus.Healthy => HealthCheckResult.Healthy(result.Description + result.Exception,
                                                               result.Data),
             _ => throw new ArgumentOutOfRangeException(nameof(context),
                                                        "Context has been registered with a non supported FailureStatus"),
           };
  }
}
