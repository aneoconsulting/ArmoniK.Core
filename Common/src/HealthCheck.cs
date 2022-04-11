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
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Threading;
using System.Threading.Tasks;

using JetBrains.Annotations;

using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ArmoniK.Core.Common;

[PublicAPI]
public class HealthCheck : IHealthCheck
{
  private readonly IHealthCheckProvider healthCheckProvider_;
  private readonly HealthCheckTag       tag_;

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
    if (await healthCheckProvider_.Check(tag_)
                                  .ConfigureAwait(false))
    {
      return HealthCheckResult.Healthy();
    }

    return context.Registration.FailureStatus switch
           {
             HealthStatus.Unhealthy => HealthCheckResult.Unhealthy(),
             HealthStatus.Degraded  => HealthCheckResult.Degraded(),
             HealthStatus.Healthy   => HealthCheckResult.Healthy(),
             _ => throw new ArgumentOutOfRangeException(nameof(context),
                                                        "Context has been registered with a non supported FailureStatus"),
           };
  }
}
