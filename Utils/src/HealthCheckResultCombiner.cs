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
using System.Text;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Utils;

using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ArmoniK.Core.Utils;

/// <summary>
///   Helper class to combine the results of multiple <inheritdoc cref="IHealthCheckProvider.Check" />
/// </summary>
public static class HealthCheckResultCombiner
{
  /// <summary>
  ///   Combine the results of multiple <inheritdoc cref="IHealthCheckProvider.Check" /> for a given tag and description
  /// </summary>
  /// <param name="tag">The tag on which to combine the results</param>
  /// <param name="desc">The description to add to the results</param>
  /// <param name="providers">The sources from which to get the results</param>
  /// <returns>
  ///   The combined result
  /// </returns>
  public static async Task<HealthCheckResult> Combine(HealthCheckTag                tag,
                                                      string                        desc,
                                                      params IHealthCheckProvider[] providers)
  {
    var exceptions  = new List<Exception>();
    var data        = new Dictionary<string, object>();
    var description = new StringBuilder();
    var worstStatus = HealthStatus.Healthy;
    description.AppendLine(desc);

    foreach (var healthCheckResult in await providers.Select(p => p.Check(tag))
                                                     .WhenAll()
                                                     .ConfigureAwait(false))
    {
      if (healthCheckResult.Status == HealthStatus.Healthy)
      {
        continue;
      }

      if (healthCheckResult.Exception is not null)
      {
        exceptions.Add(healthCheckResult.Exception);
      }

      foreach (var (key, value) in healthCheckResult.Data)
      {
        data[key] = value;
      }

      if (healthCheckResult.Description is not null)
      {
        description.AppendLine(healthCheckResult.Description);
      }

      worstStatus = worstStatus < healthCheckResult.Status
                      ? worstStatus
                      : healthCheckResult.Status;
    }

    return new HealthCheckResult(worstStatus,
                                 description.ToString(),
                                 new AggregateException(exceptions),
                                 data);
  }
}
