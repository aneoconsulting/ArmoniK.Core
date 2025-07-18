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

using ArmoniK.Core.Base.DataStructures;

using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ArmoniK.Core.Common;

/// <summary>
///   Records the health check results to be retrieved at any time by other components of the application.
/// </summary>
public class HealthCheckRecord
{
  private readonly Dictionary<HealthCheckTag, HealthReport> records_ = new();

  /// <summary>
  ///   Records a HealthCheckResult.
  /// </summary>
  /// <param name="tag">The type of health check to record.</param>
  /// <param name="report">The result of the health check.</param>
  public void Record(HealthCheckTag tag,
                     HealthReport   report)
    => records_[tag] = report;

  /// <summary>
  ///   Retrieves the last HealthCheckResult.
  /// </summary>
  /// <param name="tag">The type of health check.</param>
  /// <returns>The result of the health check.</returns>
  public HealthReport LastCheck(HealthCheckTag tag = HealthCheckTag.Liveness)
  {
    if (records_.TryGetValue(tag,
                             out var report))
    {
      return report;
    }

    return new HealthReport(new Dictionary<string, HealthReportEntry>(),
                            HealthStatus.Healthy,
                            TimeSpan.Zero);
  }

  /// <summary>
  ///   Publishes health check reports to a <see cref="HealthCheckRecord" />, recording the results for later retrieval by
  ///   other components.
  ///   Implements <see cref="IHealthCheckPublisher" /> to integrate with ASP.NET Core health checks.
  /// </summary>
  public class Publisher(HealthCheckRecord healthCheckRecord) : IHealthCheckPublisher
  {
    /// <inheritdoc />
    public Task PublishAsync(HealthReport      report,
                             CancellationToken cancellationToken)
    {
      var tags = report.Entries.SelectMany(entry => entry.Value.Tags)
                       .ToHashSet();

      foreach (var tag in Enum.GetValues<HealthCheckTag>())
      {
        if (tags.Contains(tag.ToString()))
        {
          healthCheckRecord.Record(tag,
                                   new HealthReport(report.Entries.Where(kv => kv.Value.Tags.Contains(tag.ToString()))
                                                          .ToDictionary(),
                                                    report.TotalDuration));
        }
      }

      return Task.CompletedTask;
    }
  }
}
