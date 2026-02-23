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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Control.Metrics.Options;
using ArmoniK.Utils;

using Microsoft.Extensions.Logging;

using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace ArmoniK.Core.Control.Metrics;

public class ArmoniKMeter
{
  private const string QueuedName = "queued";

  private static readonly Dictionary<string, TaskStatus[]> ComputedStatuses = new()
                                                                              {
                                                                                {
                                                                                  QueuedName, new[]
                                                                                              {
                                                                                                TaskStatus.Submitted,
                                                                                                TaskStatus.Dispatched,
                                                                                                TaskStatus.Processing,
                                                                                              }
                                                                                },
                                                                              };

  private readonly ExecutionSingleizer<string> exec_;
  private readonly ILogger<ArmoniKMeter>       logger_;
  private readonly int                         parallelism_;
  private readonly IPartitionTable             partitionTable_;
  private readonly HashSet<TaskStatus>         statuses_ = new();
  private readonly ITaskTable                  taskTable_;


  public ArmoniKMeter(ITaskTable            taskTable,
                      IPartitionTable       partitionTable,
                      MetricsExporter       configuration,
                      ILogger<ArmoniKMeter> logger)
  {
    taskTable_      = taskTable;
    partitionTable_ = partitionTable;
    logger_         = logger;
    exec_           = new ExecutionSingleizer<string>(configuration.CacheValidity);
    parallelism_    = configuration.DegreeOfParallelism;

    IEnumerable<string> allMetrics = new[]
                                     {
                                       QueuedName,
                                     };

    if (configuration.Metrics.Any())
    {
      allMetrics = allMetrics.Concat(configuration.Metrics.ToLower()
                                                  .Split(","));
    }

    foreach (var metric in allMetrics)
    {
      if (ComputedStatuses.TryGetValue(metric,
                                       out var computed))
      {
        ComputedStatuses[metric] = computed;
        statuses_.UnionWith(computed);
      }
      else if (Enum.TryParse<TaskStatus>(metric,
                                         true,
                                         out var status))
      {
        statuses_.Add(status);
      }
      else
      {
        logger_.LogWarning("{Metric} is not a valid metric, ignored",
                           metric);
      }
    }
  }

  public Task<string> GetMetricsAsync(CancellationToken cancellationToken)
    => exec_.Call(BuildMetricsAsync,
                  cancellationToken);

  private async Task<long> CountPartitionStatus(string            partitionId,
                                                TaskStatus        status,
                                                CancellationToken cancellationToken)
  {
    var (_, count) = await taskTable_.ListTasksAsync(task => task.Options.PartitionId == partitionId && task.Status == status,
                                                     task => task.TaskId,
                                                     task => new ValueTuple(),
                                                     true,
                                                     0,
                                                     0,
                                                     cancellationToken)
                                     .ConfigureAwait(false);

    return count;
  }

  private async Task<string> BuildMetricsAsync(CancellationToken cancellationToken)
  {
    var partitions = await partitionTable_.FindPartitionsAsync(partition => true,
                                                               partition => partition.PartitionId,
                                                               cancellationToken)
                                          .ToListAsync(cancellationToken)
                                          .ConfigureAwait(false);

    var product =
      from p in partitions
      from s in statuses_
      select new PartitionStatusCount(p,
                                      s,
                                      0);

    var counts = await product.ParallelSelect(new ParallelTaskOptions(parallelism_),
                                              async psc => psc with
                                                           {
                                                             Count = await CountPartitionStatus(psc.Partition,
                                                                                                psc.Status,
                                                                                                cancellationToken)
                                                                       .ConfigureAwait(false),
                                                           })
                              .ToDictionaryAsync(count => (count.Partition, count.Status),
                                                 count => count.Count,
                                                 cancellationToken: cancellationToken)
                              .ConfigureAwait(false);


    var sb = new StringBuilder();
    foreach (var status in statuses_)
    {
      EmitGauge(sb,
                "",
                status.ToString()
                      .ToLower(),
                partitions.Select(partition => counts[(partition, status)])
                          .Sum());
    }

    foreach (var (metrics, statusNames) in ComputedStatuses)
    {
      EmitGauge(sb,
                "",
                metrics,
                counts.Where(pair => statusNames.Contains(pair.Key.Status))
                      .Select(pair => pair.Value)
                      .Sum());
    }

    foreach (var partition in partitions)
    {
      foreach (var status in statuses_)
      {
        EmitGauge(sb,
                  partition,
                  status.ToString()
                        .ToLower(),
                  counts[(partition, status)]);
      }

      foreach (var (metrics, statusNames) in ComputedStatuses)
      {
        EmitGauge(sb,
                  partition,
                  metrics,
                  statusNames.Select(status => counts[(partition, status)])
                             .Sum());
      }
    }

    return sb.ToString();
  }

  private static void EmitGauge(StringBuilder sb,
                                string        partition,
                                string        metricName,
                                long          value)
  {
    var name = string.IsNullOrEmpty(partition)
                 ? $"armonik_tasks_{metricName}"
                 : $"armonik_{partition}_tasks_{metricName}";
    sb.Append($"# HELP {name} \n");
    sb.Append($"# TYPE {name} gauge\n");
    sb.Append($"{name} {value}\n");
  }
}

internal record PartitionStatusCount(string     Partition,
                                     TaskStatus Status,
                                     long       Count);
