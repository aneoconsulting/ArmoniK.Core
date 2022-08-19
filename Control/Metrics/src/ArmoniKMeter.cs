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
// but WITHOUT ANY WARRANTY, without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Control.Metrics;

public class ArmoniKMeter : Meter, IHostedService
{
  private const    string                                                        QueuedName = "queued";
  private readonly IDictionary<Tuple<string, string>, ObservableGauge<long>>     gauges_;
  private readonly ILogger                                                       logger_;
  private readonly ExecutionSingleizer<IDictionary<Tuple<string, string>, long>> measurements_;
  private readonly ITaskTable                                                    taskTable_;
  private          int                                                           i_;

  public ArmoniKMeter(ITaskTable            taskTable,
                      ILogger<ArmoniKMeter> logger)
    : base(nameof(ArmoniKMeter))
  {
    using var _ = logger.LogFunction();

    taskTable_    = taskTable;
    logger_       = logger;
    gauges_       = new Dictionary<Tuple<string, string>, ObservableGauge<long>>();
    measurements_ = new ExecutionSingleizer<IDictionary<Tuple<string, string>, long>>();

    CreateObservableCounter("test",
                            () => i_++);
    logger.LogDebug("Meter added");
  }

  public async Task StartAsync(CancellationToken cancellationToken)
    // Call FetchMeasurementsAsync in order to populate gauges.
    => await measurements_.Call(FetchMeasurementsAsync,
                                CancellationToken.None)
                          .ConfigureAwait(false);

  public Task StopAsync(CancellationToken cancellationToken)
    => Task.CompletedTask;

  /// <summary>
  ///   Fetch all the measurements from the taskTable.
  /// </summary>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Dictionary of the task count for each partition/status.
  /// </returns>
  private async Task<IDictionary<Tuple<string, string>, long>> FetchMeasurementsAsync(CancellationToken cancellationToken)
  {
    _ = logger_;

    // DB request
    var partitionStatusCounts = await taskTable_.CountPartitionTasksAsync(cancellationToken)
                                                .ConfigureAwait(false);

    // Populate dictionary from request
    var measurements = new Dictionary<Tuple<string, string>, long>();

    // Aggregates across partitions
    foreach (var status in (TaskStatus[])Enum.GetValues(typeof(TaskStatus)))
    {
      var statusName = status.ToString();
      measurements[Tuple.Create("",
                                statusName)] = 0;
      AddGauge("",
               statusName);
    }

    measurements[Tuple.Create("",
                              QueuedName)] = 0;
    AddGauge("",
             QueuedName);

    // initialize all the gauges for each partition
    foreach (var partition in partitionStatusCounts.Select(p => p.PartitionId)
                                                   .Distinct())
    {
      foreach (var status in (TaskStatus[])Enum.GetValues(typeof(TaskStatus)))
      {
        var statusName = status.ToString();
        measurements[Tuple.Create(partition,
                                  statusName)] = 0;
        AddGauge(partition,
                 statusName);
      }

      measurements[Tuple.Create(partition,
                                QueuedName)] = 0;
      AddGauge(partition,
               QueuedName);
    }

    // Count per partitions
    foreach (var psc in partitionStatusCounts)
    {
      var statusName = psc.Status.ToString();
      measurements[Tuple.Create(psc.PartitionId,
                                statusName)] = psc.Count;
      measurements[Tuple.Create("",
                                statusName)] += psc.Count;
      if (psc.Status is TaskStatus.Dispatched or TaskStatus.Submitted or TaskStatus.Processing)
      {
        measurements[Tuple.Create(psc.PartitionId,
                                  QueuedName)] = psc.Count;
        measurements[Tuple.Create("",
                                  QueuedName)] += psc.Count;
      }
    }

    return measurements;
  }

  /// <summary>
  ///   Get the Number of tasks for a given partition and task status.
  /// </summary>
  /// <param name="partition">Name of the partition to filter on</param>
  /// <param name="status">Name of the status to filter on</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Number of tasks for the given partition and status.
  /// </returns>
  private async Task<long> GetMeasurementAsync(string            partition,
                                               string            status,
                                               CancellationToken cancellationToken)
  {
    var measurements = await measurements_.Call(FetchMeasurementsAsync,
                                                cancellationToken)
                                          .ConfigureAwait(false);
    return measurements[Tuple.Create(partition,
                                     status)];
  }

  /// <summary>
  ///   Add gauge if it does not already exist
  /// </summary>
  /// <param name="partition">Name of the partition to be metered</param>
  /// <param name="statusName">Name of the Status to be metered</param>
  private void AddGauge(string partition,
                        string statusName)
  {
    var key = Tuple.Create(partition,
                           statusName);
    if (gauges_.ContainsKey(key))
    {
      return;
    }

    var metricName = string.IsNullOrEmpty(partition)
                       ? $"armonik_tasks_{statusName.ToLower()}"
                       : $"armonik_{partition}_tasks_{statusName.ToLower()}";

    var gauge = CreateObservableGauge(metricName,
                                      () => new Measurement<long>(GetMeasurementAsync(partition,
                                                                                      statusName,
                                                                                      CancellationToken.None)
                                                                    .Result));

    gauges_[key] = gauge;
  }
}
