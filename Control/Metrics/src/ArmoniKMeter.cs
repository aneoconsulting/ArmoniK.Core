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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Api.Worker.Utils;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Control.Metrics;

public class ArmoniKMeter : Meter, IHostedService
{
  private          int                                                           i;
  private readonly ITaskTable                                                    taskTable_;
  private          ILogger                                                       logger_;
  private readonly ExecutionSingleizer<IDictionary<Tuple<string, string>, long>> measurements_;
  private readonly IDictionary<Tuple<string, string>, ObservableGauge<long>>     gauges_;

  private const string QueuedName = "queued";

  public ArmoniKMeter(ITaskTable            taskTable,
                      ILogger<ArmoniKMeter> logger)
    : base(nameof(ArmoniKMeter))
  {
    using var log = logger.LogFunction();

    taskTable_    = taskTable;
    logger_       = logger;
    gauges_       = new Dictionary<Tuple<string, string>, ObservableGauge<long>>();
    measurements_ = new ExecutionSingleizer<IDictionary<Tuple<string, string>, long>>();

    _ = measurements_.Call(UpdateMeasurementsAsync,
                           CancellationToken.None)
                     .Result;

    CreateObservableCounter("test",
                            () => i++);
    logger.LogDebug("Meter added");
  }

  public Task StartAsync(CancellationToken cancellationToken)
    => Task.CompletedTask;

  public Task StopAsync(CancellationToken cancellationToken)
    => Task.CompletedTask;

  private async Task<IDictionary<Tuple<string, string>, long>> UpdateMeasurementsAsync(CancellationToken cancellationToken)
  {
    var partitionStatusCounts = await taskTable_.CountPartitionTasksAsync(new TaskFilter
                                                                          {
                                                                            Task    = new TaskFilter.Types.IdsRequest(),
                                                                            Session = new TaskFilter.Types.IdsRequest(),
                                                                          },
                                                                          cancellationToken)
                                                .ConfigureAwait(false);
    var measurements = new Dictionary<Tuple<string, string>, long>();

    foreach (var status in (TaskStatus[])Enum.GetValues(typeof(TaskStatus)))
    {
      var statusName = status.ToString();
      measurements[new Tuple<string, string>("",
                                             statusName)] = 0;
      AddGauge("",
               statusName);
    }

    measurements[new Tuple<string, string>("",
                                           QueuedName)] = 0;
    AddGauge("",
             QueuedName);

    foreach (var psc in partitionStatusCounts)
    {
      var statusName = psc.Status.ToString();
      measurements[new Tuple<string, string>(psc.PartitionId,
                                             statusName)] = psc.Count;
      measurements[new Tuple<string, string>("",
                                             statusName)] += psc.Count;
      if (psc.Status is TaskStatus.Dispatched or TaskStatus.Submitted or TaskStatus.Processing)
      {
        measurements[new Tuple<string, string>(psc.PartitionId,
                                               QueuedName)] = psc.Count;
        measurements[new Tuple<string, string>("",
                                               QueuedName)] += psc.Count;
        AddGauge(psc.PartitionId,
                 QueuedName);
      }

      AddGauge(psc.PartitionId,
               statusName);
    }

    return measurements;
  }

  private async Task<long> GetMeasurementAsync(string            partition,
                                               string            status,
                                               CancellationToken cancellationToken)
  {
    var measurements = await measurements_.Call(UpdateMeasurementsAsync,
                                                cancellationToken)
                                          .ConfigureAwait(false);
    return measurements[new Tuple<string, string>(partition,
                                                  status)];
  }

  private void AddGauge(string partition,
                        string statusName)
  {
    var key = new Tuple<string, string>(partition,
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
