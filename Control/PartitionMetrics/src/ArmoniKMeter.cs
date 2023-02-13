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
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;
using ArmoniK.Core.Control.PartitionMetrics.Options;

using Fennel.CSharp;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Control.PartitionMetrics;

public class ArmoniKMeter : Meter, IHostedService
{
  private readonly HttpClient                                     client_;
  private readonly IDictionary<string, ObservableGauge<long>>     gauges_;
  private readonly ILogger                                        logger_;
  private readonly ExecutionSingleizer<IDictionary<string, long>> measurements_;
  private readonly string                                         metricsExporterUri_;
  private readonly IPartitionTable                                partitionTable_;
  private          int                                            i_;

  public ArmoniKMeter(IPartitionTable       partitionTable,
                      MetricsExporter       optionsMetricsExporter,
                      IHttpClientFactory    httpClientFactory,
                      ILogger<ArmoniKMeter> logger)
    : base(nameof(ArmoniKMeter))
  {
    using var _ = logger.LogFunction();

    partitionTable_ = partitionTable;
    logger_         = logger;
    gauges_         = new Dictionary<string, ObservableGauge<long>>();
    measurements_   = new ExecutionSingleizer<IDictionary<string, long>>();

    CreateObservableCounter("test",
                            () => i_++);
    logger.LogDebug("Meter added");

    client_ = httpClientFactory.CreateClient();

    metricsExporterUri_ = $"{optionsMetricsExporter.Host}:{optionsMetricsExporter.Port}{optionsMetricsExporter.Path}";
    AddGauge("");
  }

  public async Task StartAsync(CancellationToken cancellationToken)
    // Call FetchMeasurementsAsync in order to populate gauges.
    => await measurements_.Call(FetchMeasurementsAsync,
                                CancellationToken.None)
                          .ConfigureAwait(false);

  public Task StopAsync(CancellationToken cancellationToken)
    => Task.CompletedTask;

  /// <summary>
  ///   Fetch all the measurements from the partitionTable and the metrics exporter.
  /// </summary>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Dictionary of the pod count for each partition.
  /// </returns>
  private async Task<IDictionary<string, long>> FetchMeasurementsAsync(CancellationToken cancellationToken)
  {
    // Populate dictionary from request
    var measurements = new Dictionary<string, long>();

    string rawMetrics;
    try
    {
      rawMetrics = await client_.GetStringAsync(metricsExporterUri_,
                                                cancellationToken)
                                .ConfigureAwait(false);
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                         "Error while trying to read metrics from metrics exporter");
      rawMetrics = string.Empty;
    }

    var parsedLines = Prometheus.ParseText(rawMetrics);

    var metrics = new Hashtable();
    foreach (var line in parsedLines.Where(line => line.IsMetric))
    {
      if (line is Metric metric)
      {
        metrics.Add(metric.MetricName,
                    metric);
      }
    }

    // Count per partitions
    await foreach (var partition in partitionTable_.GetPartitionWithAllocationAsync(cancellationToken)
                                                   .ConfigureAwait(false))
    {
      var metricValue = (metrics[$"armonik_{partition.PartitionId}_tasks_queued"] as Metric)?.MetricValue;

      if (metricValue != null)
      {
        var numberOfTasks = (long)metricValue.Value;
        measurements[partition.PartitionId] = Math.Min(numberOfTasks,
                                                       partition.PodMax);

        AddGauge(partition.PartitionId);
      }
      else
      {
        logger_.LogWarning("Gauge should have been added");
      }
    }

    return measurements;
  }

  /// <summary>
  ///   Get the Number of pods for a given partition.
  /// </summary>
  /// <param name="partition">Name of the partition to filter on</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Number of pods for the given partition.
  /// </returns>
  private async Task<long> GetMeasurementAsync(string            partition,
                                               CancellationToken cancellationToken)
  {
    var measurements = await measurements_.Call(FetchMeasurementsAsync,
                                                cancellationToken)
                                          .ConfigureAwait(false);
    return measurements[partition];
  }

  /// <summary>
  ///   Add gauge if it does not already exist
  /// </summary>
  /// <param name="partition">Name of the partition to be metered</param>
  private void AddGauge(string partition)
  {
    if (gauges_.ContainsKey(partition))
    {
      return;
    }

    var gauge = CreateObservableGauge($"armonik_{partition}_opt",
                                      () => new Measurement<long>(GetMeasurementAsync(partition,
                                                                                      CancellationToken.None)
                                                                    .Result));

    gauges_[partition] = gauge;
  }
}
