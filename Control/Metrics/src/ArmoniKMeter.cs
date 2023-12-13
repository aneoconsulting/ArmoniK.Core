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
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Utils;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

namespace ArmoniK.Core.Control.Metrics;

public class ArmoniKMeter : Meter, IHostedService
{
  private const string QueuedName = "queued";

  private static readonly TaskStatus[] Statuses =
  {
    //TaskStatus.Unspecified,
    TaskStatus.Creating,
    TaskStatus.Submitted,
    TaskStatus.Dispatched,
    //TaskStatus.Completed,
    //TaskStatus.Error,
    //TaskStatus.Timeout,
    //TaskStatus.Cancelling,
    //TaskStatus.Cancelled,
    TaskStatus.Processing,
    TaskStatus.Processed,
    TaskStatus.Retried,
  };

  private readonly IDictionary<(string partition, string status), MeasurementReader<long>> gauges_;
  private readonly ILogger                                                                 logger_;
  private readonly IPartitionTable                                                         partitionTable_;
  private readonly ITaskTable                                                              taskTable_;

  public ArmoniKMeter(ITaskTable            taskTable,
                      IPartitionTable       partitionTable,
                      ILogger<ArmoniKMeter> logger)
    : base(nameof(ArmoniKMeter))
  {
    using var _ = logger.LogFunction();

    taskTable_      = taskTable;
    partitionTable_ = partitionTable;
    logger_         = logger;
    gauges_         = new Dictionary<(string partition, string status), MeasurementReader<long>>();

    var statusNames = Statuses.Select(status => status.ToString())
                              .ToList();
    statusNames.Add(QueuedName);

    var partitionExec = new ExecutionSingleizer<ICollection<string>>(ExpireAfter);

    foreach (var statusName in statusNames)
    {
      AddGauge("",
               statusName,
               async ct =>
               {
                 var partitions = await partitionExec.Call(AddPartitionGauges,
                                                           ct)
                                                     .ConfigureAwait(false);
                 var counts = await partitions.Select(partition => gauges_[(partition, statusName)]
                                                        .ReadAsync(ct))
                                              .WhenAll()
                                              .ConfigureAwait(false);
                 return counts.Sum();
               });
    }
  }

  private static TimeSpan ExpireAfter
    => TimeSpan.FromSeconds(5);

  public async Task StartAsync(CancellationToken cancellationToken) // Call an aggregate gauge in order to populate all gauges.
  {
    await gauges_[("", QueuedName)]
          .ReadAsync(cancellationToken)
          .ConfigureAwait(false);

    logger_.LogDebug("Added the following gauges: {Gauges}",
                     gauges_.Values.Select(reader => reader.Name));
  }

  public Task StopAsync(CancellationToken cancellationToken)
    => Task.CompletedTask;

  private async Task<ICollection<string>> AddPartitionGauges(CancellationToken cancellationToken)
  {
    var partitions = await partitionTable_.FindPartitionsAsync(partition => true,
                                                               partition => partition.PartitionId,
                                                               cancellationToken)
                                          .ToListAsync(cancellationToken)
                                          .ConfigureAwait(false);

    foreach (var partition in partitions)
    {
      var queuedReaders = new List<MeasurementReader<long>>();

      foreach (var status in Statuses)
      {
        var statusName = status.ToString();
        var reader = AddGauge(partition,
                              statusName,
                              ct => CountPartitionStatus(partition,
                                                         status,
                                                         ct));

        if (status is TaskStatus.Dispatched or TaskStatus.Submitted or TaskStatus.Processing)
        {
          queuedReaders.Add(reader);
        }
      }

      AddGauge(partition,
               QueuedName,
               async ct =>
               {
                 var counts = await queuedReaders.Select(reader => reader.ReadAsync(ct))
                                                 .WhenAll()
                                                 .ConfigureAwait(false);
                 return counts.Sum();
               });
    }

    return partitions;
  }

  private MeasurementReader<long> AddGauge(string                              partition,
                                           string                              statusName,
                                           Func<CancellationToken, Task<long>> f)
  {
    if (gauges_.TryGetValue((partition, statusName),
                            out var reader))
    {
      return reader;
    }

    reader = new MeasurementReader<long>(partition,
                                         statusName,
                                         f);
    CreateObservableGauge(reader.Name,
                          reader.Read);
    gauges_.Add((partition, statusName),
                reader);

    return reader;
  }

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

  private class MeasurementReader<T>
    where T : struct
  {
    private readonly ExecutionSingleizer<T>           exec_;
    private readonly Func<CancellationToken, Task<T>> readFunc_;

    public MeasurementReader(string                           partition,
                             string                           status,
                             Func<CancellationToken, Task<T>> readFunc)
    {
      Name = string.IsNullOrEmpty(partition)
               ? $"armonik_tasks_{status.ToLower()}"
               : $"armonik_{partition}_tasks_{status.ToLower()}";
      exec_     = new ExecutionSingleizer<T>(ExpireAfter);
      readFunc_ = readFunc;
    }

    public string Name { get; }

    public Task<T> ReadAsync(CancellationToken cancellationToken)
      => exec_.Call(readFunc_,
                    cancellationToken);

    public T Read()
      => ReadAsync(CancellationToken.None)
        .WaitSync();
  }
}
