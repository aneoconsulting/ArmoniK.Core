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
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Control.Metrics.Options;

using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using NUnit.Framework;

using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

namespace ArmoniK.Core.Control.Metrics.Tests;

[TestFixture(TestOf = typeof(ArmoniKMeter))]
public class ArmoniKMeterTest
{
  [SetUp]
  public void SetUp()
  {
    taskTableMock_      = new Mock<ITaskTable>();
    partitionTableMock_ = new Mock<IPartitionTable>();
  }

  private Mock<IPartitionTable> partitionTableMock_ = null!;
  private Mock<ITaskTable>      taskTableMock_      = null!;

  /// <summary>
  ///   Creates a minimal TaskData whose Options.PartitionId and Status match the given values,
  ///   used to exercise compiled filter expressions in ListTasksAsync.
  /// </summary>
  private static TaskData MakeTask(string     partition,
                                   TaskStatus status)
  {
    var opts = new TaskOptions(new Dictionary<string, string>(),
                               TimeSpan.FromSeconds(1),
                               1,
                               1,
                               partition,
                               "",
                               "",
                               "",
                               "",
                               "");
    return new TaskData("session",
                        "taskId",
                        "",
                        "",
                        "payload",
                        "createdBy",
                        Array.Empty<string>(),
                        Array.Empty<string>(),
                        Array.Empty<string>(),
                        Array.Empty<string>(),
                        status,
                        opts,
                        new Output(OutputStatus.Success,
                                   ""));
  }

  /// <summary>Configures FindPartitionsAsync to return the given partition IDs.</summary>
  private void SetupPartitions(params string[] partitionIds)
    => partitionTableMock_.Setup(pt => pt.FindPartitionsAsync(It.IsAny<Expression<Func<PartitionData, bool>>>(),
                                                              It.IsAny<Expression<Func<PartitionData, string>>>(),
                                                              It.IsAny<CancellationToken>()))
                          .Returns<Expression<Func<PartitionData, bool>>, Expression<Func<PartitionData, string>>, CancellationToken>((_,
                                                                                                                                       selector,
                                                                                                                                       _) => partitionIds
                                                                                                                                             .Select(id => selector
                                                                                                                                                       .Compile()(new
                                                                                                                                                                    PartitionData(id,
                                                                                                                                                                                  new
                                                                                                                                                                                    List
                                                                                                                                                                                    <string>(),
                                                                                                                                                                                  1,
                                                                                                                                                                                  1,
                                                                                                                                                                                  1,
                                                                                                                                                                                  1,
                                                                                                                                                                                  null)))
                                                                                                                                             .ToAsyncEnumerable());

  /// <summary>
  ///   Configures ListTasksAsync to return the count matching each (partition, status) entry.
  ///   The filter is compiled and tested against a minimal TaskData to determine which entry matches.
  /// </summary>
  private void SetupTaskCounts(params (string partition, TaskStatus status, long count)[] entries)
    => taskTableMock_.Setup(tt => tt.ListTasksAsync(It.IsAny<Expression<Func<TaskData, bool>>>(),
                                                    It.IsAny<Expression<Func<TaskData, object?>>>(),
                                                    It.IsAny<Expression<Func<TaskData, ValueTuple>>>(),
                                                    It.IsAny<bool>(),
                                                    It.IsAny<int>(),
                                                    It.IsAny<int>(),
                                                    It.IsAny<CancellationToken>()))
                     .Returns<Expression<Func<TaskData, bool>>, Expression<Func<TaskData, object?>>, Expression<Func<TaskData, ValueTuple>>, bool, int, int,
                       CancellationToken>((filter,
                                           _,
                                           _,
                                           _,
                                           _,
                                           _,
                                           _) =>
                                          {
                                            var compiled = filter.Compile();
                                            var count = entries.Where(e => compiled(MakeTask(e.partition,
                                                                                             e.status)))
                                                               .Sum(e => e.count);
                                            return Task.FromResult<(IEnumerable<ValueTuple>, long)>((Enumerable.Empty<ValueTuple>(), count));
                                          });

  private ArmoniKMeter CreateMeter(string    metrics       = "",
                                   TimeSpan? cacheValidity = null,
                                   int       parallelism   = 1)
    => new(taskTableMock_.Object,
           partitionTableMock_.Object,
           new MetricsExporter
           {
             Metrics             = metrics,
             CacheValidity       = cacheValidity ?? TimeSpan.FromMinutes(1),
             DegreeOfParallelism = parallelism,
           },
           NullLogger<ArmoniKMeter>.Instance);

  /// <summary>Parses Prometheus text output into a name→value dictionary (skips # lines).</summary>
  private static Dictionary<string, long> ParseMetrics(string output)
    => output.Split('\n',
                    StringSplitOptions.RemoveEmptyEntries)
             .Where(l => !l.StartsWith('#'))
             .ToDictionary(l => l.Split(' ')[0],
                           l => long.Parse(l.Split(' ')[1]));


  [Test]
  public void ParseMetricsShouldBeOk()
  {
    var sb = new StringBuilder();
    sb.AppendLine("# HELP armonik_tasks_queued Total number of queued tasks");
    sb.AppendLine("# TYPE armonik_tasks_queued gauge");
    sb.AppendLine("armonik_tasks_queued 5");
    sb.AppendLine("# HELP armonik_tasks_queued Total number of queued tasks");
    sb.AppendLine("# TYPE armonik_tasks_queued gauge");
    sb.AppendLine("armonik_tasks_submitted 3");

    var metrics = ParseMetrics(sb.ToString());

    // Root metrics are always emitted even with no partitions
    Assert.That(metrics["armonik_tasks_queued"],
                Is.EqualTo(5));
    Assert.That(metrics["armonik_tasks_submitted"],
                Is.EqualTo(3));
  }

  [Test]
  public async Task GetMetricsWithNoPartitionsShouldOnlyEmitRootMetricsAtZero()
  {
    SetupPartitions();
    SetupTaskCounts();
    var meter = CreateMeter();

    var metrics = ParseMetrics(await meter.GetMetricsAsync(CancellationToken.None)
                                          .ConfigureAwait(false));

    // Root metrics are always emitted even with no partitions
    Assert.That(metrics["armonik_tasks_queued"],
                Is.EqualTo(0));
    Assert.That(metrics["armonik_tasks_submitted"],
                Is.EqualTo(0));
    Assert.That(metrics["armonik_tasks_dispatched"],
                Is.EqualTo(0));
    Assert.That(metrics["armonik_tasks_processing"],
                Is.EqualTo(0));
    // No per-partition entries (per-partition keys have the form armonik_{partition}_tasks_{status})
    Assert.That(metrics.Keys.Any(k => !k.StartsWith("armonik_tasks_")),
                Is.False);
  }

  [Test]
  public async Task GetMetricsWithSinglePartitionShouldHaveCorrectQueuedSum()
  {
    SetupPartitions("p1");
    SetupTaskCounts(("p1", TaskStatus.Submitted, 3),
                    ("p1", TaskStatus.Dispatched, 2),
                    ("p1", TaskStatus.Processing, 1));
    var meter = CreateMeter();

    var metrics = ParseMetrics(await meter.GetMetricsAsync(CancellationToken.None)
                                          .ConfigureAwait(false));

    // Root aggregates (no partition prefix)
    Assert.That(metrics["armonik_tasks_queued"],
                Is.EqualTo(6));
    Assert.That(metrics["armonik_tasks_submitted"],
                Is.EqualTo(3));
    Assert.That(metrics["armonik_tasks_dispatched"],
                Is.EqualTo(2));
    Assert.That(metrics["armonik_tasks_processing"],
                Is.EqualTo(1));

    // Per-partition: computed queued and individual components
    Assert.That(metrics["armonik_p1_tasks_queued"],
                Is.EqualTo(6));
    Assert.That(metrics["armonik_p1_tasks_submitted"],
                Is.EqualTo(3));
    Assert.That(metrics["armonik_p1_tasks_dispatched"],
                Is.EqualTo(2));
    Assert.That(metrics["armonik_p1_tasks_processing"],
                Is.EqualTo(1));
  }

  [Test]
  public async Task GetMetricsWithMultiplePartitionsShouldEmitAllMetricsPerPartition()
  {
    SetupPartitions("p1",
                    "p2");
    SetupTaskCounts(("p1", TaskStatus.Submitted, 5),
                    ("p2", TaskStatus.Processing, 3));
    var meter = CreateMeter();

    var metrics = ParseMetrics(await meter.GetMetricsAsync(CancellationToken.None)
                                          .ConfigureAwait(false));

    // Root aggregates across both partitions
    Assert.That(metrics["armonik_tasks_queued"],
                Is.EqualTo(8));
    Assert.That(metrics["armonik_tasks_submitted"],
                Is.EqualTo(5));
    Assert.That(metrics["armonik_tasks_processing"],
                Is.EqualTo(3));

    // Per-partition
    Assert.That(metrics["armonik_p1_tasks_queued"],
                Is.EqualTo(5));
    Assert.That(metrics["armonik_p2_tasks_queued"],
                Is.EqualTo(3));
    // p2 has no submitted tasks, but the metric is still emitted as 0
    Assert.That(metrics["armonik_p2_tasks_submitted"],
                Is.EqualTo(0));
  }

  [Test]
  public async Task GetMetricsWithNoTasksShouldAlwaysEmitAllMetricsAtZero()
  {
    // p1 exists but has no tasks in any status
    SetupPartitions("p1");
    SetupTaskCounts(); // no counts at all
    var meter = CreateMeter();

    var metrics = ParseMetrics(await meter.GetMetricsAsync(CancellationToken.None)
                                          .ConfigureAwait(false));

    // Root aggregates are 0
    Assert.That(metrics["armonik_tasks_queued"],
                Is.EqualTo(0));
    Assert.That(metrics["armonik_tasks_submitted"],
                Is.EqualTo(0));
    Assert.That(metrics["armonik_tasks_dispatched"],
                Is.EqualTo(0));
    Assert.That(metrics["armonik_tasks_processing"],
                Is.EqualTo(0));

    // Per-partition are also 0
    Assert.That(metrics["armonik_p1_tasks_queued"],
                Is.EqualTo(0));
    Assert.That(metrics["armonik_p1_tasks_submitted"],
                Is.EqualTo(0));
    Assert.That(metrics["armonik_p1_tasks_dispatched"],
                Is.EqualTo(0));
    Assert.That(metrics["armonik_p1_tasks_processing"],
                Is.EqualTo(0));
  }

  [Test]
  public async Task GetMetricsWithCompletedConfiguredShouldEmitItAlongsideQueued()
  {
    SetupPartitions("p1");
    SetupTaskCounts(("p1", TaskStatus.Completed, 7),
                    ("p1", TaskStatus.Submitted, 2));
    var meter = CreateMeter("Completed");

    var metrics = ParseMetrics(await meter.GetMetricsAsync(CancellationToken.None)
                                          .ConfigureAwait(false));

    Assert.That(metrics["armonik_p1_tasks_completed"],
                Is.EqualTo(7));
    Assert.That(metrics["armonik_p1_tasks_queued"],
                Is.EqualTo(2));
    Assert.That(metrics["armonik_tasks_queued"],
                Is.EqualTo(2));
  }

  [Test]
  public async Task GetMetricsWithInvalidConfigMetricShouldIgnoreIt()
  {
    SetupPartitions("p1");
    SetupTaskCounts(("p1", TaskStatus.Submitted, 3));
    var meter = CreateMeter("NotAValidStatus");

    var metrics = ParseMetrics(await meter.GetMetricsAsync(CancellationToken.None)
                                          .ConfigureAwait(false));

    // Only default metrics (queued + its components) should be present
    Assert.That(metrics,
                Does.Not.ContainKey("armonik_p1_tasks_notavalidstatus"));
    Assert.That(metrics,
                Contains.Key("armonik_p1_tasks_queued"));
  }

  [Test]
  public async Task GetMetricsWithExplicitQueuedConfigShouldNotDuplicateQueuedMetric()
  {
    SetupPartitions("p1");
    SetupTaskCounts(("p1", TaskStatus.Submitted, 5));
    var meter = CreateMeter("Queued");

    var output = await meter.GetMetricsAsync(CancellationToken.None)
                            .ConfigureAwait(false);

    // "armonik_p1_tasks_queued" value line must appear exactly once
    var count = output.Split('\n')
                      .Count(l => l.StartsWith("armonik_p1_tasks_queued "));
    Assert.That(count,
                Is.EqualTo(1));
  }

  [Test]
  public async Task GetMetricsShouldEmitTypeLineBeforeValueLine()
  {
    SetupPartitions("p1");
    SetupTaskCounts(("p1", TaskStatus.Submitted, 4));
    var meter = CreateMeter();

    var output = await meter.GetMetricsAsync(CancellationToken.None)
                            .ConfigureAwait(false);

    Assert.That(output,
                Contains.Substring("# TYPE armonik_p1_tasks_queued gauge"));
    Assert.That(output,
                Contains.Substring("armonik_p1_tasks_queued 4"));

    var typeIdx = output.IndexOf("# TYPE armonik_p1_tasks_queued gauge",
                                 StringComparison.Ordinal);
    var valueIdx = output.IndexOf("armonik_p1_tasks_queued 4",
                                  StringComparison.Ordinal);
    Assert.That(typeIdx,
                Is.LessThan(valueIdx));
  }

  [Test]
  public async Task GetMetricsTwiceWithinCacheWindowShouldNotQueryDbAgain()
  {
    // 1 partition × 3 default statuses = 3 ListTasksAsync calls per BuildMetricsAsync
    SetupPartitions("p1");
    SetupTaskCounts(("p1", TaskStatus.Submitted, 5));
    var meter = CreateMeter(cacheValidity: TimeSpan.FromMinutes(5));

    await meter.GetMetricsAsync(CancellationToken.None)
               .ConfigureAwait(false);
    await meter.GetMetricsAsync(CancellationToken.None)
               .ConfigureAwait(false);

    // DB was queried only once (cache hit on second call)
    taskTableMock_.Verify(tt => tt.ListTasksAsync(It.IsAny<Expression<Func<TaskData, bool>>>(),
                                                  It.IsAny<Expression<Func<TaskData, object?>>>(),
                                                  It.IsAny<Expression<Func<TaskData, ValueTuple>>>(),
                                                  It.IsAny<bool>(),
                                                  It.IsAny<int>(),
                                                  It.IsAny<int>(),
                                                  It.IsAny<CancellationToken>()),
                          Times.Exactly(3)); // 1 partition × 3 statuses
  }

  [Test]
  public async Task GetMetricsShouldProduceValidPrometheusTextFormat()
  {
    SetupPartitions("p1");
    SetupTaskCounts(("p1", TaskStatus.Submitted, 3),
                    ("p1", TaskStatus.Processing, 2));
    var meter = CreateMeter();

    var output = await meter.GetMetricsAsync(CancellationToken.None)
                            .ConfigureAwait(false);

    // Use prometheus-net (the authoritative reference implementation) to
    // serialise the same gauges and compare name→value pairs with our output.
    var registry = Prometheus.Metrics.NewCustomRegistry();
    var factory  = Prometheus.Metrics.WithCustomRegistry(registry);

    factory.CreateGauge("armonik_tasks_submitted",
                        "")
           .Set(3);
    factory.CreateGauge("armonik_tasks_dispatched",
                        "")
           .Set(0);
    factory.CreateGauge("armonik_tasks_processing",
                        "")
           .Set(2);
    factory.CreateGauge("armonik_tasks_queued",
                        "")
           .Set(5); // 3+2

    factory.CreateGauge("armonik_p1_tasks_submitted",
                        "")
           .Set(3);
    factory.CreateGauge("armonik_p1_tasks_dispatched",
                        "")
           .Set(0);
    factory.CreateGauge("armonik_p1_tasks_processing",
                        "")
           .Set(2);
    factory.CreateGauge("armonik_p1_tasks_queued",
                        "")
           .Set(5);

    using var ms = new MemoryStream();
    await registry.CollectAndExportAsTextAsync(ms,
                                               CancellationToken.None)
                  .ConfigureAwait(false);
    var reference = Encoding.UTF8.GetString(ms.ToArray());

    Assert.That(output,
                Is.EqualTo(reference));
  }

  [Test]
  public async Task GetMetricsAfterCacheExpiryWindowShouldQueryDbAgain()
  {
    SetupPartitions("p1");
    SetupTaskCounts(("p1", TaskStatus.Submitted, 5));
    var meter = CreateMeter(cacheValidity: TimeSpan.FromMilliseconds(50));

    await meter.GetMetricsAsync(CancellationToken.None)
               .ConfigureAwait(false);
    await Task.Delay(150)
              .ConfigureAwait(false);
    await meter.GetMetricsAsync(CancellationToken.None)
               .ConfigureAwait(false);

    // DB was queried twice (once before expiry, once after)
    taskTableMock_.Verify(tt => tt.ListTasksAsync(It.IsAny<Expression<Func<TaskData, bool>>>(),
                                                  It.IsAny<Expression<Func<TaskData, object?>>>(),
                                                  It.IsAny<Expression<Func<TaskData, ValueTuple>>>(),
                                                  It.IsAny<bool>(),
                                                  It.IsAny<int>(),
                                                  It.IsAny<int>(),
                                                  It.IsAny<CancellationToken>()),
                          Times.Exactly(6)); // 2 builds × 1 partition × 3 statuses
  }
}
