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

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.DependencyInjection;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.PostgreSQL.Tests;

/// <summary>
///   The tables trace their calls, and the spans of the Npgsql commands are nested in them.
/// </summary>
[TestFixture]
[NonParallelizable]
public class TracingTests
{
  [SetUp]
  public void SetUp()
  {
    tableProvider_ = new PostgresDatabaseProvider();
    listener_ = new ActivityListener
                {
                  ShouldListenTo  = source => source.Name is PostgresDatabaseProvider.ActivitySourceName or "Npgsql",
                  Sample          = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                  ActivityStopped = activity => activities_.Add(activity),
                };
    ActivitySource.AddActivityListener(listener_);
  }

  [TearDown]
  public void TearDown()
  {
    listener_?.Dispose();
    tableProvider_?.Dispose();
    activities_.Clear();
  }

  private readonly ConcurrentBag<Activity>   activities_ = new();
  private          ActivityListener?         listener_;
  private          PostgresDatabaseProvider? tableProvider_;

  [Test]
  public async Task ReadPartitionShouldBeTracedWithItsCommands()
  {
    var partitionTable = tableProvider_!.GetServiceProvider()
                                        .GetRequiredService<IPartitionTable>();
    await partitionTable.Init(CancellationToken.None)
                        .ConfigureAwait(false);
    await partitionTable.CreatePartitionsAsync(new[]
                                               {
                                                 new PartitionData("TracedPartition",
                                                                   new List<string>(),
                                                                   1,
                                                                   2,
                                                                   50,
                                                                   1,
                                                                   null),
                                               })
                        .ConfigureAwait(false);
    activities_.Clear();

    await partitionTable.ReadPartitionAsync("TracedPartition")
                        .ConfigureAwait(false);

    var read = activities_.Single(activity => activity.Source.Name == PostgresDatabaseProvider.ActivitySourceName);
    Assert.That(read.OperationName,
                Is.EqualTo(nameof(IPartitionTable.ReadPartitionAsync)));
    Assert.That(read.GetTagItem("ReadPartitionId"),
                Is.EqualTo("TracedPartition"));
    Assert.That(activities_.Where(activity => activity.Source.Name == "Npgsql")
                           .Select(activity => activity.ParentSpanId),
                Has.Some.EqualTo(read.SpanId));
  }
}
