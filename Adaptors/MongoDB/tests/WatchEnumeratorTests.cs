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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Storage.Events;

using Microsoft.Extensions.DependencyInjection;

using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.Events;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.MongoDB.Tests;

/// <summary>
///   Checks that the change streams of the watchers release their connections and their server cursors
///   when the subscription is cancelled.
/// </summary>
[TestFixture]
public class WatchEnumeratorTests
{
  [SetUp]
  public async Task SetUp()
  {
    connectionFailures_ = 0;
    getMoreFailures_    = 0;
    provider_ = new MongoDatabaseProvider(true,
                                          clusterConfigurator: cb =>
                                                               {
                                                                 cb.Subscribe<ConnectionFailedEvent>(_ => Interlocked.Increment(ref connectionFailures_));
                                                                 cb.Subscribe<CommandFailedEvent>(e =>
                                                                                                  {
                                                                                                    if (e.CommandName == "getMore")
                                                                                                    {
                                                                                                      Interlocked.Increment(ref getMoreFailures_);
                                                                                                    }
                                                                                                  });
                                                               });

    var services = provider_.GetServiceProvider();
    taskWatcher_ = services.GetRequiredService<ITaskWatcher>();
    database_    = services.GetRequiredService<IMongoDatabase>();

    await services.GetRequiredService<ITaskTable>()
                  .Init(CancellationToken.None)
                  .ConfigureAwait(false);
    await taskWatcher_.Init(CancellationToken.None)
                      .ConfigureAwait(false);
  }

  [TearDown]
  public void TearDown()
    => provider_?.Dispose();

  private MongoDatabaseProvider? provider_;
  private ITaskWatcher?          taskWatcher_;
  private IMongoDatabase?        database_;
  private int                    connectionFailures_;
  private int                    getMoreFailures_;

  /// <summary>
  ///   Watch new tasks and task status updates until <paramref name="duration" /> has elapsed
  /// </summary>
  private async Task Watch(TimeSpan duration)
  {
    using var cts = new CancellationTokenSource(duration);

    var newTasks = await taskWatcher_!.GetNewTasks(data => data.SessionId == "SessionId",
                                                   cts.Token)
                                      .ConfigureAwait(false);
    var statusUpdates = await taskWatcher_.GetTaskStatusUpdates(data => data.SessionId == "SessionId",
                                                                cts.Token)
                                          .ConfigureAwait(false);

    await Task.WhenAll(newTasks.ToListAsync(cts.Token)
                               .AsTask(),
                       statusUpdates.ToListAsync(cts.Token)
                                    .AsTask())
              .ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
  }

  /// <summary>
  ///   Count the cursors of the test database still open on the server
  /// </summary>
  private async Task<int> CountOpenCursors()
  {
    var admin = database_!.Client.GetDatabase("admin");
    var pipeline = new[]
                   {
                     new BsonDocument("$currentOp",
                                      new BsonDocument
                                      {
                                        {
                                          "idleCursors", true
                                        },
                                        {
                                          "allUsers", true
                                        },
                                      }),
                     new BsonDocument("$match",
                                      new BsonDocument("ns",
                                                       new BsonRegularExpression($"^{database_.DatabaseNamespace.DatabaseName}\\."))),
                   };

    var ops = await admin.AggregateAsync<BsonDocument>(pipeline)
                         .ConfigureAwait(false);
    var cursors = await ops.ToListAsync()
                           .ConfigureAwait(false);
    return cursors.Count(op => op.Contains("cursor"));
  }

  [Test]
  public async Task CancelledWatchShouldReleaseConnectionsAndCursors()
  {
    for (var i = 0; i < 10; i++)
    {
      await Watch(TimeSpan.FromMilliseconds(200))
        .ConfigureAwait(false);
    }

    Assert.Multiple(async () =>
                    {
                      // A cancelled getMore makes the driver close its connection
                      Assert.That(connectionFailures_,
                                  Is.Zero);
                      Assert.That(getMoreFailures_,
                                  Is.Zero);
                      Assert.That(await CountOpenCursors()
                                    .ConfigureAwait(false),
                                  Is.Zero);
                    });
  }

  [Test]
  public async Task CancelledWatchShouldEndAfterMaxAwaitTime()
  {
    var start = DateTime.UtcNow;
    await Watch(TimeSpan.FromMilliseconds(200))
      .ConfigureAwait(false);

    Assert.That(DateTime.UtcNow - start,
                Is.LessThan(TimeSpan.FromMilliseconds(200) + ChangeStreamUpdate.MaxAwaitTime + TimeSpan.FromSeconds(2)));
  }
}
