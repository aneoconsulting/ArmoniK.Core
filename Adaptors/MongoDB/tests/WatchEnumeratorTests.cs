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
  ///   Watch new tasks and task status updates until <paramref name="duration" /> has elapsed after the change
  ///   streams are opened
  /// </summary>
  /// <remarks>
  ///   The timer starts once the change streams are open: opening them can take longer than
  ///   <paramref name="duration" /> on slow hosts, and a cancelled aggregate both throws and breaks its connection,
  ///   which is not the behavior under test.
  /// </remarks>
  private async Task Watch(TimeSpan duration)
  {
    using var cts = new CancellationTokenSource();

    var newTasks = await taskWatcher_!.GetNewTasks(data => data.SessionId == "SessionId",
                                                   cts.Token)
                                      .ConfigureAwait(false);
    var statusUpdates = await taskWatcher_.GetTaskStatusUpdates(data => data.SessionId == "SessionId",
                                                                cts.Token)
                                          .ConfigureAwait(false);

    cts.CancelAfter(duration);

    await Task.WhenAll(newTasks.ToListAsync(cts.Token)
                               .AsTask(),
                       statusUpdates.ToListAsync(cts.Token)
                                    .AsTask())
              .ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
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
                      Assert.That(await MongoCursors.CountOpen(database_!)
                                                    .ConfigureAwait(false),
                                  Is.Zero);
                    });
  }

  [Test]
  public void AbortedGetMoreShouldReportTheCallerCancellation()
  {
    using var cts = new CancellationTokenSource();
    var enumerator = new WatchEnumerator<int, int>(new NeverEndingCursor(),
                                                   i => i,
                                                   cts.Token);

    var moveNext = enumerator.MoveNextAsync()
                             .AsTask();
    cts.Cancel();

    // The getMore never returns by itself: it is interrupted once the grace period has elapsed
    var exception = Assert.CatchAsync<OperationCanceledException>(() => moveNext);
    Assert.That(exception!.CancellationToken,
                Is.EqualTo(cts.Token));
  }

  /// <summary>
  ///   Cursor whose getMore never returns, as when the server does not answer
  /// </summary>
  private sealed class NeverEndingCursor : IChangeStreamCursor<int>
  {
    public IEnumerable<int> Current
      => throw new InvalidOperationException();

    public bool MoveNext(CancellationToken cancellationToken = default)
      => throw new NotSupportedException();

    public async Task<bool> MoveNextAsync(CancellationToken cancellationToken = default)
    {
      await Task.Delay(Timeout.Infinite,
                       cancellationToken)
                .ConfigureAwait(false);
      return true;
    }

    public BsonDocument? GetResumeToken()
      => null;

    public void Dispose()
    {
    }
  }
}
