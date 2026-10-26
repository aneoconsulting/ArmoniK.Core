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

using ArmoniK.Api.gRPC.V1.Events;
using ArmoniK.Core.Adapters.PostgreSQL.Common;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Storage.Events;
using ArmoniK.Core.Common.Tests.TestBase;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

using NUnit.Framework;

using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

namespace ArmoniK.Core.Adapters.PostgreSQL.Tests;

[TestFixture]
public class TaskWatcherTests : TaskWatcherTestBase
{
  public override void TearDown()
  {
    tableProvider_?.Dispose();
    RunTests = false;
  }

  private NpgsqlConnectionProvider? connectionProvider_;
  private IResultTable?             resultTable_;
  private IResultWatcher?           resultWatcher_;
  private PostgresDatabaseProvider? tableProvider_;

  public override void GetInstance()
  {
    tableProvider_ = new PostgresDatabaseProvider();
    var provider = tableProvider_.GetServiceProvider();

    TaskTable           = provider.GetRequiredService<ITaskTable>();
    TaskWatcher         = provider.GetRequiredService<ITaskWatcher>();
    resultTable_        = provider.GetRequiredService<IResultTable>();
    resultWatcher_      = provider.GetRequiredService<IResultWatcher>();
    connectionProvider_ = provider.GetRequiredService<NpgsqlConnectionProvider>();
    RunTests            = true;
  }

  [Test]
  public void WatchAfterDisposeShouldThrow()
  {
    ((IDisposable)TaskWatcher!).Dispose();

    Assert.That(() => TaskWatcher.GetNewTasks(data => data.SessionId == "SessionId"),
                Throws.InstanceOf<ObjectDisposedException>());
  }

  [Test]
  public async Task WatchShouldRecoverAfterReplicationConnectionIsTerminated()
  {
    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

    var firstStream = await TaskWatcher!.GetNewTasks(data => data.SessionId == "SessionId",
                                                     cts.Token)
                                        .ConfigureAwait(false);
    Task firstWatch = firstStream.ToListAsync(cts.Token)
                                 .AsTask();

    // Kill the walsender of the replication slot, as a network failure or a server restart would.
    await using (var command = connectionProvider_!.DataSource.CreateCommand("SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots " +
                                                                             "WHERE slot_name LIKE 'armonik_%' AND active_pid IS NOT NULL"))
    {
      await command.ExecuteNonQueryAsync(cts.Token)
                   .ConfigureAwait(false);
    }

    // The broken stream must end, with or without an exception, instead of hanging.
    await firstWatch.WaitAsync(cts.Token)
                    .ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    Assert.That(cts.IsCancellationRequested,
                Is.False);

    // A new subscription must start a new replication connection and receive events.
    var secondStream = await TaskWatcher.GetNewTasks(data => data.SessionId == "SessionId",
                                                     cts.Token)
                                        .ConfigureAwait(false);

    var template = await TaskTable!.ReadTaskAsync("TaskSubmittedId",
                                                  cts.Token)
                                   .ConfigureAwait(false);
    await TaskTable!.CreateTasks(new[]
                                 {
                                   template with
                                   {
                                     TaskId = "TaskAfterReconnection",
                                     Status = TaskStatus.Creating,
                                   },
                                 },
                                 cts.Token)
                    .ConfigureAwait(false);

    var first = await secondStream.FirstAsync(cts.Token)
                                  .ConfigureAwait(false);
    Assert.That(first.TaskId,
                Is.EqualTo("TaskAfterReconnection"));
  }

  [Test]
  public async Task GetEventsShouldFailWhenReplicationConnectionIsTerminated()
  {
    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

    var wtg = new WatchToGrpc(TaskTable!,
                              TaskWatcher!,
                              resultTable_!,
                              resultWatcher_!,
                              NullLogger.Instance);

    var events = wtg.GetEvents("SessionId",
                               new List<EventsEnum>
                               {
                                 EventsEnum.TaskStatusUpdate,
                               },
                               null,
                               null,
                               cts.Token)
                    .ToListAsync(cts.Token)
                    .AsTask();

    // Wait for the subscription to open its replication slot, then kill its walsender.
    var terminated = 0;
    while (terminated == 0)
    {
      await Task.Delay(100,
                       cts.Token)
                .ConfigureAwait(false);
      await using var command = connectionProvider_!.DataSource.CreateCommand("SELECT count(pg_terminate_backend(active_pid)) FROM pg_replication_slots " +
                                                                              "WHERE slot_name LIKE 'armonik_%' AND active_pid IS NOT NULL");
      terminated = Convert.ToInt32(await command.ExecuteScalarAsync(cts.Token)
                                                .ConfigureAwait(false));
    }

    // The client must receive an error to subscribe again, instead of waiting on a silent stream.
    Assert.That(() => events,
                Throws.InstanceOf<EventStreamInterruptedException>());
    Assert.That(cts.IsCancellationRequested,
                Is.False);
  }
}
