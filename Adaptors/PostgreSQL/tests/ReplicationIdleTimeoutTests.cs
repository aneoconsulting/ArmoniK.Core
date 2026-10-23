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

using ArmoniK.Core.Adapters.PostgreSQL.Common;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Storage.Events;

using Microsoft.Extensions.DependencyInjection;

using NUnit.Framework;

using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

namespace ArmoniK.Core.Adapters.PostgreSQL.Tests;

/// <summary>
///   Tests for <see cref="Options.PostgreSQL.ReplicationIdleTimeout" />: the WAL replication connection is kept
///   open for a while after its last subscriber left.
/// </summary>
[TestFixture]
public class ReplicationIdleTimeoutTests
{
  private NpgsqlConnectionProvider? connectionProvider_;
  private ITaskTable?               taskTable_;
  private ITaskWatcher?             taskWatcher_;

  // Returns the provider of the test services, to be disposed by the test.
  private async Task<PostgresDatabaseProvider> Setup(TimeSpan idleTimeout)
  {
    var tableProvider = new PostgresDatabaseProvider(new Dictionary<string, string?>
                                                     {
                                                       {
                                                         $"{Options.PostgreSQL.SettingSection}:{nameof(Options.PostgreSQL.ReplicationIdleTimeout)}",
                                                         idleTimeout.ToString()
                                                       },
                                                     });
    var provider = tableProvider.GetServiceProvider();
    connectionProvider_ = provider.GetRequiredService<NpgsqlConnectionProvider>();
    taskTable_          = provider.GetRequiredService<ITaskTable>();
    taskWatcher_        = provider.GetRequiredService<ITaskWatcher>();

    await taskTable_.Init(CancellationToken.None)
                    .ConfigureAwait(false);
    return tableProvider;
  }

  private async Task<List<string>> GetReplicationSlots(CancellationToken cancellationToken)
  {
    await using var command = connectionProvider_!.DataSource.CreateCommand("SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'armonik_%'");
    await using var reader = await command.ExecuteReaderAsync(cancellationToken)
                                          .ConfigureAwait(false);
    var slots = new List<string>();
    while (await reader.ReadAsync(cancellationToken)
                       .ConfigureAwait(false))
    {
      slots.Add(reader.GetString(0));
    }

    return slots;
  }

  // Subscribes to new tasks, then unsubscribes. Returns the replication slots that existed during the subscription.
  private async Task<List<string>> SubscribeAndLeave(CancellationToken cancellationToken)
  {
    using var subscriptionCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    var stream = await taskWatcher_!.GetNewTasks(_ => true,
                                                 subscriptionCts.Token)
                                    .ConfigureAwait(false);
    var watch = Task.Run(async () =>
                         {
                           await foreach (var _ in stream.WithCancellation(subscriptionCts.Token)
                                                         .ConfigureAwait(false))
                           {
                           }
                         },
                         CancellationToken.None);

    var slots = await GetReplicationSlots(cancellationToken)
                  .ConfigureAwait(false);

    await subscriptionCts.CancelAsync()
                         .ConfigureAwait(false);
    Assert.That(() => watch,
                Throws.InstanceOf<OperationCanceledException>());

    return slots;
  }

  [Test]
  public async Task SubscriptionWithinIdleTimeoutShouldReuseReplicationSlot()
  {
    using var tableProvider = await Setup(TimeSpan.FromMinutes(5))
                                .ConfigureAwait(false);
    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

    var slotsDuringFirstSubscription = await SubscribeAndLeave(cts.Token)
                                         .ConfigureAwait(false);

    var stream = await taskWatcher_!.GetNewTasks(_ => true,
                                                 cts.Token)
                                    .ConfigureAwait(false);

    // No new slot: the second subscription uses the replication connection of the first one.
    Assert.That(await GetReplicationSlots(cts.Token)
                  .ConfigureAwait(false),
                Is.Not.Empty.And.SubsetOf(slotsDuringFirstSubscription));

    await taskTable_!.CreateTasks(new[]
                                  {
                                    new TaskData("SessionId",
                                                 "TaskAfterIdle",
                                                 "",
                                                 "",
                                                 "PayloadId",
                                                 "CreatedBy",
                                                 new List<string>(),
                                                 new List<string>(),
                                                 new List<string>(),
                                                 new List<string>(),
                                                 TaskStatus.Creating,
                                                 new TaskOptions(new Dictionary<string, string>(),
                                                                 TimeSpan.FromSeconds(1),
                                                                 1,
                                                                 1,
                                                                 "part1",
                                                                 "",
                                                                 "",
                                                                 "",
                                                                 "",
                                                                 ""),
                                                 new Output(OutputStatus.Error,
                                                            "")),
                                  },
                                  cts.Token)
                     .ConfigureAwait(false);

    // The reused connection still delivers events.
    await using var enumerator = stream.GetAsyncEnumerator(cts.Token);
    Assert.That(await enumerator.MoveNextAsync()
                                .ConfigureAwait(false),
                Is.True);
    Assert.That(enumerator.Current.TaskId,
                Is.EqualTo("TaskAfterIdle"));
  }

  [Test]
  public async Task IdleReplicationConnectionShouldCloseAfterTimeout()
  {
    using var tableProvider = await Setup(TimeSpan.FromMilliseconds(200))
                                .ConfigureAwait(false);
    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

    var slotsDuringSubscription = await SubscribeAndLeave(cts.Token)
                                    .ConfigureAwait(false);
    Assert.That(slotsDuringSubscription,
                Is.Not.Empty);

    // Poll instead of waiting a fixed delay: the slot is dropped asynchronously once the connection is closed.
    while ((await GetReplicationSlots(cts.Token)
              .ConfigureAwait(false)).Intersect(slotsDuringSubscription)
                                     .Any())
    {
      await Task.Delay(TimeSpan.FromMilliseconds(100),
                       cts.Token)
                .ConfigureAwait(false);
    }
  }
}
