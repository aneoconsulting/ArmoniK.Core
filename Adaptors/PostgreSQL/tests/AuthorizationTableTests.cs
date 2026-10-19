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

using ArmoniK.Core.Adapters.PostgreSQL.Common;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.TestBase;

using Microsoft.Extensions.DependencyInjection;

using Npgsql;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.PostgreSQL.Tests;

public class AuthorizationTableTests : AuthorizationTableTestBase
{
  // Key of the advisory lock the table takes
  private const long LockKey = 0x41726d6f6e694b;

  private static readonly DateTime Date = new(2026,
                                              1,
                                              1,
                                              0,
                                              0,
                                              0,
                                              DateTimeKind.Utc);

  private PostgresDatabaseProvider? tableProvider_;

  public override void TearDown()
  {
    tableProvider_?.Dispose();
    RunTests = false;
  }

  public override void GetAuthorizationTableInstance()
  {
    tableProvider_ = new PostgresDatabaseProvider();
    AuthorizationTable = tableProvider_.GetServiceProvider()
                                       .GetRequiredService<IAuthorizationTable>();
    RunTests = true;
  }

  private static GrantData Grant(string id)
    => new(id,
           "role:team",
           "session-view",
           null,
           AuthorizationOrigin.Config,
           Date);

  private ValueTask<NpgsqlConnection> OpenAsync(PostgresDatabaseProvider? provider = null)
    => (provider ?? tableProvider_)!.GetServiceProvider()
                                    .GetRequiredService<NpgsqlConnectionProvider>()
                                    .GetConnectionAsync(CancellationToken.None);

  private static async Task<object?> ExecuteAsync(NpgsqlConnection connection,
                                                  string           sql)
  {
    await using var cmd = connection.CreateCommand();
    cmd.CommandText = sql;
    return await cmd.ExecuteScalarAsync()
                    .ConfigureAwait(false);
  }

  // Whether another session can take the lock, which it releases at once
  private async Task<bool> IsFreeAsync()
  {
    await using var connection = await OpenAsync()
                                   .ConfigureAwait(false);
    var free = (bool)(await ExecuteAsync(connection,
                                         $"SELECT pg_try_advisory_lock({LockKey})")
                        .ConfigureAwait(false))!;
    if (free)
    {
      await ExecuteAsync(connection,
                         $"SELECT pg_advisory_unlock({LockKey})")
        .ConfigureAwait(false);
    }

    return free;
  }

  [Test]
  public async Task ALockIsReleasedWhenDisposedCommittedOrNot()
  {
    foreach (var commit in new[]
                           {
                             false,
                             true,
                             false,
                           })
    {
      var held = await AuthorizationTable!.LockAsync()
                                          .ConfigureAwait(false);
      Assert.That(await IsFreeAsync()
                    .ConfigureAwait(false),
                  Is.False);
      if (commit)
      {
        await held.CommitAsync()
                  .ConfigureAwait(false);
      }

      await held.DisposeAsync()
                .ConfigureAwait(false);
      Assert.That(await IsFreeAsync()
                    .ConfigureAwait(false),
                  Is.True);
    }
  }

  [Test]
  public async Task ALockIsReleasedAsSoonAsAStatementUnderItFails()
  {
    await using var held = await AuthorizationTable!.LockAsync()
                                                    .ConfigureAwait(false);
    var table = held.Table ?? AuthorizationTable;

    // A statement under the lock stalls on a table another session locked, until it is cancelled
    await using (var blocker = await OpenAsync()
                                 .ConfigureAwait(false))
    {
      await using var transaction = await blocker.BeginTransactionAsync()
                                                 .ConfigureAwait(false);
      await ExecuteAsync(blocker,
                         "LOCK TABLE grants IN ACCESS EXCLUSIVE MODE")
        .ConfigureAwait(false);
      using var stalled = new CancellationTokenSource(TimeSpan.FromSeconds(1));
      Assert.That(() => table.CreateGrantAsync(Grant("stalled"),
                                               stalled.Token),
                  Throws.InstanceOf<OperationCanceledException>());
    }

    // Not kept by a connection until it is disposed: another replica can reconcile at once, and this one cannot keep
    // anything it wrote
    Assert.That(await IsFreeAsync()
                  .ConfigureAwait(false),
                Is.True);
    Assert.That(() => held.CommitAsync(),
                Throws.Exception);
  }

  [Test]
  public async Task WritesUnderALostLockAreNeverServed()
  {
    // Another replica, with its own connections
    using var replica = new PostgresDatabaseProvider();
    var other = replica.GetServiceProvider()
                       .GetRequiredService<IAuthorizationTable>();
    await other.Init(CancellationToken.None)
               .ConfigureAwait(false);

    await using var held = await AuthorizationTable!.LockAsync()
                                                    .ConfigureAwait(false);
    var table = held.Table ?? AuthorizationTable;
    var generation = await table.BeginChangeAsync()
                                .ConfigureAwait(false);
    await table.CreateGrantAsync(Grant("before-loss"))
               .ConfigureAwait(false);

    // The server ends the session holding the lock, as on a failover, and releases the lock with it
    await using (var connection = await OpenAsync(replica)
                                    .ConfigureAwait(false))
    {
      await ExecuteAsync(connection,
                         "SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype = 'advisory' AND granted AND pid <> pg_backend_pid()")
        .ConfigureAwait(false);
    }

    // The other replica takes the lock over and completes its change
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
    await using (var taken = await other.LockAsync(timeout.Token)
                                        .ConfigureAwait(false))
    {
      var store = taken.Table ?? other;
      var begun = await store.BeginChangeAsync()
                             .ConfigureAwait(false);
      await store.CreateGrantAsync(Grant("other"))
                 .ConfigureAwait(false);
      await store.EndChangeAsync(begun)
                 .ConfigureAwait(false);
      await taken.CommitAsync()
                 .ConfigureAwait(false);
    }

    // What the first replica writes afterwards is refused, and nothing it wrote under the lock is kept
    Assert.That(() => table.CreateGrantAsync(Grant("after-loss")),
                Throws.Exception);
    Assert.That(() => table.EndChangeAsync(generation),
                Throws.Exception);
    Assert.That(() => held.CommitAsync(),
                Throws.Exception);

    Assert.That((await other.GetGenerationAsync()
                            .ConfigureAwait(false)).Stable,
                Is.True);
    Assert.That(await other.ListGrantsAsync()
                           .Select(grant => grant.GrantId)
                           .ToListAsync()
                           .ConfigureAwait(false),
                Is.EquivalentTo(new[]
                                {
                                  "other",
                                }));
  }
}
