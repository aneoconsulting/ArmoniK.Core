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

using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.PostgreSQL.Common;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.TestBase;

using Microsoft.Extensions.DependencyInjection;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.PostgreSQL.Tests;

public class AuthorizationTableTests : AuthorizationTableTestBase
{
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

  [Test]
  public async Task ALockWhoseConnectionIsLostIsReportedLost()
  {
    await using var held = await AuthorizationTable!.LockAsync()
                                                    .ConfigureAwait(false);

    // The server ends the session holding the lock, as on a failover, and releases the lock with it
    await using (var connection = await tableProvider_!.GetServiceProvider()
                                                       .GetRequiredService<NpgsqlConnectionProvider>()
                                                       .GetConnectionAsync(CancellationToken.None)
                                                       .ConfigureAwait(false))
    {
      await using var cmd = connection.CreateCommand();
      cmd.CommandText = "SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype = 'advisory' AND granted AND pid <> pg_backend_pid()";
      await cmd.ExecuteNonQueryAsync()
               .ConfigureAwait(false);
    }

    Assert.That(() => held.Lost.IsCancellationRequested,
                Is.True.After(15000,
                              100));
  }
}
