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

using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.TestBase;

using Microsoft.Extensions.DependencyInjection;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.PostgresSQL.Tests;

[TestFixture]
public class TaskTableTests : TaskTableTestBase
{
  private PostgresDatabaseProvider? tableProvider_;

  public override void TearDown()
  {
    tableProvider_?.Dispose();
    RunTests = false;
  }

  public override void GetTaskTableInstance()
  {
    tableProvider_ = new PostgresDatabaseProvider();
    var provider = tableProvider_.GetServiceProvider();

    TaskTable = provider.GetRequiredService<ITaskTable>();
    RunTests  = true;
  }

  [Test]
  public async Task ReadTaskAsyncShouldNotReturnRemainingDataDependencies()
  {
    if (RunTests)
    {
      // Postgres-specific: RemainingDataDependencies lives in a separate join table, so
      // ReadTaskAsync only loads it when the selector explicitly reads it, avoiding the join
      // query otherwise (no production caller needs it through the identity selector, e.g.
      // Pollster's TaskHandler, or through TaskDataMask projections). Memory/MongoDB always
      // have it already in the stored object regardless of selector shape, so this contract
      // does not hold there.
      var result = await TaskTable!.ReadTaskAsync("TaskCreatingId",
                                                  CancellationToken.None)
                                   .ConfigureAwait(false);

      Assert.That(result.RemainingDataDependencies,
                  Is.Empty);
    }
  }
}
