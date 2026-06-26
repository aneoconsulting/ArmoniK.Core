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
  public Task ListTaskWithInjectionSortKeyShouldThrow()
  {
    Assert.ThrowsAsync<ArgumentException>(() => TaskTable!.ListTasksAsync(data => data.SessionId == "SessionId",
                                                                           data => data.Options.Options["key' || (SELECT pg_sleep(5))::text || 'x"],
                                                                           data => data,
                                                                           false,
                                                                           0,
                                                                           20,
                                                                           CancellationToken.None));
    return Task.CompletedTask;
  }

  [Test]
  public Task ListTaskWithSingleQuoteSortKeyShouldThrow()
  {
    Assert.ThrowsAsync<ArgumentException>(() => TaskTable!.ListTasksAsync(data => data.SessionId == "SessionId",
                                                                           data => data.Options.Options["key'value"],
                                                                           data => data,
                                                                           false,
                                                                           0,
                                                                           20,
                                                                           CancellationToken.None));
    return Task.CompletedTask;
  }
}
