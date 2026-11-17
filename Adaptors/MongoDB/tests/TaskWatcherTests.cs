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

using System.Threading.Tasks;

using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Storage.Events;
using ArmoniK.Core.Common.Testing;

using Microsoft.Extensions.DependencyInjection;

using MongoDB.Driver;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.MongoDB.Tests;

[TestFixture]
public class TaskWatcherTests : TaskWatcherTestBase
{
  public override void TearDown()
  {
    tableProvider_?.Dispose();
    RunTests = false;
  }

  private IMongoDatabase?        database_;
  private MongoDatabaseProvider? tableProvider_;

  public override void GetInstance()
  {
    tableProvider_ = new MongoDatabaseProvider(true);
    var provider = tableProvider_.GetServiceProvider();

    TaskTable   = provider.GetRequiredService<ITaskTable>();
    TaskWatcher = provider.GetRequiredService<ITaskWatcher>();
    database_   = provider.GetRequiredService<IMongoDatabase>();
    RunTests    = true;
  }

  /// <inheritdoc />
  protected override async Task<int?> CountServerWatchResources()
    => await MongoCursors.CountOpen(database_!)
                         .ConfigureAwait(false);
}
