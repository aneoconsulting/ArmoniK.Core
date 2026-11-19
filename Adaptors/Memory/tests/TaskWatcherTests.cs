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

using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Storage.Events;
using ArmoniK.Core.Common.Testing;

using Microsoft.Extensions.DependencyInjection;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.Memory.Tests;

[TestFixture]
public class TaskWatcherTests : TaskWatcherTestBase
{
  private ServiceProvider? provider_;

  public override void TearDown()
  {
    provider_?.Dispose();
    base.TearDown();
  }

  public override void GetInstance()
  {
    provider_   = MemoryTablesProvider.Build();
    TaskTable   = provider_.GetRequiredService<ITaskTable>();
    TaskWatcher = provider_.GetRequiredService<ITaskWatcher>();
    RunTests    = true;
  }
}
