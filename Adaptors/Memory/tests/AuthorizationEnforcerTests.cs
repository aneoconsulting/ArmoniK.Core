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

using System.Collections.Concurrent;

using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.TestBase;
using ArmoniK.Core.Utils.Uuid;

using Microsoft.Extensions.DependencyInjection;

namespace ArmoniK.Core.Adapters.Memory.Tests;

public class AuthorizationEnforcerTests : AuthorizationEnforcerTestBase
{
  public override void GetTablesInstance()
  {
    var services = new ServiceCollection();

    services.AddUuidGeneratorDefault();
    services.AddSingleton<ISessionTable, SessionTable>();
    services.AddSingleton<ITaskTable, TaskTable>();
    services.AddSingleton<IResultTable, ResultTable>();
    services.AddSingleton<ConcurrentDictionary<string, SessionData>>();
    services.AddSingleton<ConcurrentDictionary<string, TaskData>>();
    services.AddSingleton<ConcurrentDictionary<string, ConcurrentQueue<string>>>();
    services.AddLogging();

    var provider = services.BuildServiceProvider(true);

    SessionTable = provider.GetRequiredService<ISessionTable>();
    TaskTable    = provider.GetRequiredService<ITaskTable>();
    ResultTable  = provider.GetRequiredService<IResultTable>();
    RunTests     = true;
  }
}
