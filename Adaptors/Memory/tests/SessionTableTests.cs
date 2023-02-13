// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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

// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System.Collections.Concurrent;

using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests;

using Microsoft.Extensions.DependencyInjection;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.Memory.Tests;

[TestFixture]
public class SessionTableTests : SessionTableTestBase
{
  public override void GetSessionTableInstance()
  {
    var services = new ServiceCollection();

    services.AddTransient<ISessionTable, SessionTable>();
    services.AddTransient<ConcurrentDictionary<string, SessionData>>();
    services.AddTransient<ConcurrentDictionary<string, ConcurrentDictionary<string, string>>>();
    services.AddLogging();

    var provider = services.BuildServiceProvider(true);
    var scope    = provider.CreateScope();

    SessionTable = scope.ServiceProvider.GetRequiredService<ISessionTable>();
    RunTests     = true;
  }
}
