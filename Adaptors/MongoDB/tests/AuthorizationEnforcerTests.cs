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
using ArmoniK.Core.Common.Tests.TestBase;

using Microsoft.Extensions.DependencyInjection;

namespace ArmoniK.Core.Adapters.MongoDB.Tests;

public class AuthorizationEnforcerTests : AuthorizationEnforcerTestBase
{
  private MongoDatabaseProvider? tableProvider_;

  public override void TearDown()
  {
    tableProvider_?.Dispose();
    base.TearDown();
  }

  public override void GetTablesInstance()
  {
    tableProvider_ = new MongoDatabaseProvider();
    var provider = tableProvider_.GetServiceProvider();

    SessionTable = provider.GetRequiredService<ISessionTable>();
    TaskTable    = provider.GetRequiredService<ITaskTable>();
    ResultTable  = provider.GetRequiredService<IResultTable>();
    RunTests     = true;
  }
}
