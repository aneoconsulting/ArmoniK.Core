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

using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Injection.Options.Database;
using ArmoniK.Core.Utils;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace ArmoniK.Core.Adapters.Memory.Tests;

/// <summary>
///   Builds a service provider with the in-memory tables and watchers, configured from the environment variables
/// </summary>
internal static class MemoryTablesProvider
{
  public static ServiceProvider Build()
  {
    var configuration = new ConfigurationManager();
    configuration.AddEnvironmentVariables();

    var services = new ServiceCollection();
    services.AddLogging()
            .AddInitializedOption<InitServices>(configuration,
                                                InitServices.SettingSection)
            .AddSingleton<InitDatabase>()
            .AddMemoryTables();

    return services.BuildServiceProvider(true);
  }
}
