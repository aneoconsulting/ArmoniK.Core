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

using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Injection.Options.Database;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Storage.Events;
using ArmoniK.Core.Utils.Uuid;

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Memory;

/// <summary>
///   Registration of the in-memory tables
/// </summary>
public static class ServiceCollectionExt
{
  /// <summary>
  ///   Value of <c>Components:TableStorage</c> that selects the in-memory tables
  /// </summary>
  public const string TableStorage = "ArmoniK.Adapters.Memory.TableStorage";

  /// <summary>
  ///   Register the in-memory tables when <c>Components:TableStorage</c> is <see cref="TableStorage" />
  /// </summary>
  /// <param name="services">Collection of services to fill</param>
  /// <param name="configuration">Configuration of the application</param>
  /// <param name="logger">Logger used during the registration</param>
  /// <returns>
  ///   The collection of services
  /// </returns>
  [PublicAPI]
  public static IServiceCollection AddMemoryComponents(this IServiceCollection services,
                                                       ConfigurationManager    configuration,
                                                       ILogger                 logger)
  {
    var components = configuration.GetSection(Components.SettingSection);

    if (components["TableStorage"] == TableStorage)
    {
      logger.LogInformation("Configure in-memory tables");
      services.AddMemoryTables();
    }

    return services;
  }

  /// <summary>
  ///   Register the in-memory tables and their watchers as singletons
  /// </summary>
  /// <remarks>
  ///   Each table is registered once, and the interface resolves to the same instance as the concrete type: all the
  ///   consumers of a table, including the watchers that subscribe to its change events, share the same data.
  ///   The partition and authentication tables are filled from <see cref="InitDatabase" /> when it is registered.
  /// </remarks>
  /// <param name="services">Collection of services to fill</param>
  /// <returns>
  ///   The collection of services
  /// </returns>
  [PublicAPI]
  public static IServiceCollection AddMemoryTables(this IServiceCollection services)
    => services.AddUuidGeneratorDefault()
               .AddSingleton<TaskTable>()
               .AddSingleton<ITaskTable>(sp => sp.GetRequiredService<TaskTable>())
               .AddSingleton<SessionTable>()
               .AddSingleton<ISessionTable>(sp => sp.GetRequiredService<SessionTable>())
               .AddSingleton<ResultTable>()
               .AddSingleton<IResultTable>(sp => sp.GetRequiredService<ResultTable>())
               .AddSingleton<IPartitionTable, PartitionTable>()
               .AddSingleton<IAuthenticationTable, AuthenticationTable>()
               .AddSingleton<ITaskWatcher, TaskWatcher>()
               .AddSingleton<IResultWatcher, ResultWatcher>();
}
