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
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using ArmoniK.Core.Adapters.PostgresSQL.Common;
using ArmoniK.Core.Adapters.PostgresSQL.Options;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Storage.Events;
using ArmoniK.Core.Utils;

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

using ConfigurationExt = ArmoniK.Core.Utils.ConfigurationExt;

namespace ArmoniK.Core.Adapters.PostgresSQL;

/// <summary>
///   Provides extension methods for configuring PostgreSQL components in the service collection.
/// </summary>
public static class ServiceCollectionExt
{
  /// <summary>
  ///   Adds PostgreSQL components to the specified service collection.
  ///   This method configures the PostgreSQL connection and storage services based on the provided configuration.
  /// </summary>
  /// <param name="services">The service collection to which the PostgreSQL components will be added.</param>
  /// <param name="configuration">The configuration manager used to retrieve PostgreSQL settings.</param>
  /// <param name="logger">The logger instance used for logging purposes.</param>
  /// <returns>The updated service collection with PostgreSQL components added.</returns>
  [PublicAPI]
  public static IServiceCollection AddPostgresComponents(this IServiceCollection services,
                                                          ConfigurationManager    configuration,
                                                          ILogger                 logger)
  {
    services.AddPostgresClient(configuration,
                               logger);
    services.AddPostgresStorages(configuration,
                                 logger);
    return services;
  }

  /// <summary>
  ///   Adds PostgreSQL storage services to the specified service collection.
  /// </summary>
  /// <param name="services">The service collection to which the PostgreSQL storage services will be added.</param>
  /// <param name="configuration">The configuration manager used to retrieve PostgreSQL settings.</param>
  /// <param name="logger">The logger instance used for logging purposes.</param>
  /// <returns>The updated service collection with PostgreSQL storage services added.</returns>
  [PublicAPI]
  public static IServiceCollection AddPostgresStorages(this IServiceCollection services,
                                                        ConfigurationManager    configuration,
                                                        ILogger                 logger)
  {
    logger.LogInformation("Configure PostgreSQL Components");

    var components = configuration.GetSection(Components.SettingSection);

    if (components["TableStorage"] == "ArmoniK.Adapters.PostgresSQL.TableStorage")
    {
      services.AddInitializedOption<TableStorage>(configuration,
                                                  TableStorage.SettingSection)
              .AddSingleton<ITaskTable, TaskTable>()
              .AddSingleton<ISessionTable, SessionTable>()
              .AddSingleton<IResultTable, ResultTable>()
              .AddSingleton<IPartitionTable, PartitionTable>()
              .AddSingleton<ITaskWatcher, TaskWatcher>()
              .AddSingleton<IResultWatcher, ResultWatcher>();
    }

    services.TryAddSingleton(_ => ConfigurationExt.GetRequiredValue<Options.PostgreSQL>(configuration,
                                                                                         Options.PostgreSQL.SettingSection));
    services.AddSingletonWithHealthCheck<NpgsqlConnectionProvider>("PostgreSQL.ConnectionProvider");

    return services;
  }

  /// <summary>
  ///   Adds a PostgreSQL client to the specified service collection.
  /// </summary>
  /// <param name="services">The service collection to which the PostgreSQL client will be added.</param>
  /// <param name="configuration">The configuration manager used to retrieve PostgreSQL settings.</param>
  /// <param name="logger">The logger instance used for logging purposes.</param>
  /// <returns>The updated service collection with the PostgreSQL client added.</returns>
  [PublicAPI]
  public static IServiceCollection AddPostgresClient(this IServiceCollection services,
                                                      ConfigurationManager    configuration,
                                                      ILogger                 logger)
  {
    services.TryAddSingleton(_ => ConfigurationExt.GetRequiredValue<Options.PostgreSQL>(configuration,
                                                                                         Options.PostgreSQL.SettingSection));
    return services;
  }

  /// <summary>
  ///   Add the storage provider for the client authentication system to the service collection
  /// </summary>
  /// <param name="services">Services</param>
  /// <param name="configuration">Configuration</param>
  /// <returns>Services</returns>
  [PublicAPI]
  public static IServiceCollection AddClientSubmitterAuthenticationStorage(this IServiceCollection services,
                                                                           ConfigurationManager    configuration)
  {
    var components = configuration.GetSection(Components.SettingSection);
    if (components[nameof(Components.AuthenticationStorage)] == "ArmoniK.Adapters.PostgresSQL.AuthenticationTable")
    {
      services.AddSingleton<IAuthenticationTable, AuthenticationTable>();
    }

    return services;
  }
}
