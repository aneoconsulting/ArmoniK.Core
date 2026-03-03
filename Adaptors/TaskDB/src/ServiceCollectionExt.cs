// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-2026. All rights reserved.

using System.Diagnostics;

using ArmoniK.Core.Adapters.TaskDB.Protocol;
using ArmoniK.Core.Common.Storage;

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.TaskDB;

/// <summary>
///   Extension methods to register TaskDB adapter services.
/// </summary>
[PublicAPI]
public static class ServiceCollectionExt
{
  private static readonly ActivitySource ActivitySource = new("ArmoniK.Core.Adapters.TaskDB");

  /// <summary>
  ///   Registers all TaskDB adapter services into the DI container.
  /// </summary>
  public static IServiceCollection AddTaskDBComponents(this IServiceCollection services,
                                                        ConfigurationManager    configuration,
                                                        ILogger                 logger)
  {
    logger.LogInformation("Configuring TaskDB adapter");

    var components = configuration.GetSection("Components");

    if (components["TableStorage"] == "ArmoniK.Adapters.TaskDB.TableStorage")
    {
      services.AddTaskDBStorages(configuration, logger);
    }

    return services;
  }

  /// <summary>
  ///   Registers TaskDB storage implementations.
  /// </summary>
  public static IServiceCollection AddTaskDBStorages(this IServiceCollection services,
                                                      ConfigurationManager    configuration,
                                                      ILogger                 logger)
  {
    logger.LogInformation("Registering TaskDB table implementations");

    // Bind options
    var options = configuration.GetSection(Options.TaskDB.SettingSection)
                               .Get<Options.TaskDB>() ?? new Options.TaskDB();

    services.AddSingleton(options);
    services.AddSingleton(ActivitySource);

    // Shared TCP connection (one per process, protected by SemaphoreSlim inside)
    services.AddSingleton(provider =>
                          {
                            var opt = provider.GetRequiredService<Options.TaskDB>();
                            var log = provider.GetRequiredService<ILogger<TaskDbConnection>>();
                            return new TaskDbConnection(opt, log);
                          });

    services.AddSingleton<ITaskTable, TaskTable>()
            .AddSingleton<IResultTable, ResultTable>()
            .AddSingleton<ISessionTable, SessionTable>()
            .AddSingleton<IPartitionTable, PartitionTable>();

    return services;
  }
}
