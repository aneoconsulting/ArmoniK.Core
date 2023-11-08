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

using System;
using System.Collections.Generic;
using System.Diagnostics;

using ArmoniK.Core.Common.Injection.Options;

using EphemeralMongo;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.Events;

using Serilog;

namespace ArmoniK.Core.Adapters.MongoDB.Tests;

internal class MongoDatabaseProvider : IDisposable
{
  private const           string          DatabaseName   = "ArmoniK_TestDB";
  private static readonly ActivitySource  ActivitySource = new("ArmoniK.Core.Adapters.MongoDB.Tests");
  private readonly        ServiceProvider provider_;
  private readonly        IMongoRunner?   runner_;

  public MongoDatabaseProvider(bool                        useSingleNodeReplicaSet = false,
                               Action<IServiceCollection>? serviceConfigurator     = null,
                               bool                        showMongoLogs           = false)
  {
    var loggerSerilog = new LoggerConfiguration().WriteTo.Console()
                                                 .Enrich.FromLogContext()
                                                 .CreateLogger();

    var logger = LoggerFactory.Create(builder => builder.AddSerilog(loggerSerilog))
                              .CreateLogger("root");

    var options = new MongoRunnerOptions
                  {
                    UseSingleNodeReplicaSet = useSingleNodeReplicaSet,
#pragma warning disable CA2254 // log inputs should be constant
                    StandardOuputLogger = showMongoLogs
                                            ? line => logger.LogInformation(line)
                                            : null,
                    StandardErrorLogger = showMongoLogs
                                            ? line => logger.LogError(line)
                                            : null,
#pragma warning restore CA2254
                    ReplicaSetSetupTimeout = TimeSpan.FromSeconds(30),
                  };

    runner_ = MongoRunner.Run(options);
    var settings = MongoClientSettings.FromUrl(new MongoUrl(runner_.ConnectionString));

    settings.ClusterConfigurator = cb => cb.Subscribe<CommandStartedEvent>(e => logger.LogInformation("{CommandName} - {Command}",
                                                                                                      e.CommandName,
                                                                                                      e.Command.ToJson()));

    var client = new MongoClient(settings);

    // Minimal set of configurations to operate on a toy DB
    Dictionary<string, string?> minimalConfig = new()
                                                {
                                                  {
                                                    $"{Components.SettingSection}:{nameof(Components.TableStorage)}", "ArmoniK.Adapters.MongoDB.TableStorage"
                                                  },
                                                  {
                                                    $"{Components.SettingSection}:{nameof(Components.ObjectStorage)}", "ArmoniK.Adapters.MongoDB.ObjectStorage"
                                                  },
                                                  {
                                                    $"{Components.SettingSection}:{nameof(Components.AuthenticationStorage)}",
                                                    "ArmoniK.Adapters.MongoDB.AuthenticationTable"
                                                  },
                                                  {
                                                    $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.DatabaseName)}", DatabaseName
                                                  },
                                                  {
                                                    $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.TableStorage)}:{nameof(Options.MongoDB.TableStorage.PollingDelayMax)}",
                                                    "00:00:10"
                                                  },
                                                  {
                                                    $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.ObjectStorage)}:{nameof(Options.MongoDB.ObjectStorage.ChunkSize)}",
                                                    "140000"
                                                  },
                                                };

    var configuration = new ConfigurationManager();
    configuration.AddInMemoryCollection(minimalConfig);

    var serviceCollection = new ServiceCollection();
    serviceCollection.AddMongoStorages(configuration,
                                       logger);
    serviceCollection.AddClientSubmitterAuthenticationStorage(configuration);
    serviceCollection.AddSingleton(ActivitySource);
    serviceCollection.AddTransient<IMongoClient>(_ => client);

    serviceCollection.AddLogging(builder => builder.AddSerilog(loggerSerilog));
    serviceConfigurator?.Invoke(serviceCollection);

    provider_ = serviceCollection.BuildServiceProvider(new ServiceProviderOptions
                                                       {
                                                         ValidateOnBuild = true,
                                                       });
  }

  public void Dispose()
  {
    provider_.Dispose();
    runner_?.Dispose();
  }

  public IServiceProvider GetServiceProvider()
    => provider_;
}
