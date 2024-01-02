// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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
using System.Threading;

using ArmoniK.Api.Common.Options;
using ArmoniK.Core.Adapters.MongoDB;
using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Injection;
using ArmoniK.Core.Common.Storage;

using EphemeralMongo;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.Events;

namespace ArmoniK.Core.Common.Tests.Helpers;

public class TestDatabaseProvider : IDisposable
{
  private const           string         DatabaseName   = "ArmoniK_TestDB";
  private static readonly ActivitySource ActivitySource = new("ArmoniK.Core.Common.Tests.TestPollsterProvider");
  public readonly         WebApplication App;
  private readonly        IMongoClient   client_;
  private readonly        IMongoRunner   runner_;


  public TestDatabaseProvider(Action<IServiceCollection>?    collectionConfigurator           = null,
                              Action<IApplicationBuilder>?   applicationBuilderConfigurator   = null,
                              Action<IEndpointRouteBuilder>? endpointRouteBuilderConfigurator = null,
                              bool                           logMongoRequests                 = false,
                              bool                           validateGrpcRequests             = false,
                              bool                           useSingleNodeReplicaSet          = false)
  {
    var logger = NullLogger.Instance;
    var options = new MongoRunnerOptions
                  {
                    UseSingleNodeReplicaSet = useSingleNodeReplicaSet,
#pragma warning disable CA2254 // log inputs should be constant
                    StandardOuputLogger = line => logger.LogInformation(line),
                    StandardErrorLogger = line => logger.LogError(line),
#pragma warning restore CA2254
                  };

    var loggerProvider = new ConsoleForwardingLoggerProvider();
    var loggerDb       = loggerProvider.CreateLogger("db commands");

    runner_ = MongoRunner.Run(options);
    var settings = MongoClientSettings.FromConnectionString(runner_.ConnectionString);

    if (logMongoRequests)
    {
      settings.ClusterConfigurator = cb =>
                                     {
                                       cb.Subscribe<CommandStartedEvent>(e => loggerDb.LogTrace("{CommandName} - {Command}",
                                                                                                e.CommandName,
                                                                                                e.Command.ToJson()));
                                     };
    }

    client_ = new MongoClient(settings);

    // Minimal set of configurations to operate on a toy DB
    Dictionary<string, string?> minimalConfig = new()
                                                {
                                                  {
                                                    "Components:TableStorage", "ArmoniK.Adapters.MongoDB.TableStorage"
                                                  },
                                                  {
                                                    "Components:ObjectStorage", "ArmoniK.Adapters.MongoDB.ObjectStorage"
                                                  },
                                                  {
                                                    "Components:AuthenticationStorage", "ArmoniK.Adapters.MongoDB.AuthenticationTable"
                                                  },
                                                  {
                                                    $"{Adapters.MongoDB.Options.MongoDB.SettingSection}:{nameof(Adapters.MongoDB.Options.MongoDB.DatabaseName)}",
                                                    DatabaseName
                                                  },
                                                  {
                                                    $"{Adapters.MongoDB.Options.MongoDB.SettingSection}:{nameof(Adapters.MongoDB.Options.MongoDB.TableStorage)}:{nameof(Adapters.MongoDB.Options.MongoDB.TableStorage.PollingDelayMin)}",
                                                    "00:00:10"
                                                  },
                                                  {
                                                    $"{Adapters.MongoDB.Options.MongoDB.SettingSection}:{nameof(Adapters.MongoDB.Options.MongoDB.ObjectStorage)}:{nameof(Adapters.MongoDB.Options.MongoDB.ObjectStorage.ChunkSize)}",
                                                    "14000"
                                                  },
                                                  {
                                                    $"{ComputePlane.SettingSection}:{nameof(ComputePlane.MessageBatchSize)}", "1"
                                                  },
                                                  {
                                                    $"{Injection.Options.Submitter.SettingSection}:{nameof(Injection.Options.Submitter.DefaultPartition)}",
                                                    "DefaultPartition"
                                                  },
                                                  {
                                                    $"{Injection.Options.Pollster.SettingSection}:{nameof(Injection.Options.Pollster.GraceDelay)}", "00:00:02"
                                                  },
                                                };

    Console.WriteLine(minimalConfig.ToJson());

    var builder = WebApplication.CreateBuilder();

    builder.Configuration.AddInMemoryCollection(minimalConfig);

    builder.Logging.ClearProviders();

    builder.Logging.AddProvider(loggerProvider);

    builder.Services.AddMongoStorages(builder.Configuration,
                                      NullLogger.Instance)
           .AddClientSubmitterAuthenticationStorage(builder.Configuration)
           .AddClientSubmitterAuthServices(builder.Configuration,
                                           out _)
           .Configure<AuthenticatorOptions>(o => o.CopyFrom(AuthenticatorOptions.DefaultNoAuth))
           .AddLogging()
           .AddSingleton(loggerProvider.CreateLogger("root"))
           .AddSingleton(ActivitySource)
           .AddSingleton(_ => client_);

    if (validateGrpcRequests)
    {
      builder.Services.ValidateGrpcRequests();
    }

    collectionConfigurator?.Invoke(builder.Services);

    builder.WebHost.UseTestServer(o => o.PreserveExecutionContext = true);

    App = builder.Build();

    applicationBuilderConfigurator?.Invoke(App);
    endpointRouteBuilderConfigurator?.Invoke(App);

    App.Services.GetRequiredService<SessionProvider>()
       .Init(CancellationToken.None)
       .Wait();

    App.Services.GetRequiredService<IResultTable>()
       .Init(CancellationToken.None)
       .Wait();

    App.Services.GetRequiredService<ITaskTable>()
       .Init(CancellationToken.None)
       .Wait();

    App.Services.GetRequiredService<ISessionTable>()
       .Init(CancellationToken.None)
       .Wait();

    App.Services.GetRequiredService<IPartitionTable>()
       .Init(CancellationToken.None)
       .Wait();

    App.Services.GetRequiredService<IObjectStorage>()
       .Init(CancellationToken.None)
       .Wait();
  }

  public void Dispose()
  {
    ((IDisposable)App).Dispose();
    runner_.Dispose();
    GC.SuppressFinalize(this);
  }

  public T GetRequiredService<T>()
    where T : notnull
    => App.Services.GetRequiredService<T>();
}
