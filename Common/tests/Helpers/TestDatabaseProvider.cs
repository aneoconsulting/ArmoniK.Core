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
using System.Threading;

using ArmoniK.Api.Common.Options;
using ArmoniK.Core.Adapters.MongoDB;
using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Common.Storage;

using EphemeralMongo;

using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using MongoDB.Bson;
using MongoDB.Driver;

namespace ArmoniK.Core.Common.Tests.Helpers;

public class TestDatabaseProvider : IDisposable
{
  private const           string         DatabaseName   = "ArmoniK_TestDB";
  private static readonly ActivitySource ActivitySource = new("ArmoniK.Core.Common.Tests.TestPollsterProvider");
  private readonly        WebApplication app_;
  private readonly        IMongoClient   client_;
  private readonly        IMongoRunner   runner_;


  public TestDatabaseProvider(Action<IServiceCollection>? configurator = null)
  {
    var logger = NullLogger.Instance;
    var options = new MongoRunnerOptions
                  {
                    UseSingleNodeReplicaSet = false,
                    StandardOuputLogger     = line => logger.LogInformation(line),
                    StandardErrorLogger     = line => logger.LogError(line),
                  };

    runner_ = MongoRunner.Run(options);
    client_ = new MongoClient(runner_.ConnectionString);

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

    var loggerProvider = new ConsoleForwardingLoggerProvider();

    builder.Logging.AddProvider(loggerProvider);

    builder.Services.AddMongoStorages(builder.Configuration,
                                      NullLogger.Instance)
           .AddLogging()
           .AddSingleton(loggerProvider.CreateLogger("root"))
           .AddSingleton(ActivitySource)
           .AddSingleton(_ => client_);
    configurator?.Invoke(builder.Services);

    app_ = builder.Build();

    var sessionProvider = app_.Services.GetRequiredService<SessionProvider>();
    sessionProvider.Init(CancellationToken.None)
                   .Wait();

    app_.Services.GetRequiredService<IResultTable>()
        .Init(CancellationToken.None)
        .Wait();

    app_.Services.GetRequiredService<ITaskTable>()
        .Init(CancellationToken.None)
        .Wait();

    app_.Services.GetRequiredService<ISessionTable>()
        .Init(CancellationToken.None)
        .Wait();

    app_.Services.GetRequiredService<IPartitionTable>()
        .Init(CancellationToken.None)
        .Wait();

    app_.Services.GetRequiredService<IObjectStorageFactory>()
        .Init(CancellationToken.None)
        .Wait();
  }

  public void Dispose()
  {
    ((IDisposable)app_).Dispose();
    runner_.Dispose();
    GC.SuppressFinalize(this);
  }

  public T GetRequiredService<T>()
    where T : notnull
    => app_.Services.GetRequiredService<T>();
}
