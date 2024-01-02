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
using System.IO;
using System.Threading;

using ArmoniK.Api.Common.Options;
using ArmoniK.Core.Adapters.Memory;
using ArmoniK.Core.Adapters.MongoDB;
using ArmoniK.Core.Base;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.Pollster.TaskProcessingChecker;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Stream.Worker;
using ArmoniK.Core.Utils;

using EphemeralMongo;

using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using MongoDB.Bson;
using MongoDB.Driver;

namespace ArmoniK.Core.Common.Tests.Helpers;

public class TestTaskHandlerProvider : IDisposable
{
  private const           string          DatabaseName   = "ArmoniK_TestDB";
  private static readonly ActivitySource  ActivitySource = new("ArmoniK.Core.Common.Tests.TestTaskHandlerProvider");
  private readonly        WebApplication  app_;
  private readonly        IMongoClient    client_;
  private readonly        LoggerFactory   loggerFactory_;
  private readonly        IObjectStorage  objectStorage_;
  public readonly         IPartitionTable PartitionTable;
  public readonly         IResultTable    ResultTable;
  private readonly        IMongoRunner    runner_;
  public readonly         ISessionTable   SessionTable;
  public readonly         ISubmitter      Submitter;
  public readonly         TaskHandler     TaskHandler;
  public readonly         ITaskTable      TaskTable;


  public TestTaskHandlerProvider(IWorkerStreamHandler    workerStreamHandler,
                                 IAgentHandler           agentHandler,
                                 IQueueMessageHandler    queueStorage,
                                 CancellationTokenSource cancellationTokenSource,
                                 ITaskTable?             inputTaskTable    = null,
                                 ISessionTable?          inputSessionTable = null)
  {
    var logger = NullLogger.Instance;

    var options = new MongoRunnerOptions
                  {
                    UseSingleNodeReplicaSet = false,
#pragma warning disable CA2254 // log inputs should be constant
                    StandardOuputLogger = line => logger.LogInformation(line),
                    StandardErrorLogger = line => logger.LogError(line),
#pragma warning restore CA2254
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
                                                  {
                                                    $"{Injection.Options.Pollster.SettingSection}:{nameof(Injection.Options.Pollster.SharedCacheFolder)}",
                                                    Path.Combine(Path.GetTempPath(),
                                                                 "data")
                                                  },
                                                  {
                                                    $"{Injection.Options.Pollster.SettingSection}:{nameof(Injection.Options.Pollster.InternalCacheFolder)}",
                                                    Path.Combine(Path.GetTempPath(),
                                                                 "internal")
                                                  },
                                                };

    Console.WriteLine(minimalConfig.ToJson());

    loggerFactory_ = new LoggerFactory();
    loggerFactory_.AddProvider(new ConsoleForwardingLoggerProvider());

    var builder = WebApplication.CreateBuilder();

    builder.Configuration.AddInMemoryCollection(minimalConfig);

    builder.Services.AddMongoStorages(builder.Configuration,
                                      logger)
           .AddSingleton(ActivitySource)
           .AddSingleton(_ => client_)
           .AddLogging()
           .AddSingleton(loggerFactory_.CreateLogger(nameof(TestTaskHandlerProvider)))
           .AddSingleton<ISubmitter, gRPC.Services.Submitter>()
           .AddOption<Injection.Options.Submitter>(builder.Configuration,
                                                   Injection.Options.Submitter.SettingSection)
           .AddOption<Injection.Options.Pollster>(builder.Configuration,
                                                  Injection.Options.Pollster.SettingSection)
           .AddSingleton<IPushQueueStorage, PushQueueStorage>()
           .AddSingleton(provider => new TaskHandler(provider.GetRequiredService<ISessionTable>(),
                                                     provider.GetRequiredService<ITaskTable>(),
                                                     provider.GetRequiredService<IResultTable>(),
                                                     provider.GetRequiredService<ISubmitter>(),
                                                     provider.GetRequiredService<DataPrefetcher>(),
                                                     workerStreamHandler,
                                                     queueStorage,
                                                     provider.GetRequiredService<ITaskProcessingChecker>(),
                                                     "ownerpodid",
                                                     "ownerpodname",
                                                     provider.GetRequiredService<ActivitySource>(),
                                                     agentHandler,
                                                     provider.GetRequiredService<ILogger>(),
                                                     provider.GetRequiredService<Injection.Options.Pollster>(),
                                                     () =>
                                                     {
                                                     },
                                                     cancellationTokenSource))
           .AddSingleton<DataPrefetcher>()
           .AddSingleton<ITaskProcessingChecker, HelperTaskProcessingChecker>();

    if (inputTaskTable is not null)
    {
      builder.Services.AddSingleton(inputTaskTable);
    }

    if (inputSessionTable is not null)
    {
      builder.Services.AddSingleton(inputSessionTable);
    }

    var computePlanOptions = builder.Configuration.GetRequiredValue<ComputePlane>(ComputePlane.SettingSection);
    builder.Services.AddSingleton(computePlanOptions);

    app_ = builder.Build();

    ResultTable    = app_.Services.GetRequiredService<IResultTable>();
    TaskTable      = app_.Services.GetRequiredService<ITaskTable>();
    PartitionTable = app_.Services.GetRequiredService<IPartitionTable>();
    SessionTable   = app_.Services.GetRequiredService<ISessionTable>();
    Submitter      = app_.Services.GetRequiredService<ISubmitter>();
    TaskHandler    = app_.Services.GetRequiredService<TaskHandler>();
    objectStorage_ = app_.Services.GetRequiredService<IObjectStorage>();

    ResultTable.Init(CancellationToken.None)
               .Wait();
    TaskTable.Init(CancellationToken.None)
             .Wait();
    PartitionTable.Init(CancellationToken.None)
                  .Wait();
    SessionTable.Init(CancellationToken.None)
                .Wait();
    objectStorage_.Init(CancellationToken.None)
                  .Wait();
  }

  public void Dispose()
  {
    ((IDisposable)app_)?.Dispose();
    loggerFactory_?.Dispose();
    runner_?.Dispose();
    TaskHandler.DisposeAsync()
               .AsTask()
               .Wait();
    GC.SuppressFinalize(this);
  }
}
