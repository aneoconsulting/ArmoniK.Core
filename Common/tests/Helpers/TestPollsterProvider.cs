// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Options;
using ArmoniK.Core.Adapters.Memory;
using ArmoniK.Core.Adapters.MongoDB;
using ArmoniK.Core.Base;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Injection.Options.Database;
using ArmoniK.Core.Common.Meter;
using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.Pollster.TaskProcessingChecker;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Stream.Worker;
using ArmoniK.Core.Common.Utils;
using ArmoniK.Core.Utils;

using EphemeralMongo;

using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using MongoDB.Bson;
using MongoDB.Driver;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Helpers;

public class TestPollsterProvider : IDisposable
{
  private const           string         DatabaseName   = "ArmoniK_TestDB";
  private static readonly ActivitySource ActivitySource = new("ArmoniK.Core.Common.Tests.TestPollsterProvider");
  private readonly        WebApplication app_;
  private readonly        IMongoClient   client_;

  [SuppressMessage("Usage",
                   "CA2213: Disposable fields must be disposed")]
  public readonly ExceptionManager ExceptionManager;

  private readonly TimeSpan?                graceDelay_;
  public readonly  HealthCheckRecord        HealthCheckRecord;
  public readonly  IHostApplicationLifetime Lifetime;
  private readonly IObjectStorage           objectStorage_;
  public readonly  IPartitionTable          PartitionTable;
  public readonly  Common.Pollster.Pollster Pollster;
  public readonly  IResultTable             ResultTable;
  private readonly IMongoRunner             runner_;
  public readonly  ISessionTable            SessionTable;
  public readonly  ISubmitter               Submitter;
  public readonly  ITaskTable               TaskTable;


  public TestPollsterProvider(IWorkerStreamHandler         workerStreamHandler,
                              IAgentHandler                agentHandler,
                              IPullQueueStorage            pullQueueStorage,
                              TimeSpan?                    graceDelay       = null,
                              TimeSpan?                    acquireTimeout   = null,
                              int                          maxError         = 5,
                              IDictionary<string, string>? additionalConfig = null)
  {
    graceDelay_ = graceDelay;
    var logger = NullLogger.Instance;
    var options = new MongoRunnerOptions
                  {
                    UseSingleNodeReplicaSet = false,
#pragma warning disable CA2254 // log inputs should be constant
                    StandardOuputLogger = line => logger.LogInformation(line),
                    StandardErrorLogger = line => logger.LogError(line),
#pragma warning restore CA2254
                  };

    var binDir = Environment.GetEnvironmentVariable("EphemeralMongo__BinaryDirectory");
    if (!string.IsNullOrEmpty(binDir))
    {
      options.BinaryDirectory = binDir;
    }

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
                                                    $"{ComputePlane.SettingSection}:{nameof(ComputePlane.MessageBatchSize)}", "1"
                                                  },
                                                  {
                                                    $"{Injection.Options.Submitter.SettingSection}:{nameof(Injection.Options.Submitter.DefaultPartition)}",
                                                    "DefaultPartition"
                                                  },
                                                  {
                                                    $"{Injection.Options.Pollster.SettingSection}:{nameof(Injection.Options.Pollster.GraceDelay)}", graceDelay is null
                                                                                                                                                      ? TimeSpan
                                                                                                                                                        .FromSeconds(2)
                                                                                                                                                        .ToString()
                                                                                                                                                      : graceDelay
                                                                                                                                                        .ToString()
                                                  },
                                                  {
                                                    $"{Injection.Options.Pollster.SettingSection}:{nameof(Injection.Options.Pollster.PartitionId)}", "DefaultPartition"
                                                  },
                                                  {
                                                    $"{Injection.Options.Pollster.SettingSection}:{nameof(Injection.Options.Pollster.TimeoutBeforeNextAcquisition)}",
                                                    acquireTimeout is null
                                                      ? TimeSpan.FromSeconds(10)
                                                                .ToString()
                                                      : acquireTimeout.ToString()
                                                  },
                                                  {
                                                    $"{Injection.Options.Pollster.SettingSection}:{nameof(Injection.Options.Pollster.MaxErrorAllowed)}",
                                                    maxError.ToString()
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

    if (additionalConfig is not null)
    {
      foreach (var pair in additionalConfig)
      {
        minimalConfig.Add(pair.Key,
                          pair.Value);
      }
    }

    Console.WriteLine(minimalConfig.ToJson());

    var builder = WebApplication.CreateBuilder();

    builder.Configuration.AddInMemoryCollection(minimalConfig);

    builder.Logging.ClearProviders();
    builder.Logging.AddProvider(new ConsoleForwardingLoggerProvider());

    builder.Services.AddMongoStorages(builder.Configuration,
                                      NullLogger.Instance)
           .AddSingleton(ActivitySource)
           .AddSingleton(_ => client_)
           .AddLogging()
           .AddSingleton<ISubmitter, gRPC.Services.Submitter>()
           .AddOption<Injection.Options.Submitter>(builder.Configuration,
                                                   Injection.Options.Submitter.SettingSection)
           .AddSingleton<IPushQueueStorage, PushQueueStorage>()
           .AddSingleton("ownerpodid")
           .AddSingleton<DataPrefetcher>()
           .AddHostedService<RunningTaskProcessor>()
           .AddHostedService<PostProcessor>()
           .AddSingleton<RunningTaskQueue>()
           .AddSingleton<PostProcessingTaskQueue>()
           .AddSingleton<Common.Pollster.Pollster>()
           .AddSingleton<IObjectStorage, ObjectStorage>()
           .AddSingleton<ITaskProcessingChecker, HelperTaskProcessingChecker>()
           .AddOption<Injection.Options.Pollster>(builder.Configuration,
                                                  Injection.Options.Pollster.SettingSection)
           .AddInitializedOption<InitServices>(builder.Configuration,
                                               InitServices.SettingSection)
           .AddSingleton<InitDatabase>()
           .AddSingleton(sp => new ExceptionManager.Options(sp.GetRequiredService<Injection.Options.Pollster>()
                                                              .GraceDelay,
                                                            sp.GetRequiredService<Injection.Options.Pollster>()
                                                              .MaxErrorAllowed))
           .AddSingleton<ExceptionManager>()
           .AddSingleton<MeterHolder>()
           .AddSingleton<AgentIdentifier>()
           .AddScoped(typeof(FunctionExecutionMetrics<>))
           .AddSingleton<HealthCheckRecord>()
           .AddSingleton(workerStreamHandler)
           .AddSingleton(agentHandler)
           .AddSingleton(pullQueueStorage);

    var computePlanOptions = builder.Configuration.GetRequiredValue<ComputePlane>(ComputePlane.SettingSection);
    builder.Services.AddSingleton(computePlanOptions);

    app_ = builder.Build();
    app_.Start();

    ResultTable       = app_.Services.GetRequiredService<IResultTable>();
    TaskTable         = app_.Services.GetRequiredService<ITaskTable>();
    PartitionTable    = app_.Services.GetRequiredService<IPartitionTable>();
    SessionTable      = app_.Services.GetRequiredService<ISessionTable>();
    Submitter         = app_.Services.GetRequiredService<ISubmitter>();
    Pollster          = app_.Services.GetRequiredService<Common.Pollster.Pollster>();
    objectStorage_    = app_.Services.GetRequiredService<IObjectStorage>();
    ExceptionManager  = app_.Services.GetRequiredService<ExceptionManager>();
    HealthCheckRecord = app_.Services.GetRequiredService<HealthCheckRecord>();
    Lifetime          = app_.Lifetime;

    ResultTable.Init(CancellationToken.None)
               .Wait();
    TaskTable.Init(CancellationToken.None)
             .Wait();
    objectStorage_.Init(CancellationToken.None)
                  .Wait();
    PartitionTable.Init(CancellationToken.None)
                  .Wait();
    SessionTable.Init(CancellationToken.None)
                .Wait();
  }

  public void Dispose()
  {
    app_.StopAsync()
        .Wait();
    ((IDisposable)app_)?.Dispose();
    runner_?.Dispose();
    GC.SuppressFinalize(this);
  }

  public Task StopApplicationAfter(TimeSpan delay = default)
    => Task.Run(async () =>
                {
                  await Task.Delay(delay,
                                   ExceptionManager.EarlyCancellationToken)
                            .ConfigureAwait(false);

                  Lifetime.StopApplication();
                });

  public void AssertFailAfterError(int nbError = 1)
  {
    for (var i = 0; i < nbError; i++)
    {
      if (ExceptionManager.Failed)
      {
        Assert.Fail($"ExceptionManager failed after {i} errors while it was expected to failed after {nbError}");
      }

      ExceptionManager.RecordError(null,
                                   null,
                                   "Dummy Error");
    }

    if (!ExceptionManager.Failed)
    {
      Assert.Fail($"ExceptionManager did not failed while it was expected to failed after {nbError}");
    }
  }
}
