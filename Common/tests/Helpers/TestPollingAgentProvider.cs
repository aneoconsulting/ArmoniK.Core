// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.Memory;
using ArmoniK.Core.Adapters.MongoDB;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.Pollster.TaskProcessingChecker;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Stream.Worker;

using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Mongo2Go;

using MongoDB.Bson;
using MongoDB.Driver;

namespace ArmoniK.Core.Common.Tests.Helpers;

public class TestPollingAgentProvider : IDisposable
{
  private readonly MongoDbRunner runner_;
  private readonly IMongoClient client_;
  private const string DatabaseName = "ArmoniK_TestDB";
  private static readonly ActivitySource ActivitySource = new("ArmoniK.Core.Common.Tests.FullIntegration");
  private readonly IResultTable resultTable_;
  private readonly ITaskTable taskTable_;
  private readonly ISessionTable sessionTable_;
  public readonly ISubmitter Submitter;
  private readonly LoggerFactory loggerFactory_;
  private readonly WebApplication app;
  private readonly Common.Pollster.Pollster pollster_;


  public TestPollingAgentProvider(IWorkerStreamHandler workerStreamHandler)
  {
    var logger = NullLogger.Instance;
    runner_ = MongoDbRunner.Start(singleNodeReplSet: false,
                                  logger: logger);
    client_ = new MongoClient(runner_.ConnectionString);

    // Minimal set of configurations to operate on a toy DB
    Dictionary<string, string> minimalConfig = new()
                                               {
                                                 {
                                                   "Components:TableStorage", "ArmoniK.Adapters.MongoDB.TableStorage"
                                                 },
                                                 {
                                                   "Components:ObjectStorage", "ArmoniK.Adapters.MongoDB.ObjectStorage"
                                                 },
                                                 {
                                                   $"{Adapters.MongoDB.Options.MongoDB.SettingSection}:{nameof(Adapters.MongoDB.Options.MongoDB.DatabaseName)}", DatabaseName
                                                 },
                                                 {
                                                   $"{Adapters.MongoDB.Options.MongoDB.SettingSection}:{nameof(Adapters.MongoDB.Options.MongoDB.TableStorage)}:{nameof(Adapters.MongoDB.Options.MongoDB.TableStorage.PollingDelayMin)}",
                                                   "00:00:10"
                                                 },
                                                 {
                                                   $"{Adapters.MongoDB.Options.MongoDB.SettingSection}:{nameof(Adapters.MongoDB.Options.MongoDB.ObjectStorage)}:{nameof(Adapters.MongoDB.Options.MongoDB.ObjectStorage.ChunkSize)}", "14000"
                                                 },
                                                 {
                                                   $"{ComputePlan.SettingSection}:{nameof(ComputePlan.MessageBatchSize)}", "1"
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
           .AddSingleton(serviceProvider => client_)
           .AddLogging()
           .AddSingleton<ISubmitter, gRPC.Services.Submitter>()
           .AddSingleton<IQueueStorage, QueueStorage>()
           .AddSingleton<Common.Pollster.Pollster>()
           .AddSingleton<DataPrefetcher>()
           .AddSingleton<ITaskProcessingChecker, HelperTaskProcessingChecker>()
           .AddSingleton(workerStreamHandler);

    var computePlanComponent = builder.Configuration.GetSection(ComputePlan.SettingSection);
    var computePlanOptions = computePlanComponent.Get<ComputePlan>();

    builder.Services.AddSingleton(computePlanOptions);

    app = builder.Build();

    resultTable_ = app.Services.GetRequiredService<IResultTable>();
    taskTable_ = app.Services.GetRequiredService<ITaskTable>();
    sessionTable_ = app.Services.GetRequiredService<ISessionTable>();
    Submitter = app.Services.GetRequiredService<ISubmitter>();
    pollster_ = app.Services.GetRequiredService<Common.Pollster.Pollster>();

    sessionTable_.Init(CancellationToken.None).Wait();

    pollsterRunningTask = Task.Factory.StartNew(() => pollster_.MainLoop(pollsterCancellationTokenSource_.Token),
                                                TaskCreationOptions.LongRunning);
  }

  private readonly CancellationTokenSource pollsterCancellationTokenSource_ = new CancellationTokenSource();
  private readonly Task                    pollsterRunningTask;

  public void Dispose()
  {
    pollsterCancellationTokenSource_?.Cancel(false);
    pollsterRunningTask?.Wait();
    pollsterRunningTask?.Dispose();
    pollsterCancellationTokenSource_?.Dispose();
    ((IDisposable)app)?.Dispose();
    loggerFactory_?.Dispose();
    runner_?.Dispose();
    GC.SuppressFinalize(this);
  }
}
