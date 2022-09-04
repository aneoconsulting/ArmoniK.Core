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
using ArmoniK.Core.Adapters.Memory;
using ArmoniK.Core.Adapters.MongoDB;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Injection;
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

public class TestPollsterProvider : IDisposable
{
  private const           string                   DatabaseName   = "ArmoniK_TestDB";
  private static readonly ActivitySource           ActivitySource = new("ArmoniK.Core.Common.Tests.TestPollsterProvider");
  private readonly        WebApplication           app_;
  private readonly        IMongoClient             client_;
  private readonly        ILoggerFactory           loggerFactory_;
  public readonly         IPartitionTable          PartitionTable;
  private readonly        IResultTable             resultTable_;
  private readonly        MongoDbRunner            runner_;
  private readonly        ISessionTable            sessionTable_;
  public readonly         ISubmitter               Submitter;
  public                  Common.Pollster.Pollster Pollster;
  public readonly         ITaskTable               TaskTable;


  public TestPollsterProvider(IWorkerStreamHandler workerStreamHandler,
                              IAgentHandler        agentHandler,
                              IPullQueueStorage    pullQueueStorage)
  {
    runner_ = MongoDbRunner.Start(singleNodeReplSet: false,
                                  logger: NullLogger.Instance);
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
    builder.Logging.AddProvider(new ConsoleForwardingLoggerProvider());

    builder.Services.AddMongoStorages(builder.Configuration,
                                      NullLogger.Instance)
           .AddSingleton(ActivitySource)
           .AddSingleton(_ => client_)
           .AddSingleton<ISubmitter, gRPC.Services.Submitter>()
           .AddOption<Injection.Options.Submitter>(builder.Configuration,
                                                   Injection.Options.Submitter.SettingSection)
           .AddSingleton<IPushQueueStorage, PushQueueStorage>()
           .AddSingleton("ownerpodid")
           .AddSingleton<DataPrefetcher>()
           .AddSingleton<Common.Pollster.Pollster>()
           .AddSingleton<ITaskProcessingChecker, HelperTaskProcessingChecker>()
           .AddOption<Injection.Options.Pollster>(builder.Configuration,
                                                  Injection.Options.Pollster.SettingSection)
           .AddSingleton(workerStreamHandler)
           .AddSingleton(agentHandler)
           .AddSingleton(pullQueueStorage);


    var computePlanComponent = builder.Configuration.GetSection(ComputePlane.SettingSection);
    var computePlanOptions   = computePlanComponent.Get<ComputePlane>();

    builder.Services.AddSingleton(computePlanOptions);

    app_ = builder.Build();

    resultTable_   = app_.Services.GetRequiredService<IResultTable>();
    TaskTable      = app_.Services.GetRequiredService<ITaskTable>();
    PartitionTable = app_.Services.GetRequiredService<IPartitionTable>();
    sessionTable_  = app_.Services.GetRequiredService<ISessionTable>();
    Submitter      = app_.Services.GetRequiredService<ISubmitter>();
    Pollster       = app_.Services.GetRequiredService<Common.Pollster.Pollster>();

    sessionTable_.Init(CancellationToken.None)
                 .Wait();
  }

  public void Dispose()
  {
    ((IDisposable)app_)?.Dispose();
    loggerFactory_?.Dispose();
    runner_?.Dispose();
    GC.SuppressFinalize(this);
  }
}
