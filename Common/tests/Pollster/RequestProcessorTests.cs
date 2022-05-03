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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Adapters.MongoDB;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Pollster;

using ArmoniK.Core.Common.Storage;

using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Stream.Worker;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Mongo2Go;

using MongoDB.Driver;

using Moq;

using NUnit.Framework;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;
using TaskOptions = ArmoniK.Core.Common.Storage.TaskOptions;
using Output = ArmoniK.Core.Common.Storage.Output;
using ComputeRequest = ArmoniK.Api.gRPC.V1.ProcessRequest.Types.ComputeRequest;
using Empty = ArmoniK.Api.gRPC.V1.Empty;

namespace ArmoniK.Core.Common.Tests.Pollster;

internal class MyAsyncStreamReader<T> : IAsyncStreamReader<T>
{
  private readonly IEnumerator<T> enumerator_;

  public MyAsyncStreamReader(IEnumerable<T> results)
    => enumerator_ = results.GetEnumerator();

  public T Current
    => enumerator_.Current;

  public Task<bool> MoveNext(CancellationToken cancellationToken)
    => Task.Run(() => enumerator_.MoveNext(),
                cancellationToken);
}

internal class MyClientStreamWriter<T> : IClientStreamWriter<T>
{
  private readonly List<T> messages_ = new();

  public IList<T> Messages
    => messages_;

  public Task WriteAsync(T message)
  {
    messages_.Add(message);
    return Task.CompletedTask;
  }
  public Task CompleteAsync()
  {
    return Task.CompletedTask;
  }

  public WriteOptions WriteOptions { get; set; }
}

[TestFixture]
public class RequestProcessorTest
{
  private       ActivitySource              activitySource_;
  private       Mock<IObjectStorage>        mockObjectStorage_;
  private       Mock<IObjectStorageFactory> mockObjectStorageFactory_;
  private       Mock<IObjectStorage>        mockResultStorage_;
  private       Mock<IWorkerStreamHandler>  mockWorkerStreamHandler_;
  private       ILoggerFactory              loggerFactory_;
  private       RequestProcessor            requestProcessor_;
  private       MongoDbRunner               runner_;
  private       MongoClient                 client_;
  private       IResultTable                resultTable_;
  private       ISessionTable               sessionTable_;
  private       ITaskTable                  taskTable_;
  private const string                      DatabaseName = "ArmoniK_TestDB";
  private const string                      SessionId    = "SessionId";
  private const string                      ParentTaskId = "ParentTaskId";
  private const string                      TaskId       = "TaskId";
  private const string                      Output1      = "Output1";
  private const string                      Dependency1  = "Dependency1";
  private const string                      PodId        = "PodId";

  [SetUp]
  public void SetUp()
  {
    activitySource_           = new ActivitySource(nameof(RequestProcessorTest));
    mockObjectStorageFactory_ = new Mock<IObjectStorageFactory>();
    mockObjectStorage_        = new Mock<IObjectStorage>();
    mockResultStorage_        = new Mock<IObjectStorage>();
    mockWorkerStreamHandler_  = new Mock<IWorkerStreamHandler>(); 

    loggerFactory_ = LoggerFactory.Create(builder =>
    {
      builder.AddFilter("Microsoft",
                        LogLevel.Warning)
             .AddFilter("Microsoft",
                        LogLevel.Error)
             .AddConsole();
    });

    runner_ = MongoDbRunner.Start(singleNodeReplSet: false,
                                  logger: loggerFactory_.CreateLogger<NullLogger>());
    client_ = new MongoClient(runner_.ConnectionString);

    // Minimal set of configurations to operate on a toy DB
    Dictionary<string, string> minimalConfig = new()
    {
      {
        "Components:TableStorage",
        "ArmoniK.Adapters.MongoDB.TableStorage"
      },
      {
        $"{Adapters.MongoDB.Options.MongoDB.SettingSection}:{nameof(Adapters.MongoDB.Options.MongoDB.DatabaseName)}",
        DatabaseName
      },
      {
        $"{Adapters.MongoDB.Options.MongoDB.SettingSection}:{nameof(Adapters.MongoDB.Options.MongoDB.TableStorage)}:PollingDelay",
        "00:00:10"
      },
    };

    var configuration = new ConfigurationManager();
    configuration.AddInMemoryCollection(minimalConfig);

    var services = new ServiceCollection();
    services.AddMongoStorages(configuration,
                              loggerFactory_.CreateLogger<NullLogger>());
    services.AddSingleton(activitySource_);
    services.AddTransient<IMongoClient>(serviceProvider => client_);
    services.AddLogging(builder => builder.AddConsole()
                                          .AddFilter(level => level >= LogLevel.Information));

    var provider = services.BuildServiceProvider(new ServiceProviderOptions
    {
      ValidateOnBuild = true,
    });

    resultTable_ = provider.GetRequiredService<IResultTable>();
    sessionTable_ = provider.GetRequiredService<ISessionTable>();
    taskTable_ = provider.GetRequiredService<ITaskTable>();

    mockResultStorage_.Setup(x => x.GetValuesAsync(It.IsAny<string>(),
                                                  It.IsAny<CancellationToken>()))
                      .Returns((string key,
                                CancellationToken token) => new List<byte[]>
                                                            {
                                                              Convert.FromBase64String("1234"),
                                                            }.ToAsyncEnumerable());
    mockObjectStorage_.Setup(x => x.GetValuesAsync(It.IsAny<string>(),
                                                   It.IsAny<CancellationToken>()))
                      .Returns((string key,
                                CancellationToken token) => new List<byte[]>().ToAsyncEnumerable());

    mockObjectStorageFactory_.Setup(x => x.CreateObjectStorage(It.IsAny<string>()))
                             .Returns((string objectName) =>
                             {
                               if (objectName.StartsWith("results"))
                               {
                                 return mockResultStorage_.Object;
                               }

                               if (objectName.StartsWith("payloads"))
                               {
                                 return mockObjectStorage_.Object;
                               }

                               return null;
                             });

    taskTable_.CreateTasks(new[]
                           {
                             new TaskData(SessionId,
                                          TaskId,
                                          PodId,
                                          new[]
                                          {
                                            ParentTaskId,
                                          },
                                          new[]
                                          {
                                            Dependency1,
                                          },
                                          new[]
                                          {
                                            Output1,
                                          },
                                          Array.Empty<string>(),
                                          TaskStatus.Submitted,
                                          "",
                                          new TaskOptions(new Dictionary<string, string>(),
                                                          TimeSpan.FromSeconds(100),
                                                          5,
                                                          1),
                                          DateTime.Now,
                                          DateTime.Now + TimeSpan.FromSeconds(1),
                                          DateTime.MinValue,
                                          DateTime.MinValue,
                                          DateTime.Now,
                                          new Output(true,
                                                     "")),
                           });

    resultTable_.Create(new[]
                       {
                         new Result("sessionId",
                                    "ResultIsAvailable",
                                    "OwnerId",
                                    "Completed",
                                    DateTime.Today,
                                    new[]
                                    {
                                      (byte) 1,
                                    }),
                         new Result("sessionId",
                                    "ResultIsNotAvailable",
                                    "OwnerId",
                                    "Created",
                                    DateTime.Today,
                                    new[]
                                    {
                                      (byte) 1,
                                    }),
                       })
               .Wait();

    sessionTable_.CreateSessionDataAsync("sessionId",
                                        "taskId",
                                        new Api.gRPC.V1.TaskOptions(),
                                        CancellationToken.None).Wait();

    var queueStorage = new Mock<IQueueStorage>();
    var submitter = new gRPC.Services.Submitter(queueStorage.Object,
                                                mockObjectStorageFactory_.Object,
                                                loggerFactory_.CreateLogger<gRPC.Services.Submitter>(),
                                                sessionTable_,
                                                taskTable_,
                                                resultTable_,
                                                activitySource_);

    requestProcessor_ = new RequestProcessor(mockWorkerStreamHandler_.Object,
                                             mockObjectStorageFactory_.Object,
                                             loggerFactory_.CreateLogger<Common.Pollster.Pollster>(),
                                             submitter,
                                             resultTable_,
                                             activitySource_);
  }

  [TearDown]
  public virtual void TearDown()
  {
    runner_.Dispose();
    client_ = null;
  }

  [Test]
  public void SetUpCreatesRequiredObjects()
  {
    // Just check that the SetUp function does not crash
    Assert.IsTrue(true);
  }

  [Test]
  public async Task TestProcessInternalsAsync()
  {
    var taskData = new TaskData(SessionId,
                                TaskId,
                                PodId,
                                new[]
                                {
                                  ParentTaskId,
                                },
                                new[]
                                {
                                  Dependency1,
                                },
                                new[]
                                {
                                  Output1,
                                },
                                Array.Empty<string>(),
                                TaskStatus.Submitted,
                                "",
                                new TaskOptions(new Dictionary<string, string>(),
                                                TimeSpan.FromSeconds(100),
                                                5,
                                                1),
                                DateTime.Now,
                                DateTime.Now + TimeSpan.FromSeconds(1),
                                DateTime.MinValue,
                                DateTime.MinValue,
                                DateTime.Now,
                                new Output(true,
                                           ""));

    var dataPrefetcher = new DataPrefetcher(mockObjectStorageFactory_.Object,
                                            activitySource_,
                                            loggerFactory_.CreateLogger<DataPrefetcher>());

    var requests = await dataPrefetcher.PrefetchDataAsync(taskData,
                                                          CancellationToken.None)
                                       .ConfigureAwait(false);


    Console.WriteLine("Requests:");
    foreach (var cr in requests)
    {
      Console.WriteLine(cr.ToString());
    }

    var computeReplies = new List<ProcessReply>();

    computeReplies.Add(
                       new ProcessReply
                       {
                         Output = new Api.gRPC.V1.Output
                                  {
                                    Ok = new Empty(), 
                                    Status = TaskStatus.Completed,
                                  },
                       });

    Console.WriteLine("Replies:");
    foreach (var cr in computeReplies)
    {
      Console.WriteLine(cr.ToString());
    }

    mockWorkerStreamHandler_.Setup(s => s.WorkerResponseStream)
                            .Returns(() => new MyAsyncStreamReader<ProcessReply>(computeReplies));

    mockWorkerStreamHandler_.Setup(s => s.WorkerRequestStream)
                            .Returns(() => new MyClientStreamWriter<ProcessRequest>());

    await requestProcessor_.ProcessInternalsAsync(taskData,
                                                  requests,
                                                  CancellationToken.None).ConfigureAwait(false);
  }
}