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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Agent;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.Helpers;

using Google.Protobuf;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using NUnit.Framework;

using Agent = ArmoniK.Core.Common.gRPC.Services.Agent;
using Result = ArmoniK.Api.gRPC.V1.Agent.Result;
using TaskOptions = ArmoniK.Core.Common.Storage.TaskOptions;
using TaskRequest = ArmoniK.Core.Common.gRPC.Services.TaskRequest;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Tests.Pollster;

[TestFixture]
public class AgentTest
{
  [SetUp]
  public void SetUp()
  {
  }

  [TearDown]
  public virtual void TearDown()
  {
  }

  private const string Partition       = "PartitionId";
  private const string ExpectedOutput1 = "ExpectedOutput1";
  private const string ExpectedOutput2 = "ExpectedOutput2";


  private static readonly TaskOptions Options = new(new Dictionary<string, string>
                                                    {
                                                      {
                                                        "key1", "val1"
                                                      },
                                                      {
                                                        "key2", "val2"
                                                      },
                                                    },
                                                    TimeSpan.FromMinutes(10),
                                                    5,
                                                    1,
                                                    Partition,
                                                    "applicationName",
                                                    "applicationVersion",
                                                    "applicationNamespace",
                                                    "applicationService",
                                                    "engineType");

  private static readonly Injection.Options.Submitter SubmitterOptions = new()
                                                                         {
                                                                           DefaultPartition = Partition,
                                                                           MaxErrorAllowed  = -1,
                                                                         };

  private static readonly Injection.Options.DependencyResolver DependencyResolverOptions = new()
                                                                                           {
                                                                                             UnresolvedDependenciesQueue =
                                                                                               nameof(DependencyResolverOptions.UnresolvedDependenciesQueue),
                                                                                           };

  public class MyPushQueueStorage : IPushQueueStorage
  {
    public ConcurrentDictionary<string, ConcurrentBag<string>> Messages = new();

    public Task<HealthCheckResult> Check(HealthCheckTag tag)
      => throw new NotImplementedException();

    public Task Init(CancellationToken cancellationToken)
      => throw new NotImplementedException();

    public int MaxPriority
      => 10;

    public Task PushMessagesAsync(IEnumerable<string> messages,
                                  string              partitionId,
                                  int                 priority          = 1,
                                  CancellationToken   cancellationToken = default)
    {
      var partitionMessages = Messages.GetOrAdd(partitionId,
                                                _ => new ConcurrentBag<string>());
      foreach (var message in messages)
      {
        partitionMessages.Add(message);
      }

      return Task.CompletedTask;
    }
  }

  private class AgentHolder : IDisposable
  {
    public readonly  Agent                Agent;
    private readonly TestDatabaseProvider prov_;
    public readonly  MyPushQueueStorage   QueueStorage;
    public readonly  IObjectStorage       ResourceStorage;
    public readonly  IResultTable         ResultTable;
    public readonly  string               Session;
    public readonly  TaskData             TaskData;
    public readonly  ITaskTable           TaskTable;
    public readonly  string               TaskWithDependencies1;
    public readonly  string               TaskWithDependencies2;
    public readonly  string               Token;

    public AgentHolder()
    {
      QueueStorage = new MyPushQueueStorage();
      prov_ = new TestDatabaseProvider(collection => collection.AddSingleton<ISubmitter, gRPC.Services.Submitter>()
                                                               .AddSingleton(SubmitterOptions)
                                                               .AddSingleton(DependencyResolverOptions)
                                                               .AddSingleton<IPushQueueStorage>(QueueStorage));

      ResultTable = prov_.GetRequiredService<IResultTable>();
      TaskTable   = prov_.GetRequiredService<ITaskTable>();

      ResultTable.Init(CancellationToken.None)
                 .ConfigureAwait(false);
      TaskTable.Init(CancellationToken.None)
               .Wait();

      var sessionTable         = prov_.GetRequiredService<ISessionTable>();
      var submitter            = prov_.GetRequiredService<ISubmitter>();
      var objectStorageFactory = prov_.GetRequiredService<IObjectStorageFactory>();

      sessionTable.Init(CancellationToken.None)
                  .Wait();
      objectStorageFactory.Init(CancellationToken.None)
                          .Wait();
      ResourceStorage = objectStorageFactory.CreateResourcesStorage();

      Session = sessionTable.SetSessionDataAsync(new[]
                                                 {
                                                   Partition,
                                                 },
                                                 Options,
                                                 CancellationToken.None)
                            .Result;

      var sessionData = sessionTable.GetSessionAsync(Session,
                                                     CancellationToken.None)
                                    .Result;

      var createdTasks = submitter.CreateTasks(Session,
                                               Session,
                                               Options,
                                               new[]
                                               {
                                                 new TaskRequest(new List<string>
                                                                 {
                                                                   ExpectedOutput1,
                                                                 },
                                                                 new List<string>(),
                                                                 new List<byte[]>
                                                                   {
                                                                     Encoding.ASCII.GetBytes("Payload1"),
                                                                     Encoding.ASCII.GetBytes("Payload2"),
                                                                   }.Select(bytes => new ReadOnlyMemory<byte>(bytes))
                                                                    .ToAsyncEnumerable()),
                                               }.ToAsyncEnumerable(),
                                               CancellationToken.None)
                                  .Result;

      submitter.FinalizeTaskCreation(createdTasks.requests,
                                     createdTasks.priority,
                                     createdTasks.partitionId,
                                     Session,
                                     Session,
                                     CancellationToken.None)
               .Wait();

      QueueStorage.Messages.Clear();

      TaskData = TaskTable.ReadTaskAsync(createdTasks.requests.Single()
                                                     .Id,
                                         CancellationToken.None)
                          .Result;

      var createdTasks2 = submitter.CreateTasks(Session,
                                                Session,
                                                Options,
                                                new[]
                                                {
                                                  new TaskRequest(new List<string>
                                                                  {
                                                                    ExpectedOutput2,
                                                                  },
                                                                  new List<string>
                                                                  {
                                                                    ExpectedOutput1,
                                                                  },
                                                                  new List<byte[]>
                                                                    {
                                                                      Encoding.ASCII.GetBytes("Payload1"),
                                                                      Encoding.ASCII.GetBytes("Payload2"),
                                                                    }.Select(bytes => new ReadOnlyMemory<byte>(bytes))
                                                                     .ToAsyncEnumerable()),
                                                  new TaskRequest(new List<string>(),
                                                                  new List<string>
                                                                  {
                                                                    ExpectedOutput1,
                                                                  },
                                                                  new List<byte[]>
                                                                    {
                                                                      Encoding.ASCII.GetBytes("Payload1"),
                                                                      Encoding.ASCII.GetBytes("Payload2"),
                                                                    }.Select(bytes => new ReadOnlyMemory<byte>(bytes))
                                                                     .ToAsyncEnumerable()),
                                                }.ToAsyncEnumerable(),
                                                CancellationToken.None)
                                   .Result;

      TaskWithDependencies1 = createdTasks2.requests.First()
                                           .Id;
      TaskWithDependencies2 = createdTasks2.requests.Last()
                                           .Id;

      Token = Guid.NewGuid()
                  .ToString();

      Agent = new Agent(submitter,
                        objectStorageFactory,
                        QueueStorage,
                        TaskTable,
                        DependencyResolverOptions,
                        sessionData,
                        TaskData,
                        Token,
                        prov_.GetRequiredService<ILogger<Agent>>());
    }

    public void Dispose()
    {
      Agent.Dispose();
      prov_.Dispose();
    }
  }

  [Test]
  [TestCase("")]
  [TestCase("WrongToken")]
  public async Task WrongTokens(string token)
  {
    using var holder = new AgentHolder();

    var createTaskReply = await holder.Agent.CreateTask(new TestHelperAsyncStreamReader<CreateTaskRequest>(new[]
                                                                                                           {
                                                                                                             new CreateTaskRequest
                                                                                                             {
                                                                                                               CommunicationToken = token,
                                                                                                             },
                                                                                                           }),
                                                        CancellationToken.None)
                                      .ConfigureAwait(false);

    Assert.AreEqual(CreateTaskReply.ResponseOneofCase.Error,
                    createTaskReply.ResponseCase);

    var resultReply = await holder.Agent.SendResult(new TestHelperAsyncStreamReader<Result>(new[]
                                                                                            {
                                                                                              new Result
                                                                                              {
                                                                                                CommunicationToken = token,
                                                                                              },
                                                                                            }),
                                                    CancellationToken.None)
                                  .ConfigureAwait(false);

    Assert.AreEqual(ResultReply.TypeOneofCase.Error,
                    resultReply.TypeCase);

    var commonData = new TestHelperServerStreamWriter<DataReply>();

    await holder.Agent.GetCommonData(new DataRequest
                                     {
                                       CommunicationToken = token,
                                     },
                                     commonData,
                                     CancellationToken.None)
                .ConfigureAwait(false);

    Assert.AreEqual(DataReply.TypeOneofCase.Error,
                    commonData.Messages.Single()
                              .TypeCase);

    var directData = new TestHelperServerStreamWriter<DataReply>();

    await holder.Agent.GetDirectData(new DataRequest
                                     {
                                       CommunicationToken = token,
                                     },
                                     directData,
                                     CancellationToken.None)
                .ConfigureAwait(false);

    Assert.AreEqual(DataReply.TypeOneofCase.Error,
                    directData.Messages.Single()
                              .TypeCase);

    var resourceData = new TestHelperServerStreamWriter<DataReply>();

    await holder.Agent.GetResourceData(new DataRequest
                                       {
                                         CommunicationToken = token,
                                       },
                                       resourceData,
                                       CancellationToken.None)
                .ConfigureAwait(false);

    Assert.AreEqual(DataReply.TypeOneofCase.Error,
                    resourceData.Messages.Single()
                                .TypeCase);
  }


  [Test]
  public async Task UnImplementedData()
  {
    using var holder = new AgentHolder();

    var commonData = new TestHelperServerStreamWriter<DataReply>();

    await holder.Agent.GetCommonData(new DataRequest
                                     {
                                       CommunicationToken = holder.Token,
                                     },
                                     commonData,
                                     CancellationToken.None)
                .ConfigureAwait(false);

    Assert.AreEqual(DataReply.TypeOneofCase.Error,
                    commonData.Messages.Single()
                              .TypeCase);

    var directData = new TestHelperServerStreamWriter<DataReply>();

    await holder.Agent.GetDirectData(new DataRequest
                                     {
                                       CommunicationToken = holder.Token,
                                     },
                                     directData,
                                     CancellationToken.None)
                .ConfigureAwait(false);

    Assert.AreEqual(DataReply.TypeOneofCase.Error,
                    directData.Messages.Single()
                              .TypeCase);
  }

  [Test]
  public async Task MissingResourceData()
  {
    using var holder       = new AgentHolder();
    var       resourceData = new TestHelperServerStreamWriter<DataReply>();

    await holder.Agent.GetResourceData(new DataRequest
                                       {
                                         CommunicationToken = holder.Token,
                                         Key                = "DataNotExisting",
                                       },
                                       resourceData,
                                       CancellationToken.None)
                .ConfigureAwait(false);

    Assert.AreEqual(DataReply.TypeOneofCase.Init,
                    resourceData.Messages.Single()
                                .TypeCase);
    Assert.AreEqual(DataReply.Types.Init.HasResultOneofCase.Error,
                    resourceData.Messages.Single()
                                .Init.HasResultCase);
    Assert.AreEqual("Key not found",
                    resourceData.Messages.Single()
                                .Init.Error);
  }

  [Test]
  public async Task SendResultShouldSucceed()
  {
    using var holder = new AgentHolder();

    Assert.IsFalse(holder.QueueStorage.Messages.Keys.Contains(DependencyResolverOptions.UnresolvedDependenciesQueue));
    Assert.AreEqual(0,
                    holder.QueueStorage.Messages.SelectMany(pair => pair.Value)
                          .Count());

    var resultReply = await holder.Agent.SendResult(new TestHelperAsyncStreamReader<Result>(new[]
                                                                                            {
                                                                                              new Result
                                                                                              {
                                                                                                CommunicationToken = holder.Token,
                                                                                                Init = new InitKeyedDataStream
                                                                                                       {
                                                                                                         Key = ExpectedOutput1,
                                                                                                       },
                                                                                              },
                                                                                              new Result
                                                                                              {
                                                                                                CommunicationToken = holder.Token,
                                                                                                Data = new DataChunk
                                                                                                       {
                                                                                                         Data = ByteString.CopyFromUtf8("Data1"),
                                                                                                       },
                                                                                              },
                                                                                              new Result
                                                                                              {
                                                                                                CommunicationToken = holder.Token,
                                                                                                Data = new DataChunk
                                                                                                       {
                                                                                                         Data = ByteString.CopyFromUtf8("Data2"),
                                                                                                       },
                                                                                              },
                                                                                              new Result
                                                                                              {
                                                                                                CommunicationToken = holder.Token,
                                                                                                Data = new DataChunk
                                                                                                       {
                                                                                                         DataComplete = true,
                                                                                                       },
                                                                                              },
                                                                                              new Result
                                                                                              {
                                                                                                CommunicationToken = holder.Token,
                                                                                                Init = new InitKeyedDataStream
                                                                                                       {
                                                                                                         LastResult = true,
                                                                                                       },
                                                                                              },
                                                                                            }),
                                                    CancellationToken.None)
                                  .ConfigureAwait(false);

    await holder.Agent.FinalizeTaskCreation(CancellationToken.None)
                .ConfigureAwait(false);

    Assert.AreEqual(ResultReply.TypeOneofCase.Ok,
                    resultReply.TypeCase);

    var resultData = await holder.ResultTable.GetResult(holder.Session,
                                                        ExpectedOutput1,
                                                        CancellationToken.None)
                                 .ConfigureAwait(false);

    Assert.AreEqual(ExpectedOutput1,
                    resultData.Name);
    Assert.AreEqual(ResultStatus.Completed,
                    resultData.Status);
    Assert.AreEqual(holder.TaskData.TaskId,
                    resultData.OwnerTaskId);
    Assert.Contains(holder.TaskWithDependencies1,
                    holder.QueueStorage.Messages[DependencyResolverOptions.UnresolvedDependenciesQueue]);
    Assert.Contains(holder.TaskWithDependencies2,
                    holder.QueueStorage.Messages[DependencyResolverOptions.UnresolvedDependenciesQueue]);
    Assert.AreEqual(2,
                    holder.QueueStorage.Messages[DependencyResolverOptions.UnresolvedDependenciesQueue]
                          .Count);
  }

  [Test]
  public async Task CreateTasksShouldSucceed()
  {
    using var holder = new AgentHolder();

    var requests = new[]
                   {
                     new CreateTaskRequest
                     {
                       CommunicationToken = holder.Token,
                       InitRequest = new CreateTaskRequest.Types.InitRequest
                                     {
                                       TaskOptions = Options,
                                     },
                     },
                     new CreateTaskRequest
                     {
                       CommunicationToken = holder.Token,
                       InitTask = new InitTaskRequest
                                  {
                                    Header = new TaskRequestHeader
                                             {
                                               DataDependencies =
                                               {
                                                 "Task1DD",
                                               },
                                               ExpectedOutputKeys =
                                               {
                                                 "Task1EOK",
                                               },
                                             },
                                  },
                     },
                     new CreateTaskRequest
                     {
                       CommunicationToken = holder.Token,
                       TaskPayload = new DataChunk
                                     {
                                       Data = ByteString.CopyFromUtf8("Task1Data1"),
                                     },
                     },
                     new CreateTaskRequest
                     {
                       CommunicationToken = holder.Token,
                       TaskPayload = new DataChunk
                                     {
                                       Data = ByteString.CopyFromUtf8("Task1Data2"),
                                     },
                     },
                     new CreateTaskRequest
                     {
                       CommunicationToken = holder.Token,
                       TaskPayload = new DataChunk
                                     {
                                       DataComplete = true,
                                     },
                     },
                     new CreateTaskRequest
                     {
                       CommunicationToken = holder.Token,
                       InitTask = new InitTaskRequest
                                  {
                                    Header = new TaskRequestHeader
                                             {
                                               DataDependencies =
                                               {
                                                 "Task1DD",
                                               },
                                               ExpectedOutputKeys =
                                               {
                                                 "Task2EOK",
                                               },
                                             },
                                  },
                     },
                     new CreateTaskRequest
                     {
                       CommunicationToken = holder.Token,
                       TaskPayload = new DataChunk
                                     {
                                       Data = ByteString.CopyFromUtf8("Task2Data1"),
                                     },
                     },
                     new CreateTaskRequest
                     {
                       CommunicationToken = holder.Token,
                       TaskPayload = new DataChunk
                                     {
                                       Data = ByteString.CopyFromUtf8("Task2Data2"),
                                     },
                     },
                     new CreateTaskRequest
                     {
                       CommunicationToken = holder.Token,
                       TaskPayload = new DataChunk
                                     {
                                       DataComplete = true,
                                     },
                     },
                     new CreateTaskRequest
                     {
                       CommunicationToken = holder.Token,
                       InitTask = new InitTaskRequest
                                  {
                                    LastTask = true,
                                  },
                     },
                   };

    var createTaskReply = await holder.Agent.CreateTask(new TestHelperAsyncStreamReader<CreateTaskRequest>(requests),
                                                        CancellationToken.None)
                                      .ConfigureAwait(false);


    Assert.AreEqual(CreateTaskReply.ResponseOneofCase.CreationStatusList,
                    createTaskReply.ResponseCase);

    Assert.AreEqual(0,
                    createTaskReply.CreationStatusList.CreationStatuses.Count(cs => cs.StatusCase == CreateTaskReply.Types.CreationStatus.StatusOneofCase.Error));

    Assert.AreEqual(2,
                    createTaskReply.CreationStatusList.CreationStatuses.Count(cs => cs.StatusCase == CreateTaskReply.Types.CreationStatus.StatusOneofCase.TaskInfo));


    var requests2 = new[]
                    {
                      new CreateTaskRequest
                      {
                        CommunicationToken = holder.Token,
                        InitRequest = new CreateTaskRequest.Types.InitRequest
                                      {
                                        TaskOptions = Options,
                                      },
                      },
                      new CreateTaskRequest
                      {
                        CommunicationToken = holder.Token,
                        InitTask = new InitTaskRequest
                                   {
                                     Header = new TaskRequestHeader
                                              {
                                                DataDependencies =
                                                {
                                                  "Task1DD",
                                                },
                                                ExpectedOutputKeys =
                                                {
                                                  "Task3EOK",
                                                },
                                              },
                                   },
                      },
                      new CreateTaskRequest
                      {
                        CommunicationToken = holder.Token,
                        TaskPayload = new DataChunk
                                      {
                                        Data = ByteString.CopyFromUtf8("Task3Data1"),
                                      },
                      },
                      new CreateTaskRequest
                      {
                        CommunicationToken = holder.Token,
                        TaskPayload = new DataChunk
                                      {
                                        Data = ByteString.CopyFromUtf8("Task3Data2"),
                                      },
                      },
                      new CreateTaskRequest
                      {
                        CommunicationToken = holder.Token,
                        TaskPayload = new DataChunk
                                      {
                                        DataComplete = true,
                                      },
                      },
                      new CreateTaskRequest
                      {
                        CommunicationToken = holder.Token,
                        InitTask = new InitTaskRequest
                                   {
                                     LastTask = true,
                                   },
                      },
                    };

    var createTaskReply2 = await holder.Agent.CreateTask(new TestHelperAsyncStreamReader<CreateTaskRequest>(requests2),
                                                         CancellationToken.None)
                                       .ConfigureAwait(false);

    Assert.AreEqual(CreateTaskReply.ResponseOneofCase.CreationStatusList,
                    createTaskReply2.ResponseCase);

    Assert.AreEqual(0,
                    createTaskReply2.CreationStatusList.CreationStatuses.Count(cs => cs.StatusCase == CreateTaskReply.Types.CreationStatus.StatusOneofCase.Error));

    Assert.AreEqual(1,
                    createTaskReply2.CreationStatusList.CreationStatuses.Count(cs => cs.StatusCase == CreateTaskReply.Types.CreationStatus.StatusOneofCase.TaskInfo));

    var taskId3 = createTaskReply2.CreationStatusList.CreationStatuses.Single()
                                  .TaskInfo.TaskId;

    var taskData3 = await holder.TaskTable.ReadTaskAsync(taskId3,
                                                         CancellationToken.None)
                                .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Creating,
                    taskData3.Status);


    await holder.Agent.FinalizeTaskCreation(CancellationToken.None)
                .ConfigureAwait(false);

    // First insertion is always in UnresolvedDependenciesQueue when task has dependencies
    Assert.AreEqual(3,
                    holder.QueueStorage.Messages[DependencyResolverOptions.UnresolvedDependenciesQueue]
                          .Count);

    taskData3 = await holder.TaskTable.ReadTaskAsync(taskId3,
                                                     CancellationToken.None)
                            .ConfigureAwait(false);

    // tasks with dependencies are put in the UnresolvedDependenciesQueue
    // they are put in the regular queue as submitted when their dependencies are available
    Assert.AreEqual(TaskStatus.Creating,
                    taskData3.Status);
  }

  [Test]
  public async Task GetResourceDataShouldSucceed()
  {
    using var holder       = new AgentHolder();
    var       resourceData = new TestHelperServerStreamWriter<DataReply>();

    await holder.ResourceStorage.AddOrUpdateAsync("ResourceData",
                                                  new List<byte[]>
                                                    {
                                                      Encoding.ASCII.GetBytes("Data1"),
                                                      Encoding.ASCII.GetBytes("Data2"),
                                                    }.Select(bytes => new ReadOnlyMemory<byte>(bytes))
                                                     .ToAsyncEnumerable(),
                                                  CancellationToken.None)
                .ConfigureAwait(false);

    await holder.Agent.GetResourceData(new DataRequest
                                       {
                                         CommunicationToken = holder.Token,
                                         Key                = "ResourceData",
                                       },
                                       resourceData,
                                       CancellationToken.None)
                .ConfigureAwait(false);

    foreach (var message in resourceData.Messages)
    {
      Console.WriteLine(message);
    }

    Assert.AreEqual(3,
                    resourceData.Messages.Count);
    Assert.AreEqual("Data1",
                    resourceData.Messages[0]
                                .Data.Data);
    Assert.AreEqual("Data2",
                    resourceData.Messages[1]
                                .Data.Data);
    Assert.IsTrue(resourceData.Messages[2]
                              .Data.DataComplete);
  }
}
