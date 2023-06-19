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
using ArmoniK.Core.Base;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.Helpers;
using ArmoniK.Utils;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using NUnit.Framework;

using Agent = ArmoniK.Core.Common.gRPC.Services.Agent;
using Result = ArmoniK.Core.Common.Storage.Result;
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
  private const string DataDependency1 = "Task1DD";
  private const string DataDependency2 = "Task2DD";

  private static readonly TaskOptions Options = new()
                                                {
                                                  ApplicationName      = "applicationName",
                                                  ApplicationNamespace = "applicationNamespace",
                                                  ApplicationVersion   = "applicationVersion",
                                                  ApplicationService   = "applicationService",
                                                  EngineType           = "engineType",
                                                  PartitionId          = Partition,
                                                  MaxDuration          = Duration.FromTimeSpan(TimeSpan.FromMinutes(10)),
                                                  MaxRetries           = 5,
                                                  Priority             = 1,
                                                  Options =
                                                  {
                                                    {
                                                      "key1", "val1"
                                                    },
                                                    {
                                                      "key2", "val2"
                                                    },
                                                  },
                                                };

  private static readonly Injection.Options.Submitter SubmitterOptions = new()
                                                                         {
                                                                           DefaultPartition = Partition,
                                                                           MaxErrorAllowed  = -1,
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
    public readonly  IObjectStorage       ObjectStorage;
    private readonly TestDatabaseProvider prov_;
    public readonly  MyPushQueueStorage   QueueStorage;
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
                                                               .AddSingleton<IPushQueueStorage>(QueueStorage));

      ResultTable   = prov_.GetRequiredService<IResultTable>();
      TaskTable     = prov_.GetRequiredService<ITaskTable>();
      ObjectStorage = prov_.GetRequiredService<IObjectStorage>();

      var sessionTable = prov_.GetRequiredService<ISessionTable>();
      var submitter    = prov_.GetRequiredService<ISubmitter>();

      Session = sessionTable.SetSessionDataAsync(new[]
                                                 {
                                                   Partition,
                                                 },
                                                 Options.ToTaskOptions(),
                                                 CancellationToken.None)
                            .Result;

      var sessionData = sessionTable.GetSessionAsync(Session,
                                                     CancellationToken.None)
                                    .Result;

      ResultTable.Create(new[]
                         {
                           new Result(sessionData.SessionId,
                                      DataDependency1,
                                      "",
                                      "",
                                      ResultStatus.Completed,
                                      new List<string>(),
                                      DateTime.UtcNow,
                                      Array.Empty<byte>()),
                           new Result(sessionData.SessionId,
                                      DataDependency2,
                                      "",
                                      "",
                                      ResultStatus.Completed,
                                      new List<string>(),
                                      DateTime.UtcNow,
                                      Array.Empty<byte>()),
                           new Result(Session,
                                      ExpectedOutput1,
                                      "",
                                      "",
                                      ResultStatus.Created,
                                      new List<string>(),
                                      DateTime.UtcNow,
                                      Array.Empty<byte>()),
                           new Result(Session,
                                      ExpectedOutput2,
                                      "",
                                      "",
                                      ResultStatus.Created,
                                      new List<string>(),
                                      DateTime.UtcNow,
                                      Array.Empty<byte>()),
                         },
                         CancellationToken.None)
                 .Wait();

      var createdTasks = submitter.CreateTasks(Session,
                                               Session,
                                               Options.ToTaskOptions(),
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

      submitter.FinalizeTaskCreation(createdTasks,
                                     Session,
                                     Session,
                                     CancellationToken.None)
               .Wait();

      QueueStorage.Messages.Clear();

      TaskData = TaskTable.ReadTaskAsync(createdTasks.Single()
                                                     .TaskId,
                                         CancellationToken.None)
                          .Result;

      var createdTasks2 = submitter.CreateTasks(Session,
                                                Session,
                                                Options.ToTaskOptions(),
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

      TaskWithDependencies1 = createdTasks2.First()
                                           .TaskId;
      TaskWithDependencies2 = createdTasks2.Last()
                                           .TaskId;

      submitter.FinalizeTaskCreation(createdTasks2,
                                     Session,
                                     Session,
                                     CancellationToken.None)
               .Wait();

      Token = Guid.NewGuid()
                  .ToString();

      Agent = new Agent(submitter,
                        ObjectStorage,
                        QueueStorage,
                        ResultTable,
                        TaskTable,
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

    var resultReply = await holder.Agent.SendResult(new TestHelperAsyncStreamReader<Api.gRPC.V1.Agent.Result>(new[]
                                                                                                              {
                                                                                                                new Api.gRPC.V1.Agent.Result
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

    Assert.AreEqual(0,
                    holder.QueueStorage.Messages.SelectMany(pair => pair.Value)
                          .Count());

    var resultReply = await holder.Agent.SendResult(new TestHelperAsyncStreamReader<Api.gRPC.V1.Agent.Result>(new[]
                                                                                                              {
                                                                                                                new Api.gRPC.V1.Agent.Result
                                                                                                                {
                                                                                                                  CommunicationToken = holder.Token,
                                                                                                                  Init = new InitKeyedDataStream
                                                                                                                         {
                                                                                                                           Key = ExpectedOutput1,
                                                                                                                         },
                                                                                                                },
                                                                                                                new Api.gRPC.V1.Agent.Result
                                                                                                                {
                                                                                                                  CommunicationToken = holder.Token,
                                                                                                                  Data = new DataChunk
                                                                                                                         {
                                                                                                                           Data = ByteString.CopyFromUtf8("Data1"),
                                                                                                                         },
                                                                                                                },
                                                                                                                new Api.gRPC.V1.Agent.Result
                                                                                                                {
                                                                                                                  CommunicationToken = holder.Token,
                                                                                                                  Data = new DataChunk
                                                                                                                         {
                                                                                                                           Data = ByteString.CopyFromUtf8("Data2"),
                                                                                                                         },
                                                                                                                },
                                                                                                                new Api.gRPC.V1.Agent.Result
                                                                                                                {
                                                                                                                  CommunicationToken = holder.Token,
                                                                                                                  Data = new DataChunk
                                                                                                                         {
                                                                                                                           DataComplete = true,
                                                                                                                         },
                                                                                                                },
                                                                                                                new Api.gRPC.V1.Agent.Result
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
                    resultData.ResultId);
    Assert.AreEqual(ResultStatus.Completed,
                    resultData.Status);
    Assert.AreEqual(holder.TaskData.TaskId,
                    resultData.OwnerTaskId);

    var dependents = await holder.ResultTable.GetDependents(holder.Session,
                                                            ExpectedOutput1,
                                                            CancellationToken.None)
                                 .ToListAsync()
                                 .ConfigureAwait(false);

    Assert.Contains(holder.TaskWithDependencies1,
                    dependents);
    Assert.Contains(holder.TaskWithDependencies2,
                    dependents);

    Assert.Contains(holder.TaskWithDependencies1,
                    holder.QueueStorage.Messages[Partition]);
    Assert.Contains(holder.TaskWithDependencies2,
                    holder.QueueStorage.Messages[Partition]);
    Assert.AreEqual(2,
                    holder.QueueStorage.Messages[Partition]
                          .Count);

    var taskData1 = await holder.TaskTable.ReadTaskAsync(holder.TaskWithDependencies1,
                                                         CancellationToken.None)
                                .ConfigureAwait(false);

    Assert.Contains(ExpectedOutput1,
                    taskData1.DataDependencies.ToList());
    Assert.IsEmpty(taskData1.RemainingDataDependencies);
    Assert.AreEqual(TaskStatus.Submitted,
                    taskData1.Status);

    var taskData2 = await holder.TaskTable.ReadTaskAsync(holder.TaskWithDependencies2,
                                                         CancellationToken.None)
                                .ConfigureAwait(false);

    Assert.Contains(ExpectedOutput1,
                    taskData2.DataDependencies.ToList());
    Assert.IsEmpty(taskData2.RemainingDataDependencies);
    Assert.AreEqual(TaskStatus.Submitted,
                    taskData2.Status);
  }

  /// <summary>
  ///   Create one task per result
  /// </summary>
  /// <param name="token">Communication token</param>
  /// <param name="options">Task options</param>
  /// <param name="results">Results to build task creation requests</param>
  /// <param name="payloadChunksPerTask"></param>
  /// <returns></returns>
  private static IEnumerable<CreateTaskRequest> GenerateCreateTaskRequest(string              token,
                                                                          TaskOptions?        options,
                                                                          IEnumerable<string> results,
                                                                          int                 payloadChunksPerTask)
  {
    yield return new CreateTaskRequest
                 {
                   CommunicationToken = token,
                   InitRequest = new CreateTaskRequest.Types.InitRequest
                                 {
                                   TaskOptions = options,
                                 },
                 };


    foreach (var result in results)
    {
      yield return new CreateTaskRequest
                   {
                     CommunicationToken = token,
                     InitTask = new InitTaskRequest
                                {
                                  Header = new TaskRequestHeader
                                           {
                                             DataDependencies =
                                             {
                                               DataDependency1,
                                             },
                                             ExpectedOutputKeys =
                                             {
                                               result,
                                             },
                                           },
                                },
                   };


      for (var j = 0; j < payloadChunksPerTask; j++)
      {
        yield return new CreateTaskRequest
                     {
                       CommunicationToken = token,
                       TaskPayload = new DataChunk
                                     {
                                       Data = ByteString.CopyFromUtf8($"Task1Data{j}"),
                                     },
                     };
      }

      yield return new CreateTaskRequest
                   {
                     CommunicationToken = token,
                     TaskPayload = new DataChunk
                                   {
                                     DataComplete = true,
                                   },
                   };
    }

    yield return new CreateTaskRequest
                 {
                   CommunicationToken = token,
                   InitTask = new InitTaskRequest
                              {
                                LastTask = true,
                              },
                 };
  }

  [Test]
  public async Task CreateTasksShouldSucceed()
  {
    using var holder = new AgentHolder();

    var results = await holder.Agent.CreateResultsMetaData(new CreateResultsMetaDataRequest
                                                           {
                                                             CommunicationToken = holder.Token,
                                                             SessionId          = holder.Session,
                                                             Results =
                                                             {
                                                               new CreateResultsMetaDataRequest.Types.ResultCreate
                                                               {
                                                                 Name = "Task1EOK",
                                                               },
                                                               new CreateResultsMetaDataRequest.Types.ResultCreate
                                                               {
                                                                 Name = "Task2EOK",
                                                               },
                                                             },
                                                           },
                                                           CancellationToken.None)
                              .ConfigureAwait(false);

    var createTaskReply = await holder.Agent.CreateTask(new TestHelperAsyncStreamReader<CreateTaskRequest>(GenerateCreateTaskRequest(holder.Token,
                                                                                                                                     Options,
                                                                                                                                     results.Results
                                                                                                                                            .Select(r => r.ResultId),
                                                                                                                                     2)),
                                                        CancellationToken.None)
                                      .ConfigureAwait(false);


    Assert.AreEqual(CreateTaskReply.ResponseOneofCase.CreationStatusList,
                    createTaskReply.ResponseCase);

    Assert.AreEqual(0,
                    createTaskReply.CreationStatusList.CreationStatuses.Count(cs => cs.StatusCase == CreateTaskReply.Types.CreationStatus.StatusOneofCase.Error));

    Assert.AreEqual(2,
                    createTaskReply.CreationStatusList.CreationStatuses.Count(cs => cs.StatusCase == CreateTaskReply.Types.CreationStatus.StatusOneofCase.TaskInfo));


    var results2 = await holder.Agent.CreateResultsMetaData(new CreateResultsMetaDataRequest
                                                            {
                                                              CommunicationToken = holder.Token,
                                                              SessionId          = holder.Session,
                                                              Results =
                                                              {
                                                                new CreateResultsMetaDataRequest.Types.ResultCreate
                                                                {
                                                                  Name = "Task3EOK",
                                                                },
                                                              },
                                                            },
                                                            CancellationToken.None)
                               .ConfigureAwait(false);


    var createTaskReply2 = await holder.Agent.CreateTask(new TestHelperAsyncStreamReader<CreateTaskRequest>(GenerateCreateTaskRequest(holder.Token,
                                                                                                                                      Options,
                                                                                                                                      results2.Results
                                                                                                                                              .Select(r => r.ResultId),
                                                                                                                                      2)),
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

    Assert.AreEqual(3,
                    holder.QueueStorage.Messages[Partition]
                          .Count);

    taskData3 = await holder.TaskTable.ReadTaskAsync(taskId3,
                                                     CancellationToken.None)
                            .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Submitted,
                    taskData3.Status);
  }

  [Test]
  [TestCase(false)]
  [TestCase(true)]
  public async Task CreateLotsOfTasksShouldSucceed(bool optionsNull)
  {
    using var holder = new AgentHolder();

    var results = await holder.Agent.CreateResultsMetaData(new CreateResultsMetaDataRequest
                                                           {
                                                             CommunicationToken = holder.Token,
                                                             SessionId          = holder.Session,
                                                             Results =
                                                             {
                                                               Enumerable.Range(0,
                                                                                200)
                                                                         .Select(i => new CreateResultsMetaDataRequest.Types.ResultCreate
                                                                                      {
                                                                                        Name = $"Task{i}EOK",
                                                                                      }),
                                                             },
                                                           },
                                                           CancellationToken.None)
                              .ConfigureAwait(false);

    var createTaskReply = await holder.Agent.CreateTask(new TestHelperAsyncStreamReader<CreateTaskRequest>(GenerateCreateTaskRequest(holder.Token,
                                                                                                                                     optionsNull
                                                                                                                                       ? null
                                                                                                                                       : Options,
                                                                                                                                     results.Results
                                                                                                                                            .Select(r => r.ResultId),
                                                                                                                                     2)),
                                                        CancellationToken.None)
                                      .ConfigureAwait(false);


    Assert.AreEqual(CreateTaskReply.ResponseOneofCase.CreationStatusList,
                    createTaskReply.ResponseCase);

    Assert.AreEqual(0,
                    createTaskReply.CreationStatusList.CreationStatuses.Count(cs => cs.StatusCase == CreateTaskReply.Types.CreationStatus.StatusOneofCase.Error));

    Assert.AreEqual(200,
                    createTaskReply.CreationStatusList.CreationStatuses.Count(cs => cs.StatusCase == CreateTaskReply.Types.CreationStatus.StatusOneofCase.TaskInfo));

    await holder.Agent.FinalizeTaskCreation(CancellationToken.None)
                .ConfigureAwait(false);
  }

  [Test]
  public async Task GetResourceDataShouldSucceed()
  {
    using var holder       = new AgentHolder();
    var       resourceData = new TestHelperServerStreamWriter<DataReply>();

    await holder.ObjectStorage.AddOrUpdateAsync("ResourceData",
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

  [Test]
  public async Task CreateResultsShouldSucceed()
  {
    using var holder = new AgentHolder();

    var results = await holder.Agent.CreateResults(new CreateResultsRequest
                                                   {
                                                     SessionId          = holder.Session,
                                                     CommunicationToken = holder.Token,
                                                     Results =
                                                     {
                                                       new List<CreateResultsRequest.Types.ResultCreate>
                                                       {
                                                         new()
                                                         {
                                                           Name = "Result1",
                                                           Data = ByteString.CopyFromUtf8("Result1"),
                                                         },
                                                         new()
                                                         {
                                                           Name = "Result2",
                                                           Data = ByteString.CopyFromUtf8("Result2"),
                                                         },
                                                       },
                                                     },
                                                   },
                                                   CancellationToken.None)
                              .ConfigureAwait(false);

    foreach (var result in results.Results)
    {
      Console.WriteLine(result);

      var resultMetadata = await holder.ResultTable.GetResult(holder.Session,
                                                              result.ResultId,
                                                              CancellationToken.None)
                                       .ConfigureAwait(false);

      Assert.AreEqual(result.Name,
                      resultMetadata.Name);
      Assert.AreEqual(ResultStatus.Created,
                      resultMetadata.Status);

      var bytes = (await holder.ObjectStorage.GetValuesAsync(result.ResultId)
                               .ToListAsync()
                               .ConfigureAwait(false)).Single();

      Assert.AreEqual(ByteString.CopyFromUtf8(result.Name)
                                .ToByteArray(),
                      bytes);
    }

    await holder.Agent.FinalizeTaskCreation(CancellationToken.None)
                .ConfigureAwait(false);

    foreach (var result in results.Results)
    {
      Console.WriteLine(result);

      var resultMetadata = await holder.ResultTable.GetResult(holder.Session,
                                                              result.ResultId,
                                                              CancellationToken.None)
                                       .ConfigureAwait(false);

      Assert.AreEqual(result.Name,
                      resultMetadata.Name);
      Assert.AreEqual(ResultStatus.Completed,
                      resultMetadata.Status);
    }
  }


  [Test]
  [TestCase(false)]
  [TestCase(true)]
  public async Task SubmitTasksCreateOneRequestShouldSucceed(bool optionsNull)
  {
    using var holder = new AgentHolder();

    var payload = await holder.Agent.CreateResults(new CreateResultsRequest
                                                   {
                                                     SessionId          = holder.Session,
                                                     CommunicationToken = holder.Token,
                                                     Results =
                                                     {
                                                       new List<CreateResultsRequest.Types.ResultCreate>
                                                       {
                                                         new()
                                                         {
                                                           Name = "Payload",
                                                           Data = ByteString.CopyFromUtf8("Payload"),
                                                         },
                                                       },
                                                     },
                                                   },
                                                   CancellationToken.None)
                              .ConfigureAwait(false);

    var eok = await holder.Agent.CreateResultsMetaData(new CreateResultsMetaDataRequest
                                                       {
                                                         SessionId          = holder.Session,
                                                         CommunicationToken = holder.Token,
                                                         Results =
                                                         {
                                                           new CreateResultsMetaDataRequest.Types.ResultCreate
                                                           {
                                                             Name = "EOK1",
                                                           },
                                                           new CreateResultsMetaDataRequest.Types.ResultCreate
                                                           {
                                                             Name = "EOK2",
                                                           },
                                                         },
                                                       },
                                                       CancellationToken.None)
                          .ConfigureAwait(false);

    var reply = await holder.Agent.SubmitTasks(new SubmitTasksRequest
                                               {
                                                 CommunicationToken = holder.Token,
                                                 SessionId          = holder.Session,
                                                 TaskCreations =
                                                 {
                                                   new SubmitTasksRequest.Types.TaskCreation
                                                   {
                                                     ExpectedOutputKeys =
                                                     {
                                                       eok.Results.Select(r => r.ResultId),
                                                     },
                                                     PayloadId = payload.Results.Single()
                                                                        .ResultId,
                                                     TaskOptions = optionsNull
                                                                     ? null
                                                                     : Options,
                                                   },
                                                 },
                                                 TaskOptions = optionsNull
                                                                 ? null
                                                                 : Options,
                                               },
                                               CancellationToken.None)
                            .ConfigureAwait(false);

    Assert.AreEqual(1,
                    reply.TaskInfos.Count);
    Assert.AreEqual(payload.Results.Single()
                           .ResultId,
                    reply.TaskInfos.Single()
                         .PayloadId);
    foreach (var eokResult in eok.Results)
    {
      Assert.Contains(eokResult.ResultId,
                      reply.TaskInfos.Single()
                           .ExpectedOutputIds);
    }

    await holder.Agent.FinalizeTaskCreation(CancellationToken.None)
                .ConfigureAwait(false);


    var taskData = await holder.TaskTable.ReadTaskAsync(reply.TaskInfos.Single()
                                                             .TaskId,
                                                        CancellationToken.None)
                               .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Submitted,
                    taskData.Status);
  }


  [Test]
  [TestCase(false)]
  [TestCase(true)]
  public async Task SubmitTasksUploadPayloadShouldSucceed(bool optionsNull)
  {
    using var holder = new AgentHolder();

    var eok = await holder.Agent.CreateResultsMetaData(new CreateResultsMetaDataRequest
                                                       {
                                                         SessionId          = holder.Session,
                                                         CommunicationToken = holder.Token,
                                                         Results =
                                                         {
                                                           new CreateResultsMetaDataRequest.Types.ResultCreate
                                                           {
                                                             Name = "EOK1",
                                                           },
                                                           new CreateResultsMetaDataRequest.Types.ResultCreate
                                                           {
                                                             Name = "EOK2",
                                                           },
                                                           new CreateResultsMetaDataRequest.Types.ResultCreate
                                                           {
                                                             Name = "Payload",
                                                           },
                                                         },
                                                       },
                                                       CancellationToken.None)
                          .ConfigureAwait(false);

    var reply = await holder.Agent.SubmitTasks(new SubmitTasksRequest
                                               {
                                                 CommunicationToken = holder.Token,
                                                 SessionId          = holder.Session,
                                                 TaskCreations =
                                                 {
                                                   new SubmitTasksRequest.Types.TaskCreation
                                                   {
                                                     ExpectedOutputKeys =
                                                     {
                                                       eok.Results.Select(r => r.ResultId)
                                                          .SkipLast(1),
                                                     },
                                                     PayloadId = eok.Results.Last()
                                                                    .ResultId,
                                                     TaskOptions = optionsNull
                                                                     ? null
                                                                     : Options,
                                                   },
                                                 },
                                                 TaskOptions = optionsNull
                                                                 ? null
                                                                 : Options,
                                               },
                                               CancellationToken.None)
                            .ConfigureAwait(false);


    await holder.Agent.UploadResultData(new TestHelperAsyncStreamReader<UploadResultDataRequest>(new List<UploadResultDataRequest>
                                                                                                 {
                                                                                                   new()
                                                                                                   {
                                                                                                     CommunicationToken = holder.Token,
                                                                                                     Id = new UploadResultDataRequest.Types.ResultIdentifier
                                                                                                          {
                                                                                                            ResultId = eok.Results.Last()
                                                                                                                          .ResultId,
                                                                                                            SessionId = holder.Session,
                                                                                                          },
                                                                                                   },
                                                                                                   new()
                                                                                                   {
                                                                                                     CommunicationToken = holder.Token,
                                                                                                     DataChunk          = ByteString.CopyFromUtf8("DataPart1"),
                                                                                                   },
                                                                                                   new()
                                                                                                   {
                                                                                                     CommunicationToken = holder.Token,
                                                                                                     DataChunk          = ByteString.CopyFromUtf8("DataPart2"),
                                                                                                   },
                                                                                                 }),
                                        CancellationToken.None)
                .ConfigureAwait(false);

    Assert.AreEqual(1,
                    reply.TaskInfos.Count);
    Assert.AreEqual(eok.Results.Last()
                       .ResultId,
                    reply.TaskInfos.Single()
                         .PayloadId);

    foreach (var eokResult in eok.Results.SkipLast(1))
    {
      Assert.Contains(eokResult.ResultId,
                      reply.TaskInfos.Single()
                           .ExpectedOutputIds);
    }

    var uploadedResultData = await holder.ResultTable.GetResult(holder.Session,
                                                                eok.Results.Last()
                                                                   .ResultId)
                                         .ConfigureAwait(false);

    Assert.AreEqual(ResultStatus.Created,
                    uploadedResultData.Status);

    var taskData = await holder.TaskTable.ReadTaskAsync(reply.TaskInfos.Single()
                                                             .TaskId,
                                                        CancellationToken.None)
                               .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Creating,
                    taskData.Status);

    await holder.Agent.FinalizeTaskCreation(CancellationToken.None)
                .ConfigureAwait(false);

    uploadedResultData = await holder.ResultTable.GetResult(holder.Session,
                                                            eok.Results.Last()
                                                               .ResultId)
                                     .ConfigureAwait(false);

    Assert.AreEqual(ResultStatus.Completed,
                    uploadedResultData.Status);

    taskData = await holder.TaskTable.ReadTaskAsync(reply.TaskInfos.Single()
                                                         .TaskId,
                                                    CancellationToken.None)
                           .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Submitted,
                    taskData.Status);

    /*
     *
     * TODO : payload availability is not checked before putting the task in submitted status
     * 1/ We need to check the availability
     * 2/ We need to write a test that upload the data for the payload afer task finalization and check that the task status changes from creating to submitted
     *
     */
  }
}
