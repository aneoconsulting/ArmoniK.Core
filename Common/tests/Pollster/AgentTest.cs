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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Agent;
using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.gRPC.Convertors;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.Helpers;
using ArmoniK.Utils;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using NUnit.Framework;

using Agent = ArmoniK.Core.Common.gRPC.Services.Agent;
using TaskOptions = ArmoniK.Api.gRPC.V1.TaskOptions;
using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

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

    /// <inheritdoc />
    public Task PushMessagesAsync(IEnumerable<MessageData> messages,
                                  string                   partitionId,
                                  CancellationToken        cancellationToken = default)
    {
      var partitionMessages = Messages.GetOrAdd(partitionId,
                                                _ => new ConcurrentBag<string>());
      foreach (var msgData in messages)
      {
        partitionMessages.Add(msgData.TaskId);
      }

      return Task.CompletedTask;
    }
  }

  private class AgentHolder : IDisposable
  {
    public readonly  IAgent               Agent;
    public readonly  string               Folder;
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
                                      0,
                                      Array.Empty<byte>()),
                           new Result(sessionData.SessionId,
                                      DataDependency2,
                                      "",
                                      "",
                                      ResultStatus.Completed,
                                      new List<string>(),
                                      DateTime.UtcNow,
                                      0,
                                      Array.Empty<byte>()),
                           new Result(Session,
                                      ExpectedOutput1,
                                      "",
                                      "",
                                      ResultStatus.Created,
                                      new List<string>(),
                                      DateTime.UtcNow,
                                      0,
                                      Array.Empty<byte>()),
                           new Result(Session,
                                      ExpectedOutput2,
                                      "",
                                      "",
                                      ResultStatus.Created,
                                      new List<string>(),
                                      DateTime.UtcNow,
                                      0,
                                      Array.Empty<byte>()),
                         },
                         CancellationToken.None)
                 .Wait();

      Folder = Path.Combine(Path.GetTempPath(),
                            "data");
      Directory.CreateDirectory(Folder);

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
                        Folder,
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
  public void WrongTokens(string token)
  {
    using var holder = new AgentHolder();

    Assert.ThrowsAsync<RpcException>(() => holder.Agent.SubmitTasks(new SubmitTasksRequest
                                                                    {
                                                                      CommunicationToken = token,
                                                                    },
                                                                    CancellationToken.None));

    Assert.ThrowsAsync<RpcException>(() => holder.Agent.CreateResultsMetaData(new CreateResultsMetaDataRequest
                                                                              {
                                                                                CommunicationToken = token,
                                                                              },
                                                                              CancellationToken.None));

    Assert.ThrowsAsync<RpcException>(() => holder.Agent.CreateResults(new CreateResultsRequest
                                                                      {
                                                                        CommunicationToken = token,
                                                                      },
                                                                      CancellationToken.None));

    Assert.ThrowsAsync<RpcException>(() => holder.Agent.NotifyResultData(new NotifyResultDataRequest
                                                                         {
                                                                           CommunicationToken = token,
                                                                         },
                                                                         CancellationToken.None));

    Assert.ThrowsAsync<RpcException>(() => holder.Agent.GetCommonData(new DataRequest
                                                                      {
                                                                        CommunicationToken = token,
                                                                      },
                                                                      CancellationToken.None));

    Assert.ThrowsAsync<RpcException>(() => holder.Agent.GetDirectData(new DataRequest
                                                                      {
                                                                        CommunicationToken = token,
                                                                      },
                                                                      CancellationToken.None));

    Assert.ThrowsAsync<RpcException>(() => holder.Agent.GetResourceData(new DataRequest
                                                                        {
                                                                          CommunicationToken = token,
                                                                        },
                                                                        CancellationToken.None));
  }


  [Test]
  public void UnImplementedData()
  {
    using var holder = new AgentHolder();

    Assert.ThrowsAsync<NotImplementedException>(() => holder.Agent.GetCommonData(new DataRequest
                                                                                 {
                                                                                   CommunicationToken = holder.Token,
                                                                                 },
                                                                                 CancellationToken.None));

    Assert.ThrowsAsync<NotImplementedException>(() => holder.Agent.GetDirectData(new DataRequest
                                                                                 {
                                                                                   CommunicationToken = holder.Token,
                                                                                 },
                                                                                 CancellationToken.None));
  }

  [Test]
  public void MissingResourceData()
  {
    using var holder = new AgentHolder();

    Assert.ThrowsAsync<RpcException>(() => holder.Agent.GetResourceData(new DataRequest
                                                                        {
                                                                          CommunicationToken = holder.Token,
                                                                          ResultId           = "DataNotExisting",
                                                                        },
                                                                        CancellationToken.None));
  }

  [Test]
  public async Task SendResultShouldSucceed()
  {
    using var holder = new AgentHolder();

    Assert.AreEqual(0,
                    holder.QueueStorage.Messages.SelectMany(pair => pair.Value)
                          .Count());

    var data = "Data1Data2";
    await File.WriteAllBytesAsync(Path.Combine(holder.Folder,
                                               ExpectedOutput1),
                                  Encoding.ASCII.GetBytes(data))
              .ConfigureAwait(false);

    await holder.Agent.NotifyResultData(new NotifyResultDataRequest
                                        {
                                          CommunicationToken = holder.Token,
                                          Ids =
                                          {
                                            new NotifyResultDataRequest.Types.ResultIdentifier
                                            {
                                              ResultId  = ExpectedOutput1,
                                              SessionId = holder.Session,
                                            },
                                          },
                                        },
                                        CancellationToken.None)
                .ConfigureAwait(false);

    var datAsyncEnumerable = holder.ObjectStorage.GetValuesAsync(ExpectedOutput1,
                                                                 CancellationToken.None);

    var dataStored = await datAsyncEnumerable.SingleAsync(CancellationToken.None)
                                             .ConfigureAwait(false);

    Assert.AreEqual(data,
                    dataStored);

    await holder.Agent.FinalizeTaskCreation(CancellationToken.None)
                .ConfigureAwait(false);

    var resultData = await holder.ResultTable.GetResult(ExpectedOutput1,
                                                        CancellationToken.None)
                                 .ConfigureAwait(false);

    Assert.AreEqual(ExpectedOutput1,
                    resultData.ResultId);
    Assert.AreEqual(ResultStatus.Completed,
                    resultData.Status);
    Assert.AreEqual(holder.TaskData.TaskId,
                    resultData.OwnerTaskId);
    Assert.AreEqual(data.Length,
                    resultData.Size);

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

  private static async Task<SubmitTasksResponse> CreatePayloadAndSubmit(IAgent              agent,
                                                                        TaskOptions?        options,
                                                                        ICollection<string> results)
  {
    var res = await agent.CreateResults(new CreateResultsRequest
                                        {
                                          SessionId          = agent.SessionId,
                                          CommunicationToken = agent.Token,
                                          Results =
                                          {
                                            results.Select(s => new CreateResultsRequest.Types.ResultCreate
                                                                {
                                                                  Data = ByteString.CopyFromUtf8($"Payload with name {s}"),
                                                                  Name = s,
                                                                }),
                                          },
                                        },
                                        CancellationToken.None)
                         .ConfigureAwait(false);

    var z = res.Results.Zip(results);

    return await agent.SubmitTasks(new SubmitTasksRequest
                                   {
                                     CommunicationToken = agent.Token,
                                     SessionId          = agent.SessionId,
                                     TaskCreations =
                                     {
                                       z.Select(tuple => new SubmitTasksRequest.Types.TaskCreation
                                                         {
                                                           PayloadId = tuple.First.ResultId,
                                                           DataDependencies =
                                                           {
                                                             DataDependency1,
                                                           },
                                                           ExpectedOutputKeys =
                                                           {
                                                             tuple.Second,
                                                           },
                                                         }),
                                     },
                                     TaskOptions = options,
                                   },
                                   CancellationToken.None)
                      .ConfigureAwait(false);
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

    var submit = await CreatePayloadAndSubmit(holder.Agent,
                                              Options,
                                              results.Results.Select(r => r.ResultId)
                                                     .AsICollection())
                   .ConfigureAwait(false);

    Assert.AreEqual(2,
                    submit.TaskInfos.Count);


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

    var submit2 = await CreatePayloadAndSubmit(holder.Agent,
                                               Options,
                                               results2.Results.Select(r => r.ResultId)
                                                       .AsICollection())
                    .ConfigureAwait(false);

    Assert.AreEqual(1,
                    submit2.TaskInfos.Count);

    var taskId3 = submit2.TaskInfos.Single()
                         .TaskId;
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


    var submit = await CreatePayloadAndSubmit(holder.Agent,
                                              optionsNull
                                                ? null
                                                : Options,
                                              results.Results.Select(r => r.ResultId)
                                                     .AsICollection())
                   .ConfigureAwait(false);

    Assert.AreEqual(200,
                    submit.TaskInfos.Count);

    await holder.Agent.FinalizeTaskCreation(CancellationToken.None)
                .ConfigureAwait(false);
  }

  [Test]
  public async Task GetResourceDataShouldSucceed()
  {
    using var holder = new AgentHolder();

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
                                         ResultId           = "ResourceData",
                                       },
                                       CancellationToken.None)
                .ConfigureAwait(false);

    var bytes = await File.ReadAllBytesAsync(Path.Combine(holder.Folder,
                                                          "ResourceData"))
                          .ConfigureAwait(false);
    Assert.AreEqual(Encoding.ASCII.GetBytes("Data1Data2"),
                    bytes);
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

      var resultMetadata = await holder.ResultTable.GetResult(result.ResultId,
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

      var resultMetadata = await holder.ResultTable.GetResult(result.ResultId,
                                                              CancellationToken.None)
                                       .ConfigureAwait(false);

      Assert.AreEqual(result.Name,
                      resultMetadata.Name);
      Assert.AreEqual(ResultStatus.Completed,
                      resultMetadata.Status);
      Assert.AreEqual(7,
                      resultMetadata.Size);
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


    await File.WriteAllBytesAsync(Path.Combine(holder.Folder,
                                               eok.Results.Last()
                                                  .ResultId),
                                  Encoding.ASCII.GetBytes("Data1Data2"))
              .ConfigureAwait(false);

    await holder.Agent.NotifyResultData(new NotifyResultDataRequest
                                        {
                                          CommunicationToken = holder.Token,
                                          Ids =
                                          {
                                            new NotifyResultDataRequest.Types.ResultIdentifier
                                            {
                                              ResultId = eok.Results.Last()
                                                            .ResultId,
                                              SessionId = holder.Session,
                                            },
                                          },
                                        },
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

    var uploadedResultData = await holder.ResultTable.GetResult(eok.Results.Last()
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

    uploadedResultData = await holder.ResultTable.GetResult(eok.Results.Last()
                                                               .ResultId)
                                     .ConfigureAwait(false);

    Assert.AreEqual(ResultStatus.Completed,
                    uploadedResultData.Status);
    Assert.AreEqual(10,
                    uploadedResultData.Size);

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
