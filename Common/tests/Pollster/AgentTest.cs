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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Base.Exceptions;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC.Convertors;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.Helpers;
using ArmoniK.Utils;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using NUnit.Framework;

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
    public readonly  ISessionTable        SessionTable;
    public readonly  ISubmitter           Submitter;
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

      SessionTable = prov_.GetRequiredService<ISessionTable>();
      Submitter    = prov_.GetRequiredService<ISubmitter>();

      Session = SessionTable.SetSessionDataAsync(new[]
                                                 {
                                                   Partition,
                                                 },
                                                 Options.ToTaskOptions(),
                                                 CancellationToken.None)
                            .Result;

      var sessionData = SessionTable.GetSessionAsync(Session,
                                                     CancellationToken.None)
                                    .Result;

      ResultTable.Create(new[]
                         {
                           new Result(sessionData.SessionId,
                                      DataDependency1,
                                      "",
                                      "",
                                      "",
                                      "",
                                      ResultStatus.Completed,
                                      new List<string>(),
                                      DateTime.UtcNow,
                                      DateTime.UtcNow,
                                      0,
                                      Array.Empty<byte>(),
                                      false),
                           new Result(sessionData.SessionId,
                                      DataDependency2,
                                      "",
                                      "",
                                      "",
                                      "",
                                      ResultStatus.Completed,
                                      new List<string>(),
                                      DateTime.UtcNow,
                                      DateTime.UtcNow,
                                      0,
                                      Array.Empty<byte>(),
                                      false),
                           new Result(Session,
                                      ExpectedOutput1,
                                      "",
                                      "",
                                      "",
                                      "",
                                      ResultStatus.Created,
                                      new List<string>(),
                                      DateTime.UtcNow,
                                      null,
                                      0,
                                      Array.Empty<byte>(),
                                      false),
                           new Result(Session,
                                      ExpectedOutput2,
                                      "",
                                      "",
                                      "",
                                      "",
                                      ResultStatus.Created,
                                      new List<string>(),
                                      DateTime.UtcNow,
                                      null,
                                      0,
                                      Array.Empty<byte>(),
                                      false),
                         },
                         CancellationToken.None)
                 .Wait();

      Folder = Path.Combine(Path.GetTempPath(),
                            "data");
      Directory.CreateDirectory(Folder);

      var createdTasks = Submitter.CreateTasks(Session,
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

      Submitter.FinalizeTaskCreation(createdTasks,
                                     sessionData,
                                     Session,
                                     CancellationToken.None)
               .Wait();

      QueueStorage.Messages.Clear();

      TaskData = TaskTable.ReadTaskAsync(createdTasks.Single()
                                                     .TaskId,
                                         CancellationToken.None)
                          .Result;

      var createdTasks2 = Submitter.CreateTasks(Session,
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

      Submitter.FinalizeTaskCreation(createdTasks2,
                                     sessionData,
                                     Session,
                                     CancellationToken.None)
               .Wait();

      Token = Guid.NewGuid()
                  .ToString();

      Agent = new Agent(Submitter,
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

    Assert.ThrowsAsync<RpcException>(() => holder.Agent.SubmitTasks(Array.Empty<TaskSubmissionRequest>(),
                                                                    Options.ToNullableTaskOptions(),
                                                                    "",
                                                                    token,
                                                                    CancellationToken.None));

    Assert.ThrowsAsync<RpcException>(() => holder.Agent.CreateResultsMetaData(token,
                                                                              Array.Empty<ResultCreationRequest>(),
                                                                              CancellationToken.None));

    Assert.ThrowsAsync<RpcException>(() => holder.Agent.CreateResults(token,
                                                                      Array.Empty<(ResultCreationRequest request, ReadOnlyMemory<byte> data)>(),
                                                                      CancellationToken.None));

    Assert.ThrowsAsync<RpcException>(() => holder.Agent.NotifyResultData(token,
                                                                         Array.Empty<string>(),
                                                                         CancellationToken.None));

    Assert.ThrowsAsync<RpcException>(() => holder.Agent.GetCommonData(token,
                                                                      "",
                                                                      CancellationToken.None));

    Assert.ThrowsAsync<RpcException>(() => holder.Agent.GetDirectData(token,
                                                                      "",
                                                                      CancellationToken.None));

    Assert.ThrowsAsync<RpcException>(() => holder.Agent.GetResourceData(token,
                                                                        "",
                                                                        CancellationToken.None));
  }


  [Test]
  public void UnImplementedData()
  {
    using var holder = new AgentHolder();

    Assert.ThrowsAsync<NotImplementedException>(() => holder.Agent.GetCommonData(holder.Token,
                                                                                 "",
                                                                                 CancellationToken.None));

    Assert.ThrowsAsync<NotImplementedException>(() => holder.Agent.GetDirectData(holder.Token,
                                                                                 "",
                                                                                 CancellationToken.None));
  }

  [Test]
  public void MissingResourceData()
  {
    using var holder = new AgentHolder();

    Assert.ThrowsAsync<RpcException>(() => holder.Agent.GetResourceData(holder.Token,
                                                                        "DataNotExisting",
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

    await holder.Agent.NotifyResultData(holder.Token,
                                        new[]
                                        {
                                          ExpectedOutput1,
                                        },
                                        CancellationToken.None)
                .ConfigureAwait(false);

    await holder.Agent.CreateResultsAndSubmitChildTasksAsync(CancellationToken.None)
                .ConfigureAwait(false);

    var resultData = await holder.ResultTable.GetResult(ExpectedOutput1,
                                                        CancellationToken.None)
                                 .ConfigureAwait(false);

    Assert.AreEqual(ExpectedOutput1,
                    resultData.ResultId);
    Assert.AreEqual(ResultStatus.Completed,
                    resultData.Status);
    Assert.NotNull(resultData.CompletionDate);
    Assert.AreEqual(holder.TaskData.TaskId,
                    resultData.OwnerTaskId);
    Assert.AreEqual(data.Length,
                    resultData.Size);

    var datAsyncEnumerable = holder.ObjectStorage.GetValuesAsync(resultData.OpaqueId,
                                                                 CancellationToken.None);

    var dataStored = await datAsyncEnumerable.SingleAsync(CancellationToken.None)
                                             .ConfigureAwait(false);

    Assert.AreEqual(data,
                    dataStored);

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

  private static async Task<ICollection<TaskCreationRequest>> CreatePayloadAndSubmit(IAgent              agent,
                                                                                     TaskOptions?        options,
                                                                                     ICollection<string> results)
  {
    var res = await agent.CreateResults(agent.Token,
                                        results.ViewSelect(s => (new ResultCreationRequest(agent.SessionId,
                                                                                           s),
                                                                 new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes($"Payload with name {s}")))),
                                        CancellationToken.None)
                         .ConfigureAwait(false);

    var z = res.Zip(results);

    return await agent.SubmitTasks(z.Select(tuple => new TaskSubmissionRequest(tuple.First.ResultId,
                                                                               null,
                                                                               new List<string>
                                                                               {
                                                                                 tuple.Second,
                                                                               },
                                                                               new List<string>
                                                                               {
                                                                                 DataDependency1,
                                                                               }))
                                    .AsICollection(),
                                   options.ToNullableTaskOptions(),
                                   agent.SessionId,
                                   agent.Token,
                                   CancellationToken.None)
                      .ConfigureAwait(false);
  }

  [Test]
  public async Task CreateTasksShouldSucceed()
  {
    using var holder = new AgentHolder();

    var results = await holder.Agent.CreateResultsMetaData(holder.Token,
                                                           new ResultCreationRequest[]
                                                           {
                                                             new(holder.Session,
                                                                 "Task1EOK"),
                                                             new(holder.Session,
                                                                 "Task2EOK"),
                                                           },
                                                           CancellationToken.None)
                              .ConfigureAwait(false);

    var submit = await CreatePayloadAndSubmit(holder.Agent,
                                              Options,
                                              results.ViewSelect(r => r.ResultId))
                   .ConfigureAwait(false);

    Assert.AreEqual(2,
                    submit.Count);


    var results2 = await holder.Agent.CreateResultsMetaData(holder.Token,
                                                            new ResultCreationRequest[]
                                                            {
                                                              new(holder.Session,
                                                                  "Task3EOK"),
                                                            },
                                                            CancellationToken.None)
                               .ConfigureAwait(false);

    var submit2 = await CreatePayloadAndSubmit(holder.Agent,
                                               Options,
                                               results2.ViewSelect(r => r.ResultId))
                    .ConfigureAwait(false);

    Assert.AreEqual(1,
                    submit2.Count);

    var taskId3 = submit2.Single()
                         .TaskId;

    await holder.Agent.CreateResultsAndSubmitChildTasksAsync(CancellationToken.None)
                .ConfigureAwait(false);

    Assert.AreEqual(3,
                    holder.QueueStorage.Messages[Partition]
                          .Count);

    var taskData3 = await holder.TaskTable.ReadTaskAsync(taskId3,
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

    var results = await holder.Agent.CreateResultsMetaData(holder.Token,
                                                           Enumerable.Range(0,
                                                                            200)
                                                                     .Select(i => new ResultCreationRequest(holder.Session,
                                                                                                            $"Task{i}EOK"))
                                                                     .AsICollection(),
                                                           CancellationToken.None)
                              .ConfigureAwait(false);


    var submit = await CreatePayloadAndSubmit(holder.Agent,
                                              optionsNull
                                                ? null
                                                : Options,
                                              results.ViewSelect(r => r.ResultId))
                   .ConfigureAwait(false);

    Assert.AreEqual(200,
                    submit.Count);

    await holder.Agent.CreateResultsAndSubmitChildTasksAsync(CancellationToken.None)
                .ConfigureAwait(false);
  }

  [Test]
  public async Task GetResourceDataShouldSucceed()
  {
    using var holder = new AgentHolder();

    var (id, size) = await holder.ObjectStorage.AddOrUpdateAsync(new ObjectData
                                                                 {
                                                                   ResultId  = "ResultId",
                                                                   SessionId = "SessionId",
                                                                 },
                                                                 new List<byte[]>
                                                                   {
                                                                     Encoding.ASCII.GetBytes("Data1"),
                                                                     Encoding.ASCII.GetBytes("Data2"),
                                                                   }.Select(bytes => new ReadOnlyMemory<byte>(bytes))
                                                                    .ToAsyncEnumerable(),
                                                                 CancellationToken.None)
                                 .ConfigureAwait(false);

    await holder.ResultTable.Create(new List<Result>
                                    {
                                      new("SessionId",
                                          "ResourceData",
                                          "",
                                          "",
                                          "",
                                          "",
                                          ResultStatus.Completed,
                                          new List<string>(),
                                          DateTime.UtcNow,
                                          DateTime.UtcNow,
                                          size,
                                          id,
                                          false),
                                    })
                .ConfigureAwait(false);

    await holder.Agent.GetResourceData(holder.Token,
                                       "ResourceData",
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

    var results = await holder.Agent.CreateResults(holder.Token,
                                                   new (ResultCreationRequest request, ReadOnlyMemory<byte> data)[]
                                                   {
                                                     (new ResultCreationRequest(holder.Session,
                                                                                "Result1"), new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes("Result1"))),
                                                     (new ResultCreationRequest(holder.Session,
                                                                                "Result2"), new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes("Result2"))),
                                                   },
                                                   CancellationToken.None)
                              .ConfigureAwait(false);

    await holder.Agent.CreateResultsAndSubmitChildTasksAsync(CancellationToken.None)
                .ConfigureAwait(false);

    foreach (var result in results)
    {
      Console.WriteLine(result);

      var resultMetadata = await holder.ResultTable.GetResult(result.ResultId,
                                                              CancellationToken.None)
                                       .ConfigureAwait(false);

      Assert.AreEqual(result.Name,
                      resultMetadata.Name);
      Assert.AreEqual(ResultStatus.Completed,
                      resultMetadata.Status);
      Assert.NotNull(resultMetadata.CompletionDate);
      Assert.AreEqual(7,
                      resultMetadata.Size);

      var bytes = (await holder.ObjectStorage.GetValuesAsync(resultMetadata.OpaqueId)
                               .ToListAsync()
                               .ConfigureAwait(false)).Single();

      Assert.AreEqual(ByteString.CopyFromUtf8(result.Name)
                                .ToByteArray(),
                      bytes);
    }
  }


  [Test]
  public async Task SubmitTaskWhileSubmissionIsClosedShouldThrow()
  {
    using var holder = new AgentHolder();

    await holder.SessionTable.StopSubmissionAsync(holder.Session,
                                                  false,
                                                  true)
                .ConfigureAwait(false);

    var sessionData = await holder.SessionTable.GetSessionAsync(holder.Session)
                                  .ConfigureAwait(false);

    var agent = new Agent(holder.Submitter,
                          holder.ObjectStorage,
                          holder.QueueStorage,
                          holder.ResultTable,
                          holder.TaskTable,
                          sessionData,
                          holder.TaskData,
                          holder.Folder,
                          holder.Token,
                          NullLogger.Instance);

    Assert.ThrowsAsync<SubmissionClosedException>(() => agent.SubmitTasks(new TaskSubmissionRequest[]
                                                                          {
                                                                            new("payload",
                                                                                null,
                                                                                new List<string>(),
                                                                                new List<string>()),
                                                                          },
                                                                          null,
                                                                          holder.Session,
                                                                          holder.Token,
                                                                          CancellationToken.None));
  }

  [Test]
  [TestCase(false)]
  [TestCase(true)]
  public async Task SubmitTasksCreateOneRequestShouldSucceed(bool optionsNull)
  {
    using var holder = new AgentHolder();

    var payload = await holder.Agent.CreateResults(holder.Token,
                                                   new (ResultCreationRequest request, ReadOnlyMemory<byte> data)[]
                                                   {
                                                     (new ResultCreationRequest(holder.Session,
                                                                                "Payload"), new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes("Payload"))),
                                                   },
                                                   CancellationToken.None)
                              .ConfigureAwait(false);

    var eok = await holder.Agent.CreateResultsMetaData(holder.Token,
                                                       new ResultCreationRequest[]
                                                       {
                                                         new(holder.Session,
                                                             "EOK1"),
                                                         new(holder.Session,
                                                             "EOK2"),
                                                       },
                                                       CancellationToken.None)
                          .ConfigureAwait(false);

    var reply = await holder.Agent.SubmitTasks(new TaskSubmissionRequest[]
                                               {
                                                 new(payload.Single()
                                                            .ResultId,
                                                     optionsNull
                                                       ? null
                                                       : Options.ToNullableTaskOptions(),
                                                     eok.Select(r => r.ResultId)
                                                        .AsICollection(),
                                                     new List<string>()),
                                               },
                                               optionsNull
                                                 ? null
                                                 : Options.ToNullableTaskOptions(),
                                               holder.Session,
                                               holder.Token,
                                               CancellationToken.None)
                            .ConfigureAwait(false);

    Assert.AreEqual(1,
                    reply.Count);
    Assert.AreEqual(payload.Single()
                           .ResultId,
                    reply.Single()
                         .PayloadId);
    Assert.AreEqual(holder.TaskData.TaskId,
                    payload.Single()
                           .CreatedBy);
    foreach (var eokResult in eok)
    {
      Assert.Contains(eokResult.ResultId,
                      reply.Single()
                           .ExpectedOutputKeys.ToList());
      Assert.AreEqual(holder.TaskData.TaskId,
                      eokResult.CreatedBy);
    }

    await holder.Agent.CreateResultsAndSubmitChildTasksAsync(CancellationToken.None)
                .ConfigureAwait(false);


    var taskData = await holder.TaskTable.ReadTaskAsync(reply.Single()
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

    var eok = await holder.Agent.CreateResultsMetaData(holder.Token,
                                                       new ResultCreationRequest[]
                                                       {
                                                         new(holder.Session,
                                                             "EOK1"),
                                                         new(holder.Session,
                                                             "EOK2"),
                                                         new(holder.Session,
                                                             "Payload"),
                                                       },
                                                       CancellationToken.None)
                          .ConfigureAwait(false);

    var reply = await holder.Agent.SubmitTasks(new TaskSubmissionRequest[]
                                               {
                                                 new(eok.Last()
                                                        .ResultId,
                                                     optionsNull
                                                       ? null
                                                       : Options.ToNullableTaskOptions(),
                                                     eok.Select(r => r.ResultId)
                                                        .SkipLast(1)
                                                        .AsICollection(),
                                                     new List<string>()),
                                               },
                                               optionsNull
                                                 ? null
                                                 : Options.ToNullableTaskOptions(),
                                               holder.Session,
                                               holder.Token,
                                               CancellationToken.None)
                            .ConfigureAwait(false);


    await File.WriteAllBytesAsync(Path.Combine(holder.Folder,
                                               eok.Last()
                                                  .ResultId),
                                  Encoding.ASCII.GetBytes("Data1Data2"))
              .ConfigureAwait(false);

    await holder.Agent.NotifyResultData(holder.Token,
                                        new[]
                                        {
                                          eok.Last()
                                             .ResultId,
                                        },
                                        CancellationToken.None)
                .ConfigureAwait(false);

    Assert.AreEqual(1,
                    reply.Count);
    Assert.AreEqual(eok.Last()
                       .ResultId,
                    reply.Single()
                         .PayloadId);

    foreach (var eokResult in eok.SkipLast(1))
    {
      Assert.Contains(eokResult.ResultId,
                      reply.Single()
                           .ExpectedOutputKeys.ToList());
    }

    await holder.Agent.CreateResultsAndSubmitChildTasksAsync(CancellationToken.None)
                .ConfigureAwait(false);

    var uploadedResultData = await holder.ResultTable.GetResult(eok.Last()
                                                                   .ResultId)
                                         .ConfigureAwait(false);

    Assert.AreEqual(ResultStatus.Completed,
                    uploadedResultData.Status);
    Assert.NotNull(uploadedResultData.CompletionDate);
    Assert.AreEqual(10,
                    uploadedResultData.Size);

    var taskData = await holder.TaskTable.ReadTaskAsync(reply.Single()
                                                             .TaskId,
                                                        CancellationToken.None)
                               .ConfigureAwait(false);

    Assert.IsEmpty(taskData.RemainingDataDependencies);
    Assert.AreEqual(TaskStatus.Submitted,
                    taskData.Status);
  }

  [Test]
  public async Task SubmitTasksDuplicateResult()
  {
    using var holder = new AgentHolder();

    var payload = await holder.Agent.CreateResults(holder.Token,
                                                   new[]
                                                   {
                                                     (new ResultCreationRequest(holder.Session,
                                                                                "Payload"), new ReadOnlyMemory<byte>("Payload"u8.ToArray())),
                                                   },
                                                   CancellationToken.None)
                              .ConfigureAwait(false);

    var reply = await holder.Agent.SubmitTasks(new[]
                                               {
                                                 new TaskSubmissionRequest(payload.Single()
                                                                                  .ResultId,
                                                                           null,
                                                                           Array.Empty<string>(),
                                                                           Array.Empty<string>()),
                                               },
                                               null,
                                               holder.Session,
                                               holder.Token,
                                               CancellationToken.None)
                            .ConfigureAwait(false);

    // Create the result before finalizing the task to create a failure
    await holder.ResultTable.Create(new[]
                                    {
                                      payload.Single(),
                                    })
                .ConfigureAwait(false);

    Assert.That(() => holder.Agent.CreateResultsAndSubmitChildTasksAsync(CancellationToken.None),
                Throws.TypeOf<ArmoniKException>());

    Assert.That(() => holder.TaskTable.ReadTaskAsync(reply.Single()
                                                          .TaskId,
                                                     CancellationToken.None),
                Throws.TypeOf<TaskNotFoundException>());
  }
}
