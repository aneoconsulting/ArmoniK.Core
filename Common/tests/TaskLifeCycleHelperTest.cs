// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Base;
using ArmoniK.Core.Common.gRPC.Convertors;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.Helpers;
using ArmoniK.Utils;

using Google.Protobuf.WellKnownTypes;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

using NUnit.Framework;

using ResultStatus = ArmoniK.Core.Common.Storage.ResultStatus;
using SessionStatus = ArmoniK.Core.Common.Storage.SessionStatus;
using TaskRequest = ArmoniK.Core.Common.gRPC.Services.TaskRequest;
using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

namespace ArmoniK.Core.Common.Tests;

[TestFixture]
public class TaskLifeCycleHelperTest
{
  private class Holder : IDisposable
  {
    public const  string Partition       = "PartitionId";
    private const string ExpectedOutput1 = "ExpectedOutput1";
    private const string ExpectedOutput2 = "ExpectedOutput2";
    private const string DataDependency1 = "DataDependency1";
    private const string DataDependency2 = "DataDependency2";

    private static readonly Injection.Options.Submitter SubmitterOptions = new()
                                                                           {
                                                                             DefaultPartition = Partition,
                                                                             MaxErrorAllowed  = -1,
                                                                           };


    public readonly string         Folder;
    public readonly IObjectStorage ObjectStorage;

    public readonly TaskOptions Options = new()
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

    public readonly  IPartitionTable               PartitionTable;
    private readonly TestDatabaseProvider          prov_;
    public readonly  IPullQueueStorage             PullQueueStorage;
    public readonly  IPushQueueStorage             PushQueueStorage;
    public readonly  SimplePullQueueStorageChannel QueueStorage;
    public readonly  IResultTable                  ResultTable;
    public readonly  string                        Session;
    public readonly  ISessionTable                 SessionTable;
    public readonly  ISubmitter                    Submitter;
    public readonly  ITaskTable                    TaskTable;

    public Holder()
    {
      QueueStorage     = new SimplePullQueueStorageChannel();
      PushQueueStorage = QueueStorage;
      PullQueueStorage = QueueStorage;
      prov_ = new TestDatabaseProvider(collection => collection.AddSingleton<ISubmitter, gRPC.Services.Submitter>()
                                                               .AddSingleton(SubmitterOptions)
                                                               .AddSingleton(PushQueueStorage)
                                                               .AddSingleton(PullQueueStorage));

      ResultTable    = prov_.GetRequiredService<IResultTable>();
      TaskTable      = prov_.GetRequiredService<ITaskTable>();
      ObjectStorage  = prov_.GetRequiredService<IObjectStorage>();
      SessionTable   = prov_.GetRequiredService<ISessionTable>();
      PartitionTable = prov_.GetRequiredService<IPartitionTable>();
      Submitter      = prov_.GetRequiredService<ISubmitter>();

      PartitionTable.CreatePartitionsAsync(new List<PartitionData>
                                           {
                                             new(Partition,
                                                 new List<string>(),
                                                 1,
                                                 1,
                                                 1,
                                                 1,
                                                 null),
                                           })
                    .Wait();

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

      Submitter.FinalizeTaskCreation(createdTasks2,
                                     sessionData,
                                     Session,
                                     CancellationToken.None)
               .Wait();
    }

    public void Dispose()
      => prov_.Dispose();
  }


  [Test]
  public async Task SubmitTasksCreateOneRequestShouldSucceed()
  {
    using var holder = new Holder();

    await TaskLifeCycleHelper.PauseAsync(holder.TaskTable,
                                         holder.SessionTable,
                                         holder.Session)
                             .ConfigureAwait(false);

    var session = await holder.SessionTable.GetSessionAsync(holder.Session)
                              .ConfigureAwait(false);

    Assert.That(session.Status,
                Is.EqualTo(SessionStatus.Paused));

    await holder.QueueStorage.EmptyAsync()
                .ConfigureAwait(false);
    Assert.That(holder.QueueStorage.Channel.Reader.Count,
                Is.EqualTo(0));

    await TaskLifeCycleHelper.ResumeAsync(holder.TaskTable,
                                          holder.SessionTable,
                                          holder.PushQueueStorage,
                                          holder.Session)
                             .ConfigureAwait(false);

    session = await holder.SessionTable.GetSessionAsync(holder.Session)
                          .ConfigureAwait(false);

    Assert.That(session.Status,
                Is.EqualTo(SessionStatus.Running));
    Assert.That(holder.QueueStorage.Channel.Reader.Count,
                Is.EqualTo(1));
  }


  [Test]
  public async Task TaskSubmissionShouldSucceed()
  {
    using var holder = new Holder();

    var sessionId = await SessionLifeCycleHelper.CreateSession(holder.SessionTable,
                                                               holder.PartitionTable,
                                                               new List<string>
                                                               {
                                                                 Holder.Partition,
                                                               },
                                                               holder.Options.ToTaskOptions(),
                                                               Holder.Partition)
                                                .ConfigureAwait(false);

    var sessionData = await holder.SessionTable.GetSessionAsync(sessionId)
                                  .ConfigureAwait(false);

    var results = new List<Result>
                  {
                    new(sessionId,
                        Guid.NewGuid()
                            .ToString(),
                        "Payload",
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
                    new(sessionId,
                        Guid.NewGuid()
                            .ToString(),
                        "DataDependency1",
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
                    new(sessionId,
                        Guid.NewGuid()
                            .ToString(),
                        "DataDependency2",
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
                    new(sessionId,
                        Guid.NewGuid()
                            .ToString(),
                        "Output",
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
                  };

    await holder.ResultTable.Create(results)
                .ConfigureAwait(false);

    var tasks = new List<TaskCreationRequest>
                {
                  new(Guid.NewGuid()
                          .ToString(),
                      results[0]
                        .ResultId,
                      holder.Options.ToTaskOptions(),
                      new List<string>
                      {
                        results[3]
                          .ResultId,
                      },
                      new List<string>
                      {
                        results[1]
                          .ResultId,
                        results[2]
                          .ResultId,
                      }),
                };

    var taskId = tasks.Single()
                      .TaskId;

    await TaskLifeCycleHelper.CreateTasks(holder.TaskTable,
                                          holder.ResultTable,
                                          sessionId,
                                          sessionId,
                                          tasks,
                                          NullLogger.Instance)
                             .ConfigureAwait(false);

    var taskData = await holder.TaskTable.ReadTaskAsync(taskId)
                               .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Creating,
                    taskData.Status);

    await TaskLifeCycleHelper.FinalizeTaskCreation(holder.TaskTable,
                                                   holder.ResultTable,
                                                   holder.PushQueueStorage,
                                                   tasks,
                                                   sessionData,
                                                   sessionId,
                                                   NullLogger.Instance)
                             .ConfigureAwait(false);

    taskData = await holder.TaskTable.ReadTaskAsync(taskId)
                           .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Pending,
                    taskData.Status);
    Assert.AreEqual(2,
                    taskData.RemainingDataDependencies.Count);


    // complete first data dependency
    await holder.ResultTable.CompleteResult(sessionId,
                                            results[1]
                                              .ResultId,
                                            10,
                                            Encoding.UTF8.GetBytes("first data dependency"))
                .ConfigureAwait(false);

    await TaskLifeCycleHelper.ResolveDependencies(holder.TaskTable,
                                                  holder.ResultTable,
                                                  holder.PushQueueStorage,
                                                  sessionData,
                                                  new List<string>
                                                  {
                                                    results[1]
                                                      .ResultId,
                                                  },
                                                  NullLogger.Instance)
                             .ConfigureAwait(false);

    taskData = await holder.TaskTable.ReadTaskAsync(taskId)
                           .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Pending,
                    taskData.Status);
    Assert.AreEqual(1,
                    taskData.RemainingDataDependencies.Count);


    // complete second data dependency
    await holder.ResultTable.CompleteResult(sessionId,
                                            results[2]
                                              .ResultId,
                                            10,
                                            Encoding.UTF8.GetBytes("second data dependency"))
                .ConfigureAwait(false);

    await TaskLifeCycleHelper.ResolveDependencies(holder.TaskTable,
                                                  holder.ResultTable,
                                                  holder.PushQueueStorage,
                                                  sessionData,
                                                  new List<string>
                                                  {
                                                    results[2]
                                                      .ResultId,
                                                  },
                                                  NullLogger.Instance)
                             .ConfigureAwait(false);

    taskData = await holder.TaskTable.ReadTaskAsync(taskId)
                           .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Submitted,
                    taskData.Status);
    Assert.IsEmpty(taskData.RemainingDataDependencies);
    holder.QueueStorage.Channel.Writer.Complete();
    Assert.AreEqual(1,
                    (await holder.QueueStorage.Channel.Reader.ReadAllAsync()
                                 .ToListAsync()
                                 .ConfigureAwait(false)).Select(handler => handler.TaskId)
                                                        .Count(s => s == taskId));
  }

  [Test]
  public async Task TaskDeletionDuringSubmissionShouldSucceed([Values] bool finalize)
  {
    using var holder = new Holder();

    var sessionId = await SessionLifeCycleHelper.CreateSession(holder.SessionTable,
                                                               holder.PartitionTable,
                                                               new List<string>
                                                               {
                                                                 Holder.Partition,
                                                               },
                                                               holder.Options.ToTaskOptions(),
                                                               Holder.Partition)
                                                .ConfigureAwait(false);

    var sessionData = await holder.SessionTable.GetSessionAsync(sessionId)
                                  .ConfigureAwait(false);

    var results = new List<Result>
                  {
                    new(sessionId,
                        Guid.NewGuid()
                            .ToString(),
                        "Payload",
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
                    new(sessionId,
                        Guid.NewGuid()
                            .ToString(),
                        "DataDependency1",
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
                    new(sessionId,
                        Guid.NewGuid()
                            .ToString(),
                        "DataDependency2",
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
                    new(sessionId,
                        Guid.NewGuid()
                            .ToString(),
                        "Output",
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
                  };

    await holder.ResultTable.Create(results)
                .ConfigureAwait(false);

    var tasks = new List<TaskCreationRequest>
                {
                  new(Guid.NewGuid()
                          .ToString(),
                      results[0]
                        .ResultId,
                      holder.Options.ToTaskOptions(),
                      new List<string>
                      {
                        results[3]
                          .ResultId,
                      },
                      new List<string>
                      {
                        results[1]
                          .ResultId,
                        results[2]
                          .ResultId,
                      }),
                };

    var taskId = tasks.Single()
                      .TaskId;

    await TaskLifeCycleHelper.CreateTasks(holder.TaskTable,
                                          holder.ResultTable,
                                          sessionId,
                                          sessionId,
                                          tasks,
                                          NullLogger.Instance)
                             .ConfigureAwait(false);

    var taskData = await holder.TaskTable.ReadTaskAsync(taskId)
                               .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Creating,
                    taskData.Status);

    if (finalize)
    {
      await TaskLifeCycleHelper.FinalizeTaskCreation(holder.TaskTable,
                                                     holder.ResultTable,
                                                     holder.PushQueueStorage,
                                                     tasks,
                                                     sessionData,
                                                     sessionId,
                                                     NullLogger.Instance)
                               .ConfigureAwait(false);

      taskData = await holder.TaskTable.ReadTaskAsync(taskId)
                             .ConfigureAwait(false);

      Assert.AreEqual(TaskStatus.Pending,
                      taskData.Status);
      Assert.AreEqual(2,
                      taskData.RemainingDataDependencies.Count);
    }

    await TaskLifeCycleHelper.DeleteTasksAsync(holder.TaskTable,
                                               holder.ResultTable,
                                               tasks)
                             .ConfigureAwait(false);


    var count = await holder.TaskTable.FindTasksAsync(data => tasks.Select(request => request.TaskId)
                                                                   .Contains(data.TaskId),
                                                      data => data)
                            .CountAsync()
                            .ConfigureAwait(false);

    Assert.That(count,
                Is.EqualTo(0));
  }

  [Test]
  public async Task FinalizeTwiceShouldSucceed()
  {
    using var holder = new Holder();

    var sessionId = await SessionLifeCycleHelper.CreateSession(holder.SessionTable,
                                                               holder.PartitionTable,
                                                               new List<string>
                                                               {
                                                                 Holder.Partition,
                                                               },
                                                               holder.Options.ToTaskOptions(),
                                                               Holder.Partition)
                                                .ConfigureAwait(false);

    var sessionData = await holder.SessionTable.GetSessionAsync(sessionId)
                                  .ConfigureAwait(false);

    var results = new List<Result>
                  {
                    new(sessionId,
                        Guid.NewGuid()
                            .ToString(),
                        "Payload",
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
                    new(sessionId,
                        Guid.NewGuid()
                            .ToString(),
                        "DataDependency1",
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
                    new(sessionId,
                        Guid.NewGuid()
                            .ToString(),
                        "DataDependency2",
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
                    new(sessionId,
                        Guid.NewGuid()
                            .ToString(),
                        "Output",
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
                  };

    await holder.ResultTable.Create(results)
                .ConfigureAwait(false);

    var tasks = new List<TaskCreationRequest>
                {
                  new(Guid.NewGuid()
                          .ToString(),
                      results[0]
                        .ResultId,
                      holder.Options.ToTaskOptions(),
                      new List<string>
                      {
                        results[3]
                          .ResultId,
                      },
                      new List<string>
                      {
                        results[1]
                          .ResultId,
                        results[2]
                          .ResultId,
                      }),
                };

    var taskId = tasks.Single()
                      .TaskId;

    await TaskLifeCycleHelper.CreateTasks(holder.TaskTable,
                                          holder.ResultTable,
                                          sessionId,
                                          sessionId,
                                          tasks,
                                          NullLogger.Instance)
                             .ConfigureAwait(false);

    var taskData = await holder.TaskTable.ReadTaskAsync(taskId)
                               .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Creating,
                    taskData.Status);

    await TaskLifeCycleHelper.FinalizeTaskCreation(holder.TaskTable,
                                                   holder.ResultTable,
                                                   holder.PushQueueStorage,
                                                   tasks,
                                                   sessionData,
                                                   sessionId,
                                                   NullLogger.Instance)
                             .ConfigureAwait(false);

    taskData = await holder.TaskTable.ReadTaskAsync(taskId)
                           .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Pending,
                    taskData.Status);
    Assert.AreEqual(2,
                    taskData.RemainingDataDependencies.Count);


    // complete first data dependency
    await holder.ResultTable.CompleteResult(sessionId,
                                            results[1]
                                              .ResultId,
                                            10,
                                            Encoding.UTF8.GetBytes("first data dependency"))
                .ConfigureAwait(false);
    await holder.ResultTable.CompleteResult(sessionId,
                                            results[2]
                                              .ResultId,
                                            10,
                                            Encoding.UTF8.GetBytes("second data dependency"))
                .ConfigureAwait(false);
    await TaskLifeCycleHelper.ResolveDependencies(holder.TaskTable,
                                                  holder.ResultTable,
                                                  holder.PushQueueStorage,
                                                  sessionData,
                                                  new List<string>
                                                  {
                                                    results[1]
                                                      .ResultId,
                                                    results[2]
                                                      .ResultId,
                                                  },
                                                  NullLogger.Instance)
                             .ConfigureAwait(false);

    taskData = await holder.TaskTable.ReadTaskAsync(taskId)
                           .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Submitted,
                    taskData.Status);
    Assert.IsEmpty(taskData.RemainingDataDependencies);
    var count = 0;
    while (holder.QueueStorage.Channel.Reader.TryRead(out var handler))
    {
      if (handler.TaskId == taskId)
      {
        count++;
      }
    }

    Assert.AreEqual(1,
                    count);

    await TaskLifeCycleHelper.FinalizeTaskCreation(holder.TaskTable,
                                                   holder.ResultTable,
                                                   holder.PushQueueStorage,
                                                   tasks,
                                                   sessionData,
                                                   sessionId,
                                                   NullLogger.Instance)
                             .ConfigureAwait(false);

    await TaskLifeCycleHelper.FinalizeTaskCreation(holder.TaskTable,
                                                   holder.ResultTable,
                                                   holder.PushQueueStorage,
                                                   tasks,
                                                   sessionData,
                                                   sessionId,
                                                   NullLogger.Instance)
                             .ConfigureAwait(false);

    taskData = await holder.TaskTable.ReadTaskAsync(taskId)
                           .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Submitted,
                    taskData.Status);
    count = 0;
    while (holder.QueueStorage.Channel.Reader.TryRead(out var handler))
    {
      if (handler.TaskId == taskId)
      {
        count++;
      }
    }

    Assert.AreEqual(0,
                    count);
  }

  [Test]
  [Repeat(10)]
  [SuppressMessage("ReSharper",
                   "AccessToDisposedClosure")]
  public async Task FinalizeRace()
  {
    using var holder = new Holder();

    var sessionId = await SessionLifeCycleHelper.CreateSession(holder.SessionTable,
                                                               holder.PartitionTable,
                                                               new List<string>
                                                               {
                                                                 Holder.Partition,
                                                               },
                                                               holder.Options.ToTaskOptions(),
                                                               Holder.Partition)
                                                .ConfigureAwait(false);

    var sessionData = await holder.SessionTable.GetSessionAsync(sessionId)
                                  .ConfigureAwait(false);

    var results = new List<Result>
                  {
                    new(sessionId,
                        Guid.NewGuid()
                            .ToString(),
                        "Payload",
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
                    new(sessionId,
                        Guid.NewGuid()
                            .ToString(),
                        "DataDependency1",
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
                    new(sessionId,
                        Guid.NewGuid()
                            .ToString(),
                        "DataDependency2",
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
                    new(sessionId,
                        Guid.NewGuid()
                            .ToString(),
                        "Output",
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
                  };

    await holder.ResultTable.Create(results)
                .ConfigureAwait(false);

    var tasks = new List<TaskCreationRequest>
                {
                  new(Guid.NewGuid()
                          .ToString(),
                      results[0]
                        .ResultId,
                      holder.Options.ToTaskOptions(),
                      new List<string>
                      {
                        results[3]
                          .ResultId,
                      },
                      new List<string>
                      {
                        results[1]
                          .ResultId,
                        results[2]
                          .ResultId,
                      }),
                };

    var taskId = tasks.Single()
                      .TaskId;

    await TaskLifeCycleHelper.CreateTasks(holder.TaskTable,
                                          holder.ResultTable,
                                          sessionId,
                                          sessionId,
                                          tasks,
                                          holder.TaskTable.Logger)
                             .ConfigureAwait(false);

    var taskData = await holder.TaskTable.ReadTaskAsync(taskId)
                               .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Creating,
                    taskData.Status);

    await Task.WhenAll(FinalizeTask(),
                       FinalizeTask(),
                       CompleteResult(results[1]
                                        .ResultId),
                       CompleteResult(results[2]
                                        .ResultId))
              .ConfigureAwait(false);

    taskData = await holder.TaskTable.ReadTaskAsync(taskId)
                           .ConfigureAwait(false);

    var count = 0;
    while (holder.QueueStorage.Channel.Reader.TryRead(out var handler))
    {
      if (handler.TaskId == taskId)
      {
        count++;
      }
    }

    Assert.Multiple(() =>
                    {
                      Assert.That(taskData.Status,
                                  Is.EqualTo(TaskStatus.Submitted));
                      Assert.That(taskData.RemainingDataDependencies,
                                  Is.Empty);
                      Assert.That(count,
                                  Is.GreaterThanOrEqualTo(1));
                    });

    return;

    async Task CompleteResult(string resultId)
    {
      await holder.ResultTable.CompleteResult(sessionId,
                                              resultId,
                                              resultId.Length,
                                              Encoding.UTF8.GetBytes(resultId))
                  .ConfigureAwait(false);
      await TaskLifeCycleHelper.ResolveDependencies(holder.TaskTable,
                                                    holder.ResultTable,
                                                    holder.PushQueueStorage,
                                                    sessionData,
                                                    new List<string>
                                                    {
                                                      resultId,
                                                    },
                                                    holder.TaskTable.Logger)
                               .ConfigureAwait(false);
    }

    Task FinalizeTask()
      => TaskLifeCycleHelper.FinalizeTaskCreation(holder.TaskTable,
                                                  holder.ResultTable,
                                                  holder.PushQueueStorage,
                                                  tasks,
                                                  sessionData,
                                                  sessionId,
                                                  NullLogger.Instance);
  }

  [Test]
  public async Task PauseSessionWithDispatchedShouldSucceed()
  {
    using var holder = new Holder();

    var taskData = await holder.TaskTable.FindTasksAsync(data => data.Status == TaskStatus.Submitted && data.SessionId == holder.Session,
                                                         data => data)
                               .FirstAsync();

    taskData = taskData with
               {
                 OwnerPodId = "podId",
                 OwnerPodName = "podName",
                 AcquisitionDate = DateTime.Now,
                 ReceptionDate = DateTime.Now,
               };

    await holder.TaskTable.AcquireTask(taskData,
                                       CancellationToken.None)
                .ConfigureAwait(false);

    await TaskLifeCycleHelper.PauseAsync(holder.TaskTable,
                                         holder.SessionTable,
                                         holder.Session)
                             .ConfigureAwait(false);

    var session = await holder.SessionTable.GetSessionAsync(holder.Session)
                              .ConfigureAwait(false);

    Assert.That(session.Status,
                Is.EqualTo(SessionStatus.Paused));

    await holder.QueueStorage.EmptyAsync()
                .ConfigureAwait(false);
    Assert.That(holder.QueueStorage.Channel.Reader.Count,
                Is.EqualTo(0));

    await TaskLifeCycleHelper.ResumeAsync(holder.TaskTable,
                                          holder.SessionTable,
                                          holder.PushQueueStorage,
                                          holder.Session)
                             .ConfigureAwait(false);

    session = await holder.SessionTable.GetSessionAsync(holder.Session)
                          .ConfigureAwait(false);

    Assert.That(session.Status,
                Is.EqualTo(SessionStatus.Running));
    Assert.That(holder.QueueStorage.Channel.Reader.Count,
                Is.EqualTo(1));

    taskData = await holder.TaskTable.ReadTaskAsync(taskData.TaskId)
                           .ConfigureAwait(false);
    Assert.That(taskData.OwnerPodName,
                Is.Empty);
    Assert.That(taskData.OwnerPodId,
                Is.Empty);
    Assert.That(taskData.AcquisitionDate,
                Is.Null);
    Assert.That(taskData.ReceptionDate,
                Is.Null);
    Assert.That(taskData.Status,
                Is.EqualTo(TaskStatus.Submitted));
  }

  [Test]
  public async Task RetryRace([Values] TaskStatus status,
                              [Values(2,
                                      3,
                                      4)]
                              int concurrency)
  {
    using var holder = new Holder();

    while (holder.QueueStorage.Channel.Reader.TryRead(out var handler))
    {
      handler.Status = QueueMessageStatus.Processed;
      await handler.DisposeAsync()
                   .ConfigureAwait(false);
    }

    var taskData = await holder.TaskTable.FindTasksAsync(data => data.DataDependencies == new List<string>(),
                                                         data => data)
                               .FirstAsync()
                               .ConfigureAwait(false);

    var sessionData = await holder.SessionTable.GetSessionAsync(holder.Session)
                                  .ConfigureAwait(false);

    taskData = await holder.TaskTable.UpdateOneTask(taskData.TaskId,
                                                    null,
                                                    new UpdateDefinition<TaskData>().Set(td => td.Status,
                                                                                         status))
                           .ConfigureAwait(false);

    Assert.That(() => Enumerable.Range(0,
                                       concurrency)
                                .Select(_ => TaskLifeCycleHelper.RetryTaskAsync(holder.TaskTable,
                                                                                holder.ResultTable,
                                                                                holder.PushQueueStorage,
                                                                                taskData!,
                                                                                sessionData,
                                                                                null,
                                                                                "task has been retried",
                                                                                holder.TaskTable.Logger))
                                .WhenAll(),
                Throws.Nothing);

    taskData = await holder.TaskTable.ReadTaskAsync(taskData!.TaskId)
                           .ConfigureAwait(false);
    var retryTaskData = await holder.TaskTable.ReadTaskAsync(taskData.RetryId())
                                    .ConfigureAwait(false);

    var messages = new List<IQueueMessageHandler>();
    while (holder.QueueStorage.Channel.Reader.TryRead(out var handler))
    {
      messages.Add(handler);
    }

    Assert.Multiple(() =>
                    {
                      Assert.That(taskData.Status,
                                  Is.EqualTo(TaskStatus.Retried));
                      Assert.That(retryTaskData.Status,
                                  Is.EqualTo(TaskStatus.Submitted));
                      Assert.That(retryTaskData.RetryOfIds,
                                  Has.Member(taskData.TaskId));
                      Assert.That(messages,
                                  Has.Count.InRange(1,
                                                    concurrency)
                                     .And.All.Property("TaskId")
                                     .EqualTo(retryTaskData.TaskId));
                    });
  }

  public enum CrashState
  {
    Processing,
    ResultsCreated,
    ResultsCompleted,
    TasksCreated,
    TasksFinalized,
    DependenciesResolved,
  }

  [Test]
  public async Task HandleCrashedWhileProcessing([Values] CrashState crashState,
                                                 [Values] bool       subtask)
  {
    using var holder = new Holder();

    while (holder.QueueStorage.Channel.Reader.TryRead(out var handler))
    {
      handler.Status = QueueMessageStatus.Processed;
      await handler.DisposeAsync()
                   .ConfigureAwait(false);
    }

    var sessionId = await SessionLifeCycleHelper.CreateSession(holder.SessionTable,
                                                               holder.PartitionTable,
                                                               new List<string>
                                                               {
                                                                 Holder.Partition,
                                                               },
                                                               holder.Options.ToTaskOptions(),
                                                               Holder.Partition)
                                                .ConfigureAwait(false);

    var sessionData = await holder.SessionTable.GetSessionAsync(sessionId)
                                  .ConfigureAwait(false);

    var resultTemplate = new Result(sessionId,
                                    "",
                                    "",
                                    "",
                                    "",
                                    "",
                                    ResultStatus.Created,
                                    new List<string>(),
                                    DateTime.UtcNow,
                                    DateTime.UtcNow,
                                    0,
                                    Array.Empty<byte>(),
                                    false);

    var initResults = new List<Result>
                      {
                        resultTemplate with
                        {
                          ResultId = "payloadRoot",
                          Status = ResultStatus.Completed,
                        },
                        resultTemplate with
                        {
                          ResultId = "outputRoot",
                        },
                        resultTemplate with
                        {
                          ResultId = "payloadA",
                          CreatedBy = "root",
                          CompletedBy = "root",
                        },
                        resultTemplate with
                        {
                          ResultId = "intermediary",
                          CreatedBy = "root",
                        },
                      };

    var submitResults = new List<Result>
                        {
                          resultTemplate with
                          {
                            ResultId = "payloadB",
                          },
                          resultTemplate with
                          {
                            ResultId = "outputB",
                          },
                        };

    var initTasks = new List<TaskCreationRequest>
                    {
                      new("root",
                          "payloadRoot",
                          holder.Options.ToTaskOptions(),
                          new List<string>
                          {
                            "outputRoot",
                          },
                          new List<string>()),
                      new("A",
                          "payloadA",
                          holder.Options.ToTaskOptions(),
                          new List<string>
                          {
                            "intermediary",
                          },
                          new List<string>()),
                    };

    var submitTasks = new List<TaskCreationRequest>
                      {
                        new("B",
                            "payloadB",
                            holder.Options.ToTaskOptions(),
                            new List<string>
                            {
                              "outputRoot",
                              "outputB",
                            },
                            new List<string>
                            {
                              "intermediary",
                            }),
                      };

    if (!subtask)
    {
      initResults.AddRange(submitResults);
      initTasks.AddRange(submitTasks);

      submitResults.Clear();
      submitTasks.Clear();
    }

    await holder.ResultTable.Create(initResults)
                .ConfigureAwait(false);

    await TaskLifeCycleHelper.CreateTasks(holder.TaskTable,
                                          holder.ResultTable,
                                          sessionId,
                                          sessionId,
                                          initTasks,
                                          holder.TaskTable.Logger)
                             .ConfigureAwait(false);

    await TaskLifeCycleHelper.FinalizeTaskCreation(holder.TaskTable,
                                                   holder.ResultTable,
                                                   holder.PushQueueStorage,
                                                   initTasks,
                                                   sessionData,
                                                   "root",
                                                   holder.TaskTable.Logger)
                             .ConfigureAwait(false);

    while (holder.QueueStorage.Channel.Reader.TryRead(out var handler))
    {
      Assert.That(handler.TaskId,
                  Is.EqualTo("root"));
    }

    await holder.TaskTable.UpdateOneTask("root",
                                         null,
                                         new UpdateDefinition<TaskData>().Set(td => td.Status,
                                                                              TaskStatus.Processing)
                                                                         .Set(td => td.OwnerPodId,
                                                                              "ownerpodid"))
                .ConfigureAwait(false);


    if (crashState >= CrashState.ResultsCreated)
    {
      await holder.ResultTable.Create(submitResults.ViewSelect(r => r with
                                                                    {
                                                                      CreatedBy = "root",
                                                                    }))
                  .ConfigureAwait(false);
    }


    if (crashState >= CrashState.ResultsCompleted)
    {
      await holder.ResultTable.CompleteManyResults(new List<(string resultId, long size, byte[] opaqueId)>
                                                   {
                                                     ("payloadA", 0, ""u8.ToArray()),
                                                     ("payloadB", 0, ""u8.ToArray()),
                                                   },
                                                   "root")
                  .ConfigureAwait(false);
    }

    if (crashState >= CrashState.TasksCreated)
    {
      await TaskLifeCycleHelper.CreateTasks(holder.TaskTable,
                                            holder.ResultTable,
                                            sessionId,
                                            "root",
                                            submitTasks,
                                            holder.TaskTable.Logger)
                               .ConfigureAwait(false);
    }

    if (crashState >= CrashState.TasksFinalized)
    {
      await TaskLifeCycleHelper.FinalizeTaskCreation(holder.TaskTable,
                                                     holder.ResultTable,
                                                     holder.PushQueueStorage,
                                                     submitTasks,
                                                     sessionData,
                                                     "root",
                                                     holder.TaskTable.Logger)
                               .ConfigureAwait(false);
    }

    if (crashState >= CrashState.DependenciesResolved)
    {
      await TaskLifeCycleHelper.ResolveDependencies(holder.TaskTable,
                                                    holder.ResultTable,
                                                    holder.PushQueueStorage,
                                                    sessionData,
                                                    new List<string>
                                                    {
                                                      "payloadA",
                                                    },
                                                    holder.TaskTable.Logger)
                               .ConfigureAwait(false);
    }

    var task = await holder.TaskTable.ReadTaskAsync("root",
                                                    td => td)
                           .ConfigureAwait(false);

    var status = await TaskLifeCycleHelper.HandleTaskCrashedWhileProcessing(holder.TaskTable,
                                                                            holder.ResultTable,
                                                                            holder.ObjectStorage,
                                                                            holder.PushQueueStorage,
                                                                            new Injection.Options.Submitter(),
                                                                            TimeSpan.Zero,
                                                                            sessionData,
                                                                            task,
                                                                            holder.TaskTable.Logger,
                                                                            CancellationToken.None)
                                          .ConfigureAwait(false);
    task = await holder.TaskTable.ReadTaskAsync("root",
                                                td => td)
                       .ConfigureAwait(false);

    var committed = crashState >= CrashState.DependenciesResolved || (subtask && crashState >= CrashState.TasksFinalized);

    var taskA = await holder.TaskTable.ReadTaskAsync("A",
                                                     td => td)
                            .ConfigureAwait(false);
    var taskB = await holder.TaskTable.FindTasksAsync(td => td.TaskId == "B",
                                                      td => td)
                            .ToListAsync()
                            .ConfigureAwait(false);

    var outputRoot = await holder.ResultTable.GetResult("outputRoot")
                                 .ConfigureAwait(false);

    var outputB = await holder.ResultTable.GetResults(r => r.ResultId == "outputB",
                                                      r => r)
                              .ToListAsync()
                              .ConfigureAwait(false);

    var messages = new List<string>();

    while (holder.QueueStorage.Channel.Reader.TryRead(out var handler))
    {
      messages.Add(handler.TaskId);
    }

    Assert.Multiple(() =>
                    {
                      Assert.That(task.Status,
                                  Is.EqualTo(status));
                      Assert.That(status,
                                  Is.EqualTo(committed
                                               ? TaskStatus.Completed
                                               : TaskStatus.Retried));
                      Assert.That(taskA.Status,
                                  Is.EqualTo(committed
                                               ? TaskStatus.Submitted
                                               : TaskStatus.Pending));

                      Assert.That(taskB,
                                  crashState >= CrashState.TasksCreated || !subtask
                                    ? committed || !subtask
                                        ? Has.ItemAt(0)
                                             .Property("Status")
                                             .EqualTo(TaskStatus.Pending)
                                        : Is.Empty // subtasks are deleted when not committed
                                    : Is.Empty);

                      Assert.That(outputB,
                                  crashState >= CrashState.ResultsCreated || !subtask
                                    ? committed || !subtask
                                        ? Has.ItemAt(0)
                                             .Property("Status")
                                             .EqualTo(ResultStatus.Created)
                                        : Has.ItemAt(0) // results are aborted when not committed
                                             .Property("Status")
                                             .EqualTo(ResultStatus.Aborted)
                                    : Is.Empty);

                      Assert.That(outputRoot.Status,
                                  Is.EqualTo(ResultStatus.Created));
                      Assert.That(outputRoot.OwnerTaskId,
                                  Is.EqualTo(committed
                                               ? "B"
                                               : "root###1"));

                      Assert.That(messages,
                                  Has.Member(committed
                                               ? "A"
                                               : "root###1")
                                     .And.No.Member(committed
                                                      ? "root###1"
                                                      : "A")
                                     .And.No.Member("root"));
                    });
  }
}
