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
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Options;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Adapters.Memory;
using ArmoniK.Core.Adapters.MongoDB;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.gRPC.Validators;
using ArmoniK.Core.Common.Injection;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.Helpers;
using ArmoniK.Core.Common.Utils;

using Google.Protobuf.WellKnownTypes;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Mongo2Go;

using MongoDB.Bson;
using MongoDB.Driver;

using NUnit.Framework;

using Empty = ArmoniK.Api.gRPC.V1.Empty;
using Output = ArmoniK.Core.Common.Storage.Output;
using TaskOptions = ArmoniK.Api.gRPC.V1.TaskOptions;
using TaskRequest = ArmoniK.Core.Common.gRPC.Services.TaskRequest;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Tests.Submitter;

[TestFixture]
public class SubmitterTests
{
  [SetUp]
  public void SetUp()
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
                                                   DefaultPartition
                                                 },
                                               };

    Console.WriteLine(minimalConfig.ToJson());

    var loggerFactory = new LoggerFactory();
    loggerFactory.AddProvider(new ConsoleForwardingLoggerProvider());

    var configuration = new ConfigurationManager();
    configuration.AddInMemoryCollection(minimalConfig);

    var services = new ServiceCollection();


    services.AddMongoStorages(configuration,
                              logger)
            .AddSingleton(ActivitySource)
            .AddSingleton<IMongoClient>(client_)
            .AddLogging()
            .AddSingleton<ISubmitter, gRPC.Services.Submitter>()
            .AddOption<Injection.Options.Submitter>(configuration,
                                                    Injection.Options.Submitter.SettingSection)
            .AddSingleton<IPushQueueStorage, PushQueueStorage>();

    var provider = services.BuildServiceProvider(new ServiceProviderOptions
                                                 {
                                                   ValidateOnBuild = true,
                                                 });

    submitter_      = provider.GetRequiredService<ISubmitter>();
    sessionTable_   = provider.GetRequiredService<ISessionTable>();
    taskTable_      = provider.GetRequiredService<ITaskTable>();
    partitionTable_ = provider.GetRequiredService<IPartitionTable>();

    partitionTable_.CreatePartitionsAsync(new[]
                                          {
                                            new PartitionData(DefaultPartition,
                                                              new List<string>(),
                                                              10,
                                                              50,
                                                              20,
                                                              1,
                                                              new PodConfiguration(new Dictionary<string, string>())),
                                          })
                   .Wait();
  }

  [TearDown]
  public virtual void TearDown()
  {
    client_ = null;
    runner_?.Dispose();
    submitter_ = null;
  }

  private                 ISubmitter?      submitter_;
  private                 MongoDbRunner?   runner_;
  private                 MongoClient?     client_;
  private const           string           DatabaseName     = "ArmoniK_TestDB";
  private static readonly string           ExpectedOutput1  = "ExpectedOutput1";
  private static readonly string           ExpectedOutput2  = "ExpectedOutput2";
  private static readonly string           ExpectedOutput3  = "ExpectedOutput3";
  private static readonly string           DefaultPartition = "DefaultPartition";
  private static readonly ActivitySource   ActivitySource   = new("ArmoniK.Core.Common.Tests.Submitter");
  private                 ISessionTable?   sessionTable_;
  private                 ITaskTable?      taskTable_;
  private                 IPartitionTable? partitionTable_;

  private static async Task<(string sessionId, string taskCreating, string taskSubmitted)> InitSubmitter(ISubmitter        submitter,
                                                                                                         IPartitionTable   partitionTable,
                                                                                                         CancellationToken token)
  {
    var defaultTaskOptions = new TaskOptions
                             {
                               MaxDuration = Duration.FromTimeSpan(TimeSpan.FromSeconds(2)),
                               MaxRetries  = 2,
                               Priority    = 1,
                               PartitionId = "part1",
                             };

    await partitionTable.CreatePartitionsAsync(new[]
                                               {
                                                 new PartitionData("part1",
                                                                   new List<string>(),
                                                                   10,
                                                                   10,
                                                                   20,
                                                                   1,
                                                                   new PodConfiguration(new Dictionary<string, string>())),
                                                 new PartitionData("part2",
                                                                   new List<string>(),
                                                                   10,
                                                                   10,
                                                                   20,
                                                                   1,
                                                                   new PodConfiguration(new Dictionary<string, string>())),
                                               },
                                               token)
                        .ConfigureAwait(false);

    var sessionId = (await submitter.CreateSession(new[]
                                                   {
                                                     "part1",
                                                     "part2",
                                                   },
                                                   defaultTaskOptions,
                                                   token)
                                    .ConfigureAwait(false)).SessionId;

    var taskCreating = (await submitter.CreateTasks(sessionId,
                                                    sessionId,
                                                    defaultTaskOptions,
                                                    new List<TaskRequest>
                                                    {
                                                      new(new[]
                                                          {
                                                            ExpectedOutput1,
                                                          },
                                                          new List<string>(),
                                                          new List<ReadOnlyMemory<byte>>
                                                          {
                                                            new(Encoding.ASCII.GetBytes("AAAA")),
                                                          }.ToAsyncEnumerable()),
                                                    }.ToAsyncEnumerable(),
                                                    CancellationToken.None)
                                       .ConfigureAwait(false)).requests.First()
                                                              .Id;

    var tuple = await submitter.CreateTasks(sessionId,
                                            sessionId,
                                            defaultTaskOptions,
                                            new List<TaskRequest>
                                            {
                                              new(new[]
                                                  {
                                                    ExpectedOutput2,
                                                  },
                                                  new List<string>(),
                                                  new List<ReadOnlyMemory<byte>>
                                                  {
                                                    new(Encoding.ASCII.GetBytes("AAAA")),
                                                  }.ToAsyncEnumerable()),
                                            }.ToAsyncEnumerable(),
                                            CancellationToken.None)
                               .ConfigureAwait(false);

    var taskSubmitted = tuple.requests.First()
                             .Id;

    await submitter.FinalizeTaskCreation(tuple.requests,
                                         tuple.priority,
                                         tuple.partitionId,
                                         sessionId,
                                         sessionId,
                                         CancellationToken.None)
                   .ConfigureAwait(false);

    return (sessionId, taskCreating, taskSubmitted);
  }

  private static async Task<string> InitSubmitterCompleteTask(ISubmitter        submitter,
                                                              string            sessionId,
                                                              CancellationToken token)
  {
    var defaultTaskOptions = new TaskOptions
                             {
                               MaxDuration = Duration.FromTimeSpan(TimeSpan.FromSeconds(2)),
                               MaxRetries  = 2,
                               Priority    = 1,
                               PartitionId = "part2",
                             };


    var tuple = await submitter.CreateTasks(sessionId,
                                            sessionId,
                                            defaultTaskOptions,
                                            new List<TaskRequest>
                                            {
                                              new(new[]
                                                  {
                                                    ExpectedOutput3,
                                                  },
                                                  new List<string>(),
                                                  new List<ReadOnlyMemory<byte>>
                                                  {
                                                    new(Encoding.ASCII.GetBytes("AAAA")),
                                                  }.ToAsyncEnumerable()),
                                            }.ToAsyncEnumerable(),
                                            token)
                               .ConfigureAwait(false);

    var taskCompletedId = tuple.requests.First()
                               .Id;

    await submitter.FinalizeTaskCreation(tuple.requests,
                                         tuple.priority,
                                         tuple.partitionId,
                                         sessionId,
                                         sessionId,
                                         token)
                   .ConfigureAwait(false);

    var taskData = new TaskData(sessionId,
                                taskCompletedId,
                                "OwnerPodId",
                                "PayloadId",
                                new List<string>(),
                                new List<string>(),
                                new List<string>
                                {
                                  ExpectedOutput3,
                                },
                                new List<string>(),
                                TaskStatus.Completed,
                                defaultTaskOptions,
                                new Output(false,
                                           ""));

    await submitter.StartTask(taskCompletedId,
                              token)
                   .ConfigureAwait(false);

    await submitter.SetResult(sessionId,
                              taskCompletedId,
                              ExpectedOutput3,
                              new List<ReadOnlyMemory<byte>>
                              {
                                new(Encoding.ASCII.GetBytes(ExpectedOutput3 + "AAAA")),
                                new(Encoding.ASCII.GetBytes(ExpectedOutput3 + "BBBB")),
                              }.ToAsyncEnumerable(),
                              token)
                   .ConfigureAwait(false);

    await submitter.CompleteTaskAsync(taskData,
                                      true,
                                      new Api.gRPC.V1.Output
                                      {
                                        Ok = new Empty(),
                                      },
                                      token)
                   .ConfigureAwait(false);

    return taskCompletedId;
  }


  [Test]
  public async Task CreateSessionShouldSucceed()
  {
    var (sessionId, _, _) = await InitSubmitter(submitter_!,
                                                partitionTable_!,
                                                CancellationToken.None)
                              .ConfigureAwait(false);

    var result = await submitter_!.ListSessionsAsync(new SessionFilter
                                                     {
                                                       Sessions =
                                                       {
                                                         sessionId,
                                                       },
                                                     },
                                                     CancellationToken.None)
                                  .ConfigureAwait(false);

    Assert.AreEqual(sessionId,
                    result.SessionIds.Single());
  }

  [Test]
  public void CreateSessionWithInvalidPartition()
  {
    var defaultTaskOptions = new TaskOptions
                             {
                               MaxDuration = Duration.FromTimeSpan(TimeSpan.FromSeconds(2)),
                               MaxRetries  = 2,
                               Priority    = 1,
                               PartitionId = "invalid",
                             };

    Assert.ThrowsAsync<PartitionNotFoundException>(async () => await submitter_!.CreateSession(new List<string>
                                                                                               {
                                                                                                 "invalid",
                                                                                               },
                                                                                               defaultTaskOptions,
                                                                                               CancellationToken.None)
                                                                                .ConfigureAwait(false));
  }

  [Test]
  public void CreateSessionWithInvalidPartitionInTaskOptions()
  {
    var defaultTaskOptions = new TaskOptions
                             {
                               MaxDuration = Duration.FromTimeSpan(TimeSpan.FromSeconds(2)),
                               MaxRetries  = 2,
                               Priority    = 1,
                               PartitionId = "invalid",
                             };

    Assert.ThrowsAsync<PartitionNotFoundException>(async () => await submitter_!.CreateSession(new List<string>
                                                                                               {
                                                                                                 DefaultPartition,
                                                                                               },
                                                                                               defaultTaskOptions,
                                                                                               CancellationToken.None)
                                                                                .ConfigureAwait(false));
  }

  [Test]
  public async Task CreateSessionWithDefaultPartition()
  {
    var defaultTaskOptions = new TaskOptions
                             {
                               MaxDuration = Duration.FromTimeSpan(TimeSpan.FromSeconds(2)),
                               MaxRetries  = 2,
                               Priority    = 1,
                               PartitionId = DefaultPartition,
                             };

    var sessionReply = await submitter_!.CreateSession(new List<string>
                                                       {
                                                         DefaultPartition,
                                                       },
                                                       defaultTaskOptions,
                                                       CancellationToken.None)
                                        .ConfigureAwait(false);
    Assert.NotNull(sessionReply.SessionId);
    Assert.IsNotEmpty(sessionReply.SessionId);
  }

  [Test]
  public async Task CreateTaskShouldSucceed()
  {
    var (_, taskCreating, _) = await InitSubmitter(submitter_!,
                                                   partitionTable_!,
                                                   CancellationToken.None)
                                 .ConfigureAwait(false);

    var result = await submitter_!.ListTasksAsync(new TaskFilter
                                                  {
                                                    Task = new TaskFilter.Types.IdsRequest
                                                           {
                                                             Ids =
                                                             {
                                                               taskCreating,
                                                             },
                                                           },
                                                  },
                                                  CancellationToken.None)
                                  .ConfigureAwait(false);

    Assert.AreEqual(taskCreating,
                    result.TaskIds.Single());
  }

  [Test]
  public async Task CreateTaskWithoutPartitionShouldSucceed()
  {
    var _ = await InitSubmitter(submitter_!,
                                partitionTable_!,
                                CancellationToken.None)
              .ConfigureAwait(false);

    var taskOptions = new TaskOptions
                      {
                        MaxDuration = Duration.FromTimeSpan(TimeSpan.FromMinutes(4)),
                        MaxRetries  = 3,
                        Priority    = 1,
                      };

    var taskOptionsValidator = new TaskOptionsValidator();
    Assert.IsTrue(taskOptionsValidator.Validate(taskOptions)
                                      .IsValid);

    var sessionId = (await submitter_!.CreateSession(new List<string>(),
                                                     taskOptions,
                                                     CancellationToken.None)
                                      .ConfigureAwait(false)).SessionId;

    var tuple = await submitter_!.CreateTasks(sessionId,
                                              sessionId,
                                              taskOptions,
                                              new List<TaskRequest>
                                              {
                                                new(new[]
                                                    {
                                                      ExpectedOutput2,
                                                    },
                                                    new List<string>(),
                                                    new List<ReadOnlyMemory<byte>>
                                                    {
                                                      new(Encoding.ASCII.GetBytes("AAAA")),
                                                    }.ToAsyncEnumerable()),
                                              }.ToAsyncEnumerable(),
                                              CancellationToken.None)
                                 .ConfigureAwait(false);

    Assert.AreEqual(DefaultPartition,
                    tuple.partitionId);

    var result = await submitter_!.ListTasksAsync(new TaskFilter
                                                  {
                                                    Task = new TaskFilter.Types.IdsRequest
                                                           {
                                                             Ids =
                                                             {
                                                               tuple.requests.Single()
                                                                    .Id,
                                                             },
                                                           },
                                                  },
                                                  CancellationToken.None)
                                  .ConfigureAwait(false);

    Assert.AreEqual(tuple.requests.Single()
                         .Id,
                    result.TaskIds.Single());
  }

  [Test]
  public async Task CreateTaskWithoutPartitionForTasksShouldSucceed()
  {
    var _ = await InitSubmitter(submitter_!,
                                partitionTable_!,
                                CancellationToken.None)
              .ConfigureAwait(false);

    var taskOptions = new TaskOptions
                      {
                        MaxDuration = Duration.FromTimeSpan(TimeSpan.FromMinutes(4)),
                        MaxRetries  = 3,
                        Priority    = 1,
                      };

    var sessionTaskOptions = new TaskOptions
                             {
                               MaxDuration = Duration.FromTimeSpan(TimeSpan.FromMinutes(4)),
                               MaxRetries  = 3,
                               Priority    = 1,
                               PartitionId = "part1",
                             };

    var taskOptionsValidator = new TaskOptionsValidator();
    Assert.IsTrue(taskOptionsValidator.Validate(taskOptions)
                                      .IsValid);

    var sessionId = (await submitter_!.CreateSession(new List<string>
                                                     {
                                                       "part1",
                                                     },
                                                     sessionTaskOptions,
                                                     CancellationToken.None)
                                      .ConfigureAwait(false)).SessionId;

    var tuple = await submitter_!.CreateTasks(sessionId,
                                              sessionId,
                                              taskOptions,
                                              new List<TaskRequest>
                                              {
                                                new(new[]
                                                    {
                                                      ExpectedOutput2,
                                                    },
                                                    new List<string>(),
                                                    new List<ReadOnlyMemory<byte>>
                                                    {
                                                      new(Encoding.ASCII.GetBytes("AAAA")),
                                                    }.ToAsyncEnumerable()),
                                              }.ToAsyncEnumerable(),
                                              CancellationToken.None)
                                 .ConfigureAwait(false);

    Assert.AreEqual("part1",
                    tuple.partitionId);

    var result = await submitter_!.ListTasksAsync(new TaskFilter
                                                  {
                                                    Task = new TaskFilter.Types.IdsRequest
                                                           {
                                                             Ids =
                                                             {
                                                               tuple.requests.Single()
                                                                    .Id,
                                                             },
                                                           },
                                                  },
                                                  CancellationToken.None)
                                  .ConfigureAwait(false);

    Assert.AreEqual(tuple.requests.Single()
                         .Id,
                    result.TaskIds.Single());
  }

  [Test]
  public Task CreateSessionInvalidPartitionShouldFail()
  {
    var defaultTaskOptions = new TaskOptions
                             {
                               MaxDuration = Duration.FromTimeSpan(TimeSpan.FromSeconds(2)),
                               MaxRetries  = 2,
                               Priority    = 1,
                               PartitionId = "invalid",
                             };

    Assert.ThrowsAsync<PartitionNotFoundException>(() => submitter_!.CreateSession(new[]
                                                                                   {
                                                                                     "part1",
                                                                                     "part2",
                                                                                   },
                                                                                   defaultTaskOptions,
                                                                                   CancellationToken.None));
    return Task.CompletedTask;
  }

  [Test]
  public async Task GetStatusShouldSucceed()
  {
    var (_, taskCreating, _) = await InitSubmitter(submitter_!,
                                                   partitionTable_!,
                                                   CancellationToken.None)
                                 .ConfigureAwait(false);

    var result = await submitter_!.GetTaskStatusAsync(new GetTaskStatusRequest
                                                      {
                                                        TaskIds =
                                                        {
                                                          taskCreating,
                                                        },
                                                      },
                                                      CancellationToken.None)
                                  .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Creating,
                    result.IdStatuses.Single()
                          .Status);
  }

  [Test]
  public async Task FinalizeTaskCreationShouldSucceed()
  {
    var (_, _, taskSubmitted) = await InitSubmitter(submitter_!,
                                                    partitionTable_!,
                                                    CancellationToken.None)
                                  .ConfigureAwait(false);

    var result = await submitter_!.GetTaskStatusAsync(new GetTaskStatusRequest
                                                      {
                                                        TaskIds =
                                                        {
                                                          taskSubmitted,
                                                        },
                                                      },
                                                      CancellationToken.None)
                                  .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Submitted,
                    result.IdStatuses.Single()
                          .Status);
  }

  [Test]
  public async Task GetStatusReturnEmptyList()
  {
    await InitSubmitter(submitter_!,
                        partitionTable_!,
                        CancellationToken.None)
      .ConfigureAwait(false);

    var res = await submitter_!.GetTaskStatusAsync(new GetTaskStatusRequest
                                                   {
                                                     TaskIds =
                                                     {
                                                       "taskdoesnotexist",
                                                     },
                                                   },
                                                   CancellationToken.None);

    Assert.AreEqual(0,
                    res.IdStatuses.Count);
  }

  [Test]
  public async Task TryGetResultShouldSucceed()
  {
    var (sessionId, _, _) = await InitSubmitter(submitter_!,
                                                partitionTable_!,
                                                CancellationToken.None)
                              .ConfigureAwait(false);

    await InitSubmitterCompleteTask(submitter_!,
                                    sessionId,
                                    CancellationToken.None)
      .ConfigureAwait(false);

    var writer = new TestHelperServerStreamWriter<ResultReply>();

    var resultRequest = new ResultRequest
                        {
                          ResultId = ExpectedOutput3,
                          Session  = sessionId,
                        };

    await submitter_!.WaitForAvailabilityAsync(resultRequest,
                                               CancellationToken.None)
                     .ConfigureAwait(false);

    await submitter_.TryGetResult(resultRequest,
                                  writer,
                                  CancellationToken.None)
                    .ConfigureAwait(false);

    Assert.AreEqual(ResultReply.TypeOneofCase.Result,
                    writer.Messages[0]
                          .TypeCase);
    Assert.AreEqual(ResultReply.TypeOneofCase.Result,
                    writer.Messages[1]
                          .TypeCase);
    Assert.AreEqual(ResultReply.TypeOneofCase.Result,
                    writer.Messages[2]
                          .TypeCase);
    Assert.IsTrue(writer.Messages[2]
                        .Result.DataComplete);
  }

  [Test]
  public async Task TryGetResultShouldFail()
  {
    var (sessionId, _, _) = await InitSubmitter(submitter_!,
                                                partitionTable_!,
                                                CancellationToken.None)
                              .ConfigureAwait(false);

    var writer = new TestHelperServerStreamWriter<ResultReply>();

    Assert.ThrowsAsync<ResultNotFoundException>(() => submitter_!.TryGetResult(new ResultRequest
                                                                               {
                                                                                 ResultId = "NotExistingResult",
                                                                                 Session  = sessionId,
                                                                               },
                                                                               writer,
                                                                               CancellationToken.None));
  }

  [Test]
  public async Task TryGetResultWithNotCompletedTaskShouldReturnNotCompletedTaskReply()
  {
    var (sessionId, _, _) = await InitSubmitter(submitter_!,
                                                partitionTable_!,
                                                CancellationToken.None)
                              .ConfigureAwait(false);

    var writer = new TestHelperServerStreamWriter<ResultReply>();

    await submitter_!.TryGetResult(new ResultRequest
                                   {
                                     ResultId = ExpectedOutput2,
                                     Session  = sessionId,
                                   },
                                   writer,
                                   CancellationToken.None)
                     .ConfigureAwait(false);

    Console.WriteLine(writer.Messages.Single());

    Assert.AreEqual(ResultReply.TypeOneofCase.NotCompletedTask,
                    writer.Messages.Single()
                          .TypeCase);
  }

  [Test]
  public async Task CancelSessionShouldSucceed()
  {
    var (sessionId, _, _) = await InitSubmitter(submitter_!,
                                                partitionTable_!,
                                                CancellationToken.None)
                              .ConfigureAwait(false);

    await submitter_!.CancelSession(sessionId,
                                    CancellationToken.None)
                     .ConfigureAwait(false);

    Assert.IsTrue(await sessionTable_!.IsSessionCancelledAsync(sessionId,
                                                               CancellationToken.None)
                                      .ConfigureAwait(false));
  }

  [Test]
  public async Task GetTaskOutputShouldSucceed()
  {
    var (sessionId, _, _) = await InitSubmitter(submitter_!,
                                                partitionTable_!,
                                                CancellationToken.None)
                              .ConfigureAwait(false);

    var taskCompleted = await InitSubmitterCompleteTask(submitter_!,
                                                        sessionId,
                                                        CancellationToken.None)
                          .ConfigureAwait(false);

    await submitter_!.WaitForCompletion(new WaitRequest
                                        {
                                          Filter = new TaskFilter
                                                   {
                                                     Task = new TaskFilter.Types.IdsRequest
                                                            {
                                                              Ids =
                                                              {
                                                                taskCompleted,
                                                              },
                                                            },
                                                   },
                                        },
                                        CancellationToken.None)
                     .ConfigureAwait(false);

    var output = await submitter_.TryGetTaskOutputAsync(new TaskOutputRequest
                                                        {
                                                          TaskId  = taskCompleted,
                                                          Session = sessionId,
                                                        },
                                                        CancellationToken.None)
                                 .ConfigureAwait(false);

    Assert.AreEqual(Api.gRPC.V1.Output.TypeOneofCase.Ok,
                    output.TypeCase);
  }

  [Test]
  public async Task CancelTaskShouldSucceed()
  {
    var (sessionId, taskCreating, _) = await InitSubmitter(submitter_!,
                                                           partitionTable_!,
                                                           CancellationToken.None)
                                         .ConfigureAwait(false);

    await submitter_!.CancelTasks(new TaskFilter
                                  {
                                    Session = new TaskFilter.Types.IdsRequest
                                              {
                                                Ids =
                                                {
                                                  sessionId,
                                                },
                                              },
                                  },
                                  CancellationToken.None)
                     .ConfigureAwait(false);

    var reply = await submitter_.GetTaskStatusAsync(new GetTaskStatusRequest
                                                    {
                                                      TaskIds =
                                                      {
                                                        taskCreating,
                                                      },
                                                    },
                                                    CancellationToken.None)
                                .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Canceling,
                    reply.IdStatuses.Single()
                         .Status);
  }

  [Test]
  public async Task GetResultStatusShouldSucceed()
  {
    var (sessionId, _, _) = await InitSubmitter(submitter_!,
                                                partitionTable_!,
                                                CancellationToken.None)
                              .ConfigureAwait(false);

    var result = await submitter_!.GetResultStatusAsync(new GetResultStatusRequest
                                                        {
                                                          SessionId = sessionId,
                                                          ResultIds =
                                                          {
                                                            ExpectedOutput2,
                                                          },
                                                        },
                                                        CancellationToken.None)
                                  .ConfigureAwait(false);

    Assert.AreEqual(ResultStatus.Created,
                    result.IdStatuses.Single()
                          .Status);
    Assert.AreEqual(ExpectedOutput2,
                    result.IdStatuses.Single()
                          .ResultId);
  }

  [Test]
  public async Task GetNotExistingResultStatusShouldSucceed()
  {
    var (sessionId, _, _) = await InitSubmitter(submitter_!,
                                                partitionTable_!,
                                                CancellationToken.None)
                              .ConfigureAwait(false);

    var result = await submitter_!.GetResultStatusAsync(new GetResultStatusRequest
                                                        {
                                                          SessionId = sessionId,
                                                          ResultIds =
                                                          {
                                                            "NotExistingId",
                                                          },
                                                        },
                                                        CancellationToken.None)
                                  .ConfigureAwait(false);

    Assert.AreEqual(0,
                    result.IdStatuses.Count);
  }

  [Test]
  public async Task GetPartitionTaskStatus()
  {
    var (sessionId, _, _) = await InitSubmitter(submitter_!,
                                                partitionTable_!,
                                                CancellationToken.None)
                              .ConfigureAwait(false);

    await InitSubmitterCompleteTask(submitter_!,
                                    sessionId,
                                    CancellationToken.None)
      .ConfigureAwait(false);

    var result = (await taskTable_!.CountPartitionTasksAsync(CancellationToken.None)
                                   .ConfigureAwait(false)).OrderBy(r => r.Status)
                                                          .ThenBy(r => r.PartitionId)
                                                          .ToIList();

    Assert.AreEqual(3,
                    result.Count);
    Assert.AreEqual(new PartitionTaskStatusCount("part1",
                                                 TaskStatus.Creating,
                                                 1),
                    result[0]);
    Assert.AreEqual(new PartitionTaskStatusCount("part1",
                                                 TaskStatus.Submitted,
                                                 1),
                    result[1]);
    Assert.AreEqual(new PartitionTaskStatusCount("part2",
                                                 TaskStatus.Completed,
                                                 1),
                    result[2]);
  }
}
