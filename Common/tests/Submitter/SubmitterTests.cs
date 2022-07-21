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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Api.Worker.Options;
using ArmoniK.Core.Adapters.Memory;
using ArmoniK.Core.Adapters.MongoDB;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.Helpers;

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
using Output = ArmoniK.Api.gRPC.V1.Output;
using TaskOptions = ArmoniK.Api.gRPC.V1.TaskOptions;
using TaskRequest = ArmoniK.Core.Common.gRPC.Services.TaskRequest;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Tests.Submitter;

[TestFixture]
public class SubmitterTests
{
  private                 ISubmitter     submitter_;
  private                 MongoDbRunner  runner_;
  private                 MongoClient    client_;
  private const           string         DatabaseName    = "ArmoniK_TestDB";
  private const           string         SessionId       = "SessionId";
  private const           string         TaskCreatingId  = "TaskCreatingId";
  private const           string         TaskSubmittedId = "TaskSubmittedId";
  private const           string         TaskCompletedId = "TaskCompeletedId";
  private const           string         ExpectedOutput1 = "ExpectedOutput1";
  private const           string         ExpectedOutput2 = "ExpectedOutput2";
  private const           string         ExpectedOutput3 = "ExpectedOutput3";
  private static readonly ActivitySource ActivitySource  = new("ArmoniK.Core.Common.Tests.Submitter");
  private                 ISessionTable  sessionTable_;


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
                                                   $"{ComputePlan.SettingSection}:{nameof(ComputePlan.MessageBatchSize)}", "1"
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
            .AddSingleton<IQueueStorage, QueueStorage>();

    var provider = services.BuildServiceProvider(new ServiceProviderOptions
                                                 {
                                                   ValidateOnBuild = true,
                                                 });

    submitter_    = provider.GetRequiredService<ISubmitter>();
    sessionTable_ = provider.GetRequiredService<ISessionTable>();
  }

  [TearDown]
  public virtual void TearDown()
  {
    client_ = null;
    runner_.Dispose();
    submitter_ = null;
  }

  private static async Task InitSubmitter(ISubmitter        submitter,
                                          CancellationToken token)
  {
    var defaultTaskOptions = new TaskOptions
                             {
                               MaxDuration = Duration.FromTimeSpan(TimeSpan.FromSeconds(2)),
                               MaxRetries  = 2,
                               Priority    = 1,
                             };

    await submitter.CreateSession(SessionId,
                                  defaultTaskOptions,
                                  token)
                   .ConfigureAwait(false);

    await submitter.CreateTasks(SessionId,
                                SessionId,
                                defaultTaskOptions,
                                new List<TaskRequest>
                                {
                                  new(TaskCreatingId,
                                      new[]
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
                   .ConfigureAwait(false);

    var tuple = await submitter.CreateTasks(SessionId,
                                            SessionId,
                                            defaultTaskOptions,
                                            new List<TaskRequest>
                                            {
                                              new(TaskSubmittedId,
                                                  new[]
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

    await submitter.FinalizeTaskCreation(tuple.requests,
                                         tuple.priority,
                                         SessionId,
                                         SessionId,
                                         CancellationToken.None)
                   .ConfigureAwait(false);
  }

  private static async Task InitSubmitterCompleteTask(ISubmitter        submitter,
                                                      CancellationToken token)
  {
    var defaultTaskOptions = new TaskOptions
                             {
                               MaxDuration = Duration.FromTimeSpan(TimeSpan.FromSeconds(2)),
                               MaxRetries  = 2,
                               Priority    = 1,
                             };

    var taskdata = new TaskData(SessionId,
                                TaskCompletedId,
                                "OwnerPodId",
                                "PayloadId",
                                new List<string>(),
                                new List<string>(),
                                new List<string>
                                {
                                  ExpectedOutput3
                                },
                                new List<string>(),
                                TaskStatus.Completed,
                                defaultTaskOptions,
                                new Storage.Output(false,
                                                   ""));

    var tuple = await submitter.CreateTasks(SessionId,
                                            SessionId,
                                            defaultTaskOptions,
                                            new List<TaskRequest>
                                            {
                                              new(TaskCompletedId,
                                                  new[]
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

    await submitter.FinalizeTaskCreation(tuple.requests,
                                         tuple.priority,
                                         SessionId,
                                         SessionId,
                                         token)
                   .ConfigureAwait(false);

    await submitter.StartTask(TaskCompletedId,
                              token)
                   .ConfigureAwait(false);

    await submitter.SetResult(SessionId,
                              TaskCompletedId,
                              ExpectedOutput3,
                              new List<ReadOnlyMemory<byte>>
                              {
                                new(Encoding.ASCII.GetBytes(ExpectedOutput3 + "AAAA")),
                                new(Encoding.ASCII.GetBytes(ExpectedOutput3 + "BBBB")),
                              }.ToAsyncEnumerable(),
                              token)
                   .ConfigureAwait(false);

    await submitter.CompleteTaskAsync(taskdata,
                                      true,
                                      new Output
                                      {
                                        Ok = new Empty(),
                                      },
                                      token)
                   .ConfigureAwait(false);
  }


  [Test]
  public async Task CreateSessionShouldSucceed()
  {
    await InitSubmitter(submitter_,
                        CancellationToken.None)
      .ConfigureAwait(false);

    var result = await submitter_.ListSessionsAsync(new SessionFilter
                                                    {
                                                      Sessions =
                                                      {
                                                        SessionId,
                                                      },
                                                    },
                                                    CancellationToken.None)
                                 .ConfigureAwait(false);

    Assert.AreEqual(SessionId,
                    result.SessionIds.Single());
  }

  [Test]
  public async Task CreateTaskShouldSucceed()
  {
    await InitSubmitter(submitter_,
                        CancellationToken.None)
      .ConfigureAwait(false);

    var result = await submitter_.ListTasksAsync(new TaskFilter
                                                 {
                                                   Task = new TaskFilter.Types.IdsRequest
                                                          {
                                                            Ids =
                                                            {
                                                              TaskCreatingId,
                                                            },
                                                          },
                                                 },
                                                 CancellationToken.None)
                                 .ConfigureAwait(false);

    Assert.AreEqual(TaskCreatingId,
                    result.TaskIds.Single());
  }

  [Test]
  public async Task GetStatusShouldSucceed()
  {
    await InitSubmitter(submitter_,
                        CancellationToken.None)
      .ConfigureAwait(false);

    var result = await submitter_.GetTaskStatusAsync(new GetTaskStatusRequest
                                                     {
                                                       TaskIds =
                                                       {
                                                         TaskCreatingId,
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
    await InitSubmitter(submitter_,
                        CancellationToken.None)
      .ConfigureAwait(false);

    var result = await submitter_.GetTaskStatusAsync(new GetTaskStatusRequest
                                                     {
                                                       TaskIds =
                                                       {
                                                         TaskSubmittedId
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
    await InitSubmitter(submitter_,
                        CancellationToken.None)
      .ConfigureAwait(false);

    var res = await submitter_.GetTaskStatusAsync(new GetTaskStatusRequest
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
    await InitSubmitter(submitter_,
                        CancellationToken.None)
      .ConfigureAwait(false);

    await InitSubmitterCompleteTask(submitter_,
                                    CancellationToken.None)
      .ConfigureAwait(false);

    var writer = new TestHelperServerStreamWriter<ResultReply>();

    var resultRequest = new ResultRequest
                        {
                          Key     = ExpectedOutput3,
                          Session = SessionId,
                        };

    await submitter_.WaitForAvailabilityAsync(resultRequest,
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
    await InitSubmitter(submitter_,
                        CancellationToken.None)
      .ConfigureAwait(false);

    var writer = new TestHelperServerStreamWriter<ResultReply>();

    Assert.ThrowsAsync<ResultNotFoundException>(() => submitter_.TryGetResult(new ResultRequest
                                                                              {
                                                                                Key     = "NotExistingResult",
                                                                                Session = SessionId,
                                                                              },
                                                                              writer,
                                                                              CancellationToken.None));
  }

  [Test]
  public async Task TryGetResultWithNotCompletedTaskShouldReturnNotCompletedTaskReply()
  {
    await InitSubmitter(submitter_,
                        CancellationToken.None)
      .ConfigureAwait(false);

    var writer = new TestHelperServerStreamWriter<ResultReply>();

    await submitter_.TryGetResult(new ResultRequest
                                  {
                                    Key     = ExpectedOutput2,
                                    Session = SessionId,
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
    await InitSubmitter(submitter_,
                        CancellationToken.None)
      .ConfigureAwait(false);

    await submitter_.CancelSession(SessionId,
                                   CancellationToken.None)
                    .ConfigureAwait(false);

    Assert.IsTrue(await sessionTable_.IsSessionCancelledAsync(SessionId,
                                                              CancellationToken.None)
                                     .ConfigureAwait(false));
  }

  [Test]
  public async Task GetTaskOutputShouldSucceed()
  {
    await InitSubmitter(submitter_,
                        CancellationToken.None)
      .ConfigureAwait(false);

    await InitSubmitterCompleteTask(submitter_,
                                    CancellationToken.None)
      .ConfigureAwait(false);

    await submitter_.WaitForCompletion(new WaitRequest
                                       {
                                         Filter = new TaskFilter
                                                  {
                                                    Task = new TaskFilter.Types.IdsRequest
                                                           {
                                                             Ids =
                                                             {
                                                               TaskCompletedId,
                                                             },
                                                           },
                                                  },
                                       },
                                       CancellationToken.None)
                    .ConfigureAwait(false);

    var output = await submitter_.TryGetTaskOutputAsync(new ResultRequest
                                                        {
                                                          Key     = TaskCompletedId,
                                                          Session = SessionId,
                                                        },
                                                        CancellationToken.None)
                                 .ConfigureAwait(false);

    Assert.AreEqual(Output.TypeOneofCase.Ok,
                    output.TypeCase);
  }

  [Test]
  public async Task CancelTaskShouldSucceed()
  {
    await InitSubmitter(submitter_,
                        CancellationToken.None)
      .ConfigureAwait(false);

    await submitter_.CancelTasks(new TaskFilter
                                 {
                                   Session = new TaskFilter.Types.IdsRequest
                                             {
                                               Ids =
                                               {
                                                 SessionId,
                                               },
                                             },
                                 },
                                 CancellationToken.None)
                    .ConfigureAwait(false);

    var reply = await submitter_.GetTaskStatusAsync(new GetTaskStatusRequest
                                                    {
                                                      TaskIds =
                                                      {
                                                        TaskCreatingId,
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
    await InitSubmitter(submitter_,
                        CancellationToken.None)
      .ConfigureAwait(false);

    var result = await submitter_.GetResultStatusAsync(new GetResultStatusRequest
                                                       {
                                                         SessionId = SessionId,
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
    await InitSubmitter(submitter_,
                        CancellationToken.None)
      .ConfigureAwait(false);

    var result = await submitter_.GetResultStatusAsync(new GetResultStatusRequest
                                                       {
                                                         SessionId = SessionId,
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
}
