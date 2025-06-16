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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Base;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC.Convertors;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.gRPC.Validators;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.Helpers;
using ArmoniK.Utils;

using Google.Protobuf.WellKnownTypes;

using Microsoft.Extensions.DependencyInjection;

using NUnit.Framework;

using Output = ArmoniK.Core.Common.Storage.Output;
using ResultStatus = ArmoniK.Core.Common.Storage.ResultStatus;
using TaskRequest = ArmoniK.Core.Common.gRPC.Services.TaskRequest;
using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

namespace ArmoniK.Core.Common.Tests.Submitter;

[TestFixture]
public class SubmitterTests
{
  [SetUp]
  public async Task SetUp()
  {
    pushQueueStorage_ = new SimplePushQueueStorage();

    provider_ = new TestDatabaseProvider(collection => collection.AddSingleton<IPushQueueStorage>(pushQueueStorage_)
                                                                 .AddSingleton<ISubmitter, gRPC.Services.Submitter>());

    partitionTable_ = provider_.GetRequiredService<IPartitionTable>();
    sessionTable_   = provider_.GetRequiredService<ISessionTable>();
    submitter_      = provider_.GetRequiredService<ISubmitter>();
    taskTable_      = provider_.GetRequiredService<ITaskTable>();
    resultTable_    = provider_.GetRequiredService<IResultTable>();

    await partitionTable_.CreatePartitionsAsync(new[]
                                                {
                                                  new PartitionData(DefaultPartition,
                                                                    new List<string>(),
                                                                    10,
                                                                    50,
                                                                    20,
                                                                    1,
                                                                    new PodConfiguration(new Dictionary<string, string>())),
                                                })
                         .ConfigureAwait(false);
  }

  [TearDown]
  public virtual void TearDown()
    => provider_?.Dispose();

  private                 ISubmitter?             submitter_;
  private static readonly string                  ExpectedOutput1  = "ExpectedOutput1";
  private static readonly string                  ExpectedOutput2  = "ExpectedOutput2";
  private static readonly string                  ExpectedOutput3  = "ExpectedOutput3";
  private static readonly string                  ExpectedOutput4  = "ExpectedOutput4";
  private static readonly string                  ExpectedOutput5  = "ExpectedOutput5";
  private static readonly string                  ExpectedOutput6  = "ExpectedOutput6";
  private static readonly string                  DefaultPartition = "DefaultPartition";
  private                 ISessionTable?          sessionTable_;
  private                 ITaskTable?             taskTable_;
  private                 IPartitionTable?        partitionTable_;
  private                 IResultTable?           resultTable_;
  private                 SimplePushQueueStorage? pushQueueStorage_;
  private                 TestDatabaseProvider?   provider_;


  public static readonly TaskOptions DefaultTaskOptionsPart1 = new()
                                                               {
                                                                 MaxDuration = Duration.FromTimeSpan(TimeSpan.FromSeconds(2)),
                                                                 MaxRetries  = 2,
                                                                 Priority    = 1,
                                                                 PartitionId = "part1",
                                                               };

  private static async Task<(SessionData session, string taskCreating, string taskSubmitted)> InitSubmitter(ISubmitter        submitter,
                                                                                                            IPartitionTable   partitionTable,
                                                                                                            IResultTable      resultTable,
                                                                                                            ISessionTable     sessionTable,
                                                                                                            CancellationToken token)
  {
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
                                                   DefaultTaskOptionsPart1.ToTaskOptions(),
                                                   token)
                                    .ConfigureAwait(false)).SessionId;

    var sessionData = await sessionTable.GetSessionAsync(sessionId,
                                                         token)
                                        .ConfigureAwait(false);

    await resultTable.Create(new[]
                             {
                               new Result(sessionId,
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
                               new Result(sessionId,
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
                               new Result(sessionId,
                                          ExpectedOutput6,
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
                             token)
                     .ConfigureAwait(false);

    var taskCreating = (await submitter.CreateTasks(sessionId,
                                                    sessionId,
                                                    DefaultTaskOptionsPart1.ToTaskOptions(),
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
                                       .ConfigureAwait(false)).First()
                                                              .TaskId;

    var requests = await submitter.CreateTasks(sessionId,
                                               sessionId,
                                               DefaultTaskOptionsPart1.ToTaskOptions(),
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

    var taskSubmitted = requests.First()
                                .TaskId;

    await submitter.FinalizeTaskCreation(requests,
                                         sessionData,
                                         sessionId,
                                         CancellationToken.None)
                   .ConfigureAwait(false);

    var requests2 = await submitter.CreateTasks(sessionId,
                                                sessionId,
                                                null,
                                                new List<TaskRequest>
                                                {
                                                  new(new List<string>
                                                      {
                                                        ExpectedOutput6,
                                                      },
                                                      new List<string>(),
                                                      new List<ReadOnlyMemory<byte>>
                                                      {
                                                        new(Encoding.ASCII.GetBytes("AAAA")),
                                                      }.ToAsyncEnumerable()),
                                                }.ToAsyncEnumerable(),
                                                CancellationToken.None)
                                   .ConfigureAwait(false);

    await submitter.FinalizeTaskCreation(requests2,
                                         sessionData,
                                         sessionId,
                                         CancellationToken.None)
                   .ConfigureAwait(false);

    return (sessionData, taskCreating, taskSubmitted);
  }

  private static async Task<string> InitSubmitterCompleteTask(ISubmitter        submitter,
                                                              ITaskTable        taskTable,
                                                              IResultTable      resultTable,
                                                              SessionData       session,
                                                              CancellationToken token)
  {
    var defaultTaskOptions = new TaskOptions
                             {
                               MaxDuration = Duration.FromTimeSpan(TimeSpan.FromSeconds(2)),
                               MaxRetries  = 2,
                               Priority    = 1,
                               PartitionId = "part2",
                             };

    await resultTable.Create(new[]
                             {
                               new Result(session.SessionId,
                                          ExpectedOutput3,
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
                             token)
                     .ConfigureAwait(false);

    var requests = await submitter.CreateTasks(session.SessionId,
                                               session.SessionId,
                                               defaultTaskOptions.ToTaskOptions(),
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

    var taskCompletedId = requests.First()
                                  .TaskId;

    await submitter.FinalizeTaskCreation(requests,
                                         session,
                                         session.SessionId,
                                         token)
                   .ConfigureAwait(false);

    var taskData = new TaskData(session.SessionId,
                                taskCompletedId,
                                "OwnerPodId",
                                "OwnerPodName",
                                "PayloadId",
                                "CreatedBy",
                                new List<string>(),
                                new List<string>(),
                                new List<string>
                                {
                                  ExpectedOutput3,
                                },
                                new List<string>(),
                                TaskStatus.Completed,
                                defaultTaskOptions.ToTaskOptions(),
                                new Output(OutputStatus.Error,
                                           ""));

    await taskTable.AcquireTask(taskData,
                                token)
                   .ConfigureAwait(false);

    await taskTable.StartTask(taskData,
                              token)
                   .ConfigureAwait(false);

    await submitter.SetResult(session.SessionId,
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
                                      session,
                                      true,
                                      new Output(OutputStatus.Success,
                                                 string.Empty),
                                      token)
                   .ConfigureAwait(false);

    return taskCompletedId;
  }


  [Test]
  public async Task CreateSessionShouldSucceed()
  {
    var (session, _, _) = await InitSubmitter(submitter_!,
                                              partitionTable_!,
                                              resultTable_!,
                                              sessionTable_!,
                                              CancellationToken.None)
                            .ConfigureAwait(false);

    var result = await sessionTable_!.ListSessionsAsync(new SessionFilter
                                                        {
                                                          Sessions =
                                                          {
                                                            session.SessionId,
                                                          },
                                                        },
                                                        CancellationToken.None)
                                     .ToListAsync()
                                     .ConfigureAwait(false);

    Assert.AreEqual(session.SessionId,
                    result.Single());
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
                                                                                               defaultTaskOptions.ToTaskOptions(),
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
                                                                                               defaultTaskOptions.ToTaskOptions(),
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
                                                       defaultTaskOptions.ToTaskOptions(),
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
                                                   resultTable_!,
                                                   sessionTable_!,
                                                   CancellationToken.None)
                                 .ConfigureAwait(false);

    var result = await taskTable_!.ListTasksAsync(new TaskFilter
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
                                  .ToListAsync()
                                  .ConfigureAwait(false);

    Assert.AreEqual(taskCreating,
                    result.Single());
  }

  [Test]
  public async Task CreateTaskWithoutPartitionShouldSucceed()
  {
    var _ = await InitSubmitter(submitter_!,
                                partitionTable_!,
                                resultTable_!,
                                sessionTable_!,
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
                                                     taskOptions.ToTaskOptions(),
                                                     CancellationToken.None)
                                      .ConfigureAwait(false)).SessionId;

    var requests = await submitter_!.CreateTasks(sessionId,
                                                 sessionId,
                                                 taskOptions.ToTaskOptions(),
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
                    requests.Select(request => request.Options.PartitionId)
                            .Distinct()
                            .Single());

    var result = await taskTable_!.ListTasksAsync(new TaskFilter
                                                  {
                                                    Task = new TaskFilter.Types.IdsRequest
                                                           {
                                                             Ids =
                                                             {
                                                               requests.Single()
                                                                       .TaskId,
                                                             },
                                                           },
                                                  },
                                                  CancellationToken.None)
                                  .ToListAsync()
                                  .ConfigureAwait(false);

    Assert.AreEqual(requests.Single()
                            .TaskId,
                    result.Single());
  }

  [Test]
  public async Task CreateTaskPausedSessionShouldSucceed([Values] bool pause)
  {
    var _ = await InitSubmitter(submitter_!,
                                partitionTable_!,
                                resultTable_!,
                                sessionTable_!,
                                CancellationToken.None)
              .ConfigureAwait(false);
    pushQueueStorage_.Messages.Clear();

    var sessionId = (await submitter_!.CreateSession(new List<string>
                                                     {
                                                       "part1",
                                                     },
                                                     DefaultTaskOptionsPart1.ToTaskOptions(),
                                                     CancellationToken.None)
                                      .ConfigureAwait(false)).SessionId;

    if (pause)
    {
      await TaskLifeCycleHelper.PauseAsync(taskTable_!,
                                           sessionTable_!,
                                           sessionId)
                               .ConfigureAwait(false);
    }

    var sessionData = await sessionTable_!.GetSessionAsync(sessionId)
                                          .ConfigureAwait(false);

    var requests = await submitter_!.CreateTasks(sessionData.SessionId,
                                                 sessionData.SessionId,
                                                 DefaultTaskOptionsPart1.ToTaskOptions(),
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

    var taskId = requests.Select(request => request.TaskId)
                         .Single();
    var taskData = await taskTable_!.ReadTaskAsync(taskId)
                                    .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Creating,
                    taskData.Status);

    await submitter_.FinalizeTaskCreation(requests,
                                          sessionData,
                                          sessionData.SessionId,
                                          CancellationToken.None)
                    .ConfigureAwait(false);

    taskData = await taskTable_!.ReadTaskAsync(taskId)
                                .ConfigureAwait(false);

    Assert.AreEqual(pause
                      ? TaskStatus.Paused
                      : TaskStatus.Submitted,
                    taskData.Status);

    if (pause)
    {
      Assert.AreEqual(0,
                      pushQueueStorage_.Messages.Count);
      await TaskLifeCycleHelper.ResumeAsync(taskTable_!,
                                            sessionTable_!,
                                            pushQueueStorage_,
                                            sessionData.SessionId)
                               .ConfigureAwait(false);
    }

    taskData = await taskTable_!.ReadTaskAsync(taskId)
                                .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Submitted,
                    taskData.Status);

    Assert.AreEqual(1,
                    pushQueueStorage_.Messages.Count);
  }

  [Test]
  public async Task CreateTaskWithoutSessionShouldThrow()
  {
    var _ = await InitSubmitter(submitter_!,
                                partitionTable_!,
                                resultTable_!,
                                sessionTable_!,
                                CancellationToken.None)
              .ConfigureAwait(false);

    Assert.ThrowsAsync<SessionNotFoundException>(() => submitter_!.CreateTasks("invalidSession",
                                                                               "parenttaskid",
                                                                               null,
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
                                                                               CancellationToken.None));
  }

  [Test]
  public async Task CreateTaskWithoutPartitionForTasksShouldSucceed()
  {
    var _ = await InitSubmitter(submitter_!,
                                partitionTable_!,
                                resultTable_!,
                                sessionTable_!,
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
                                                     sessionTaskOptions.ToTaskOptions(),
                                                     CancellationToken.None)
                                      .ConfigureAwait(false)).SessionId;

    var requests = await submitter_!.CreateTasks(sessionId,
                                                 sessionId,
                                                 taskOptions.ToTaskOptions(),
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
                    requests.Select(request => request.Options.PartitionId)
                            .Distinct()
                            .Single());

    var result = await taskTable_!.ListTasksAsync(new TaskFilter
                                                  {
                                                    Task = new TaskFilter.Types.IdsRequest
                                                           {
                                                             Ids =
                                                             {
                                                               requests.Single()
                                                                       .TaskId,
                                                             },
                                                           },
                                                  },
                                                  CancellationToken.None)
                                  .ToListAsync()
                                  .ConfigureAwait(false);

    Assert.AreEqual(requests.Single()
                            .TaskId,
                    result.Single());
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
                                                                                   defaultTaskOptions.ToTaskOptions(),
                                                                                   CancellationToken.None));
    return Task.CompletedTask;
  }

  [Test]
  public async Task CompleteTimeoutTaskShouldSucceed()
  {
    var (session, _, taskSubmitted) = await InitSubmitter(submitter_!,
                                                          partitionTable_!,
                                                          resultTable_!,
                                                          sessionTable_!,
                                                          CancellationToken.None)
                                        .ConfigureAwait(false);

    var taskData = await taskTable_!.ReadTaskAsync(taskSubmitted)
                                    .ConfigureAwait(false);

    await taskTable_!.AcquireTask(taskData,
                                  CancellationToken.None)
                     .ConfigureAwait(false);

    taskData = await taskTable_!.ReadTaskAsync(taskSubmitted)
                                .ConfigureAwait(false);

    await taskTable_!.StartTask(taskData,
                                CancellationToken.None)
                     .ConfigureAwait(false);

    taskData = await taskTable_!.ReadTaskAsync(taskSubmitted)
                                .ConfigureAwait(false);

    await submitter_!.CompleteTaskAsync(taskData,
                                        session,
                                        false,
                                        new Output(OutputStatus.Timeout,
                                                   "deadline exceeded"),
                                        CancellationToken.None)
                     .ConfigureAwait(false);

    taskData = await taskTable_!.ReadTaskAsync(taskSubmitted)
                                .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Timeout,
                    taskData.Status);

    var result = await resultTable_!.GetResult(taskData.ExpectedOutputIds.First())
                                    .ConfigureAwait(false);

    Assert.AreEqual(ResultStatus.Aborted,
                    result.Status);
    Assert.NotNull(result.CompletionDate);
  }

  [Test]
  public async Task FinalizeTaskCreationShouldSucceed()
  {
    var (_, _, taskSubmitted) = await InitSubmitter(submitter_!,
                                                    partitionTable_!,
                                                    resultTable_!,
                                                    sessionTable_!,
                                                    CancellationToken.None)
                                  .ConfigureAwait(false);

    var result = await taskTable_!.GetTaskStatus(taskSubmitted,
                                                 CancellationToken.None)
                                  .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Submitted,
                    result);
  }

  [Test]
  public async Task TryGetResultShouldSucceed()
  {
    var (session, _, _) = await InitSubmitter(submitter_!,
                                              partitionTable_!,
                                              resultTable_!,
                                              sessionTable_!,
                                              CancellationToken.None)
                            .ConfigureAwait(false);

    await InitSubmitterCompleteTask(submitter_!,
                                    taskTable_!,
                                    resultTable_!,
                                    session,
                                    CancellationToken.None)
      .ConfigureAwait(false);

    var writer = new TestHelperServerStreamWriter<ResultReply>();

    var resultRequest = new ResultRequest
                        {
                          ResultId = ExpectedOutput3,
                          Session  = session.SessionId,
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
    Assert.IsTrue(writer.Messages[1]
                        .Result.DataComplete);
  }

  [Test]
  public async Task TryGetResultShouldFail()
  {
    var (session, _, _) = await InitSubmitter(submitter_!,
                                              partitionTable_!,
                                              resultTable_!,
                                              sessionTable_!,
                                              CancellationToken.None)
                            .ConfigureAwait(false);

    var writer = new TestHelperServerStreamWriter<ResultReply>();

    Assert.ThrowsAsync<ResultNotFoundException>(() => submitter_!.TryGetResult(new ResultRequest
                                                                               {
                                                                                 ResultId = "NotExistingResult",
                                                                                 Session  = session.SessionId,
                                                                               },
                                                                               writer,
                                                                               CancellationToken.None));
  }

  [Test]
  public async Task TryGetResultWithNotCompletedTaskShouldReturnNotCompletedTaskReply()
  {
    var (session, _, _) = await InitSubmitter(submitter_!,
                                              partitionTable_!,
                                              resultTable_!,
                                              sessionTable_!,
                                              CancellationToken.None)
                            .ConfigureAwait(false);

    var writer = new TestHelperServerStreamWriter<ResultReply>();

    await submitter_!.TryGetResult(new ResultRequest
                                   {
                                     ResultId = ExpectedOutput2,
                                     Session  = session.SessionId,
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
    var (session, _, _) = await InitSubmitter(submitter_!,
                                              partitionTable_!,
                                              resultTable_!,
                                              sessionTable_!,
                                              CancellationToken.None)
                            .ConfigureAwait(false);

    await submitter_!.CancelSession(session.SessionId,
                                    CancellationToken.None)
                     .ConfigureAwait(false);

    Assert.IsTrue(await sessionTable_!.IsSessionCancelledAsync(session.SessionId,
                                                               CancellationToken.None)
                                      .ConfigureAwait(false));
  }

  [Test]
  public async Task GetPartitionTaskStatus()
  {
    var (session, _, _) = await InitSubmitter(submitter_!,
                                              partitionTable_!,
                                              resultTable_!,
                                              sessionTable_!,
                                              CancellationToken.None)
                            .ConfigureAwait(false);

    await InitSubmitterCompleteTask(submitter_!,
                                    taskTable_!,
                                    resultTable_!,
                                    session,
                                    CancellationToken.None)
      .ConfigureAwait(false);

    var result = (await taskTable_!.CountPartitionTasksAsync(CancellationToken.None)
                                   .ConfigureAwait(false)).OrderBy(r => r.Status)
                                                          .ThenBy(r => r.PartitionId)
                                                          .AsIList();

    Assert.AreEqual(3,
                    result.Count);
    Assert.AreEqual(new PartitionTaskStatusCount("part1",
                                                 TaskStatus.Creating,
                                                 1),
                    result[0]);
    Assert.AreEqual(new PartitionTaskStatusCount("part1",
                                                 TaskStatus.Submitted,
                                                 2),
                    result[1]);
    Assert.AreEqual(new PartitionTaskStatusCount("part2",
                                                 TaskStatus.Completed,
                                                 1),
                    result[2]);
  }

  [Test]
  public async Task SetTaskError()
  {
    var (session, _, _) = await InitSubmitter(submitter_!,
                                              partitionTable_!,
                                              resultTable_!,
                                              sessionTable_!,
                                              CancellationToken.None)
                            .ConfigureAwait(false);

    await resultTable_!.Create(new[]
                               {
                                 new Result(session.SessionId,
                                            ExpectedOutput4,
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
                                 new Result(session.SessionId,
                                            ExpectedOutput5,
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
                       .ConfigureAwait(false);

    var requests = await submitter_!.CreateTasks(session.SessionId,
                                                 session.SessionId,
                                                 DefaultTaskOptionsPart1.ToTaskOptions(),
                                                 new List<TaskRequest>
                                                 {
                                                   new(new[]
                                                       {
                                                         ExpectedOutput4,
                                                       },
                                                       new List<string>(),
                                                       new List<ReadOnlyMemory<byte>>
                                                       {
                                                         new(Encoding.ASCII.GetBytes("AAAA")),
                                                       }.ToAsyncEnumerable()),
                                                   new(new[]
                                                       {
                                                         ExpectedOutput5,
                                                       },
                                                       new[]
                                                       {
                                                         ExpectedOutput4,
                                                       },
                                                       new List<ReadOnlyMemory<byte>>
                                                       {
                                                         new(Encoding.ASCII.GetBytes("AAAA")),
                                                       }.ToAsyncEnumerable()),
                                                 }.ToAsyncEnumerable(),
                                                 CancellationToken.None)
                                    .ConfigureAwait(false);

    var abortedTask = requests.First()
                              .TaskId;
    var taskWithDependencies = requests.Last()
                                       .TaskId;

    await submitter_.FinalizeTaskCreation(requests,
                                          session,
                                          session.SessionId,
                                          CancellationToken.None)
                    .ConfigureAwait(false);

    var taskData = await taskTable_!.ReadTaskAsync(abortedTask,
                                                   CancellationToken.None)
                                    .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Submitted,
                    taskData.Status);

    await submitter_.CompleteTaskAsync(taskData,
                                       session,
                                       false,
                                       new Output(OutputStatus.Error,
                                                  "This error should be propagated to other tasks"))
                    .ConfigureAwait(false);

    taskData = await taskTable_!.ReadTaskAsync(abortedTask,
                                               CancellationToken.None)
                                .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Error,
                    taskData.Status);

    taskData = await taskTable_!.ReadTaskAsync(taskWithDependencies,
                                               CancellationToken.None)
                                .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Error,
                    taskData.Status);
    Assert.AreEqual($"Task {abortedTask} failed:\nThis error should be propagated to other tasks",
                    taskData.Output.Error);
  }


  [Test]
  public async Task CreatedByShouldBeEmpty()
  {
    var (session, _, _) = await InitSubmitter(submitter_!,
                                              partitionTable_!,
                                              resultTable_!,
                                              sessionTable_!,
                                              CancellationToken.None)
                            .ConfigureAwait(false);

    await resultTable_!.Create(new[]
                               {
                                 new Result(session.SessionId,
                                            ExpectedOutput4,
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
                       .ConfigureAwait(false);

    var requests = await submitter_!.CreateTasks(session.SessionId,
                                                 session.SessionId,
                                                 DefaultTaskOptionsPart1.ToTaskOptions(),
                                                 new List<TaskRequest>
                                                 {
                                                   new(new[]
                                                       {
                                                         ExpectedOutput4,
                                                       },
                                                       new List<string>(),
                                                       new List<ReadOnlyMemory<byte>>
                                                       {
                                                         new(Encoding.ASCII.GetBytes("AAAA")),
                                                       }.ToAsyncEnumerable()),
                                                 }.ToAsyncEnumerable(),
                                                 CancellationToken.None)
                                    .ConfigureAwait(false);


    var task = requests.Single()
                       .TaskId;

    var taskData = await taskTable_!.ReadTaskAsync(task,
                                                   CancellationToken.None)
                                    .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Creating,
                    taskData.Status);

    Assert.AreEqual(string.Empty,
                    taskData.CreatedBy);
    var resultData = await resultTable_!.GetResult(taskData.PayloadId)
                                        .ConfigureAwait(false);

    Assert.AreEqual(string.Empty,
                    resultData.CreatedBy);
  }


  [Test]
  public async Task CreatedByShouldBeFilled()
  {
    var (session, _, taskSubmitted) = await InitSubmitter(submitter_!,
                                                          partitionTable_!,
                                                          resultTable_!,
                                                          sessionTable_!,
                                                          CancellationToken.None)
                                        .ConfigureAwait(false);

    await resultTable_!.Create(new[]
                               {
                                 new Result(session.SessionId,
                                            ExpectedOutput4,
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
                       .ConfigureAwait(false);

    var requests = await submitter_!.CreateTasks(session.SessionId,
                                                 taskSubmitted,
                                                 DefaultTaskOptionsPart1.ToTaskOptions(),
                                                 new List<TaskRequest>
                                                 {
                                                   new(new[]
                                                       {
                                                         ExpectedOutput4,
                                                       },
                                                       new List<string>(),
                                                       new List<ReadOnlyMemory<byte>>
                                                       {
                                                         new(Encoding.ASCII.GetBytes("AAAA")),
                                                       }.ToAsyncEnumerable()),
                                                 }.ToAsyncEnumerable(),
                                                 CancellationToken.None)
                                    .ConfigureAwait(false);

    var task = requests.Single()
                       .TaskId;

    var taskData = await taskTable_!.ReadTaskAsync(task,
                                                   CancellationToken.None)
                                    .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Creating,
                    taskData.Status);

    Assert.AreEqual(taskSubmitted,
                    taskData.CreatedBy);

    var resultData = await resultTable_!.GetResult(taskData.PayloadId)
                                        .ConfigureAwait(false);

    Assert.AreEqual(taskSubmitted,
                    resultData.CreatedBy);
  }
}
