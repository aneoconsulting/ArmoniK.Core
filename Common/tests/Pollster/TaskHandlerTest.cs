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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Agent;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Api.gRPC.V1.Worker;
using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Stream.Worker;
using ArmoniK.Core.Common.Tests.Helpers;
using ArmoniK.Core.Common.Utils;

using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using NUnit.Framework;

using Output = ArmoniK.Core.Common.Storage.Output;
using Result = ArmoniK.Api.gRPC.V1.Agent.Result;
using TaskOptions = ArmoniK.Core.Common.Storage.TaskOptions;
using TaskRequest = ArmoniK.Core.Common.gRPC.Services.TaskRequest;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Tests.Pollster;

[TestFixture]
public class TaskHandlerTest
{
  [SetUp]
  public void SetUp()
  {
  }

  [TearDown]
  public virtual void TearDown()
  {
  }

  [Test]
  public void InitializeTaskHandler()
  {
    var mockStreamHandler       = new Mock<IWorkerStreamHandler>();
    var mockQueueMessageHandler = new Mock<IQueueMessageHandler>();
    var mockAgentHandler        = new Mock<IAgentHandler>();
    using var testServiceProvider = new TestTaskHandlerProvider(mockStreamHandler.Object,
                                                                mockAgentHandler.Object,
                                                                mockQueueMessageHandler.Object,
                                                                new CancellationTokenSource());
    Assert.IsNotNull(testServiceProvider.TaskHandler);
  }

  [Test]
  // Mocks are not initialized so it is expected that the acquisition should not work
  public async Task AcquireTaskShouldFail()
  {
    var mockStreamHandler       = new Mock<IWorkerStreamHandler>();
    var mockQueueMessageHandler = new Mock<IQueueMessageHandler>();
    var mockAgentHandler        = new Mock<IAgentHandler>();
    using var testServiceProvider = new TestTaskHandlerProvider(mockStreamHandler.Object,
                                                                mockAgentHandler.Object,
                                                                mockQueueMessageHandler.Object,
                                                                new CancellationTokenSource());

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.IsFalse(acquired);
  }

  private async Task<(string taskId, string taskUnresolvedDepId, string taskErrorId, string sessionId)> InitProviderRunnableTask(
    TestTaskHandlerProvider testServiceProvider)
  {
    var taskRequests = new List<TaskRequest>();
    taskRequests.Add(new TaskRequest(new List<string>
                                     {
                                       "ExpectedOutput0",
                                     },
                                     new List<string>(),
                                     new List<ReadOnlyMemory<byte>>
                                     {
                                       ReadOnlyMemory<byte>.Empty,
                                     }.ToAsyncEnumerable()));

    taskRequests.Add(new TaskRequest(new List<string>
                                     {
                                       "ExpectedOutput1",
                                     },
                                     new List<string>
                                     {
                                       "DataDep",
                                     },
                                     new List<ReadOnlyMemory<byte>>
                                     {
                                       ReadOnlyMemory<byte>.Empty,
                                     }.ToAsyncEnumerable()));

    taskRequests.Add(new TaskRequest(new List<string>
                                     {
                                       "ExpectedOutput2",
                                     },
                                     new List<string>
                                     {
                                       "DataDep",
                                     },
                                     new List<ReadOnlyMemory<byte>>
                                     {
                                       ReadOnlyMemory<byte>.Empty,
                                     }.ToAsyncEnumerable()));

    await testServiceProvider.PartitionTable.CreatePartitionsAsync(new[]
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
                                                                   })
                             .ConfigureAwait(false);

    var sessionId = (await testServiceProvider.Submitter.CreateSession(new[]
                                                                       {
                                                                         "part1",
                                                                         "part2",
                                                                       },
                                                                       new Api.gRPC.V1.TaskOptions
                                                                       {
                                                                         MaxDuration = Duration.FromTimeSpan(TimeSpan.FromMinutes(2)),
                                                                         MaxRetries  = 2,
                                                                         Priority    = 1,
                                                                         PartitionId = "part1",
                                                                       },
                                                                       CancellationToken.None)
                                              .ConfigureAwait(false)).SessionId;

    var (requestsIEnumerable, priority, whichPartitionId) = await testServiceProvider.Submitter.CreateTasks(sessionId,
                                                                                                            sessionId,
                                                                                                            new Api.gRPC.V1.TaskOptions
                                                                                                            {
                                                                                                              MaxDuration =
                                                                                                                Duration.FromTimeSpan(TimeSpan.FromMinutes(2)),
                                                                                                              MaxRetries  = 2,
                                                                                                              Priority    = 1,
                                                                                                              PartitionId = "part1",
                                                                                                            },
                                                                                                            taskRequests.ToAsyncEnumerable(),
                                                                                                            CancellationToken.None)
                                                                                     .ConfigureAwait(false);
    var requests = requestsIEnumerable.ToList();
    await testServiceProvider.Submitter.FinalizeTaskCreation(requests,
                                                             priority,
                                                             whichPartitionId,
                                                             sessionId,
                                                             sessionId,
                                                             CancellationToken.None)
                             .ConfigureAwait(false);

    var taskId = requests.First()
                         .Id;
    requests.RemoveAt(0);


    var taskUnresolvedDepId = requests.First()
                                      .Id;
    requests.RemoveAt(0);

    var taskErrorId = requests.First()
                              .Id;

    var taskErrorData = await testServiceProvider.TaskTable.ReadTaskAsync(taskErrorId,
                                                                          CancellationToken.None)
                                                 .ConfigureAwait(false);

    await testServiceProvider.Submitter.CompleteTaskAsync(taskErrorData,
                                                          true,
                                                          new Api.gRPC.V1.Output
                                                          {
                                                            Error = new Api.gRPC.V1.Output.Types.Error
                                                                    {
                                                                      Details = "Created for testing tasks in error",
                                                                    },
                                                          })
                             .ConfigureAwait(false);

    return (taskId, taskUnresolvedDepId, taskErrorId, sessionId);
  }

  [Test]
  public async Task AcquireTaskShouldSucceed()
  {
    var sqmh = new SimpleQueueMessageHandler
               {
                 CancellationToken = CancellationToken.None,
                 Status            = QueueMessageStatus.Waiting,
                 MessageId = Guid.NewGuid()
                                 .ToString(),
               };

    var mockStreamHandler = new Mock<IWorkerStreamHandler>();
    var mockAgentHandler  = new Mock<IAgentHandler>();
    using var testServiceProvider = new TestTaskHandlerProvider(mockStreamHandler.Object,
                                                                mockAgentHandler.Object,
                                                                sqmh,
                                                                new CancellationTokenSource());

    var (taskId, _, _, _) = await InitProviderRunnableTask(testServiceProvider)
                              .ConfigureAwait(false);

    sqmh.TaskId = taskId;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.IsTrue(acquired);
    Assert.AreEqual(taskId,
                    testServiceProvider.TaskHandler.GetAcquiredTask());
  }

  [Test]
  public async Task AcquireCancelingTaskShouldFail()
  {
    var sqmh = new SimpleQueueMessageHandler
               {
                 CancellationToken = CancellationToken.None,
                 Status            = QueueMessageStatus.Waiting,
                 MessageId = Guid.NewGuid()
                                 .ToString(),
               };

    var mockStreamHandler = new Mock<IWorkerStreamHandler>();
    var mockAgentHandler  = new Mock<IAgentHandler>();
    using var testServiceProvider = new TestTaskHandlerProvider(mockStreamHandler.Object,
                                                                mockAgentHandler.Object,
                                                                sqmh,
                                                                new CancellationTokenSource());

    var (taskId, _, _, sessionId) = await InitProviderRunnableTask(testServiceProvider)
                                      .ConfigureAwait(false);

    await testServiceProvider.Submitter.CancelSession(sessionId,
                                                      CancellationToken.None)
                             .ConfigureAwait(false);

    sqmh.TaskId = taskId;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.IsFalse(acquired);
    Assert.AreEqual(taskId,
                    testServiceProvider.TaskHandler.GetAcquiredTask());
  }

  [Test]
  public async Task AcquireErrorTaskShouldFail()
  {
    var sqmh = new SimpleQueueMessageHandler
               {
                 CancellationToken = CancellationToken.None,
                 Status            = QueueMessageStatus.Waiting,
                 MessageId = Guid.NewGuid()
                                 .ToString(),
               };

    var mockStreamHandler = new Mock<IWorkerStreamHandler>();
    var mockAgentHandler  = new Mock<IAgentHandler>();
    using var testServiceProvider = new TestTaskHandlerProvider(mockStreamHandler.Object,
                                                                mockAgentHandler.Object,
                                                                sqmh,
                                                                new CancellationTokenSource());

    var (_, _, taskId, _) = await InitProviderRunnableTask(testServiceProvider)
                              .ConfigureAwait(false);

    sqmh.TaskId = taskId;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.IsFalse(acquired);
    Assert.AreEqual(taskId,
                    testServiceProvider.TaskHandler.GetAcquiredTask());
  }

  [Test]
  [TestCase(TaskStatus.Creating)]
  [TestCase(TaskStatus.Completed)]
  [TestCase(TaskStatus.Cancelled)]
  public async Task AcquireStatusShouldFail(TaskStatus status)
  {
    var sqmh = new SimpleQueueMessageHandler
               {
                 CancellationToken = CancellationToken.None,
                 Status            = QueueMessageStatus.Waiting,
                 MessageId = Guid.NewGuid()
                                 .ToString(),
               };

    var mockStreamHandler = new Mock<IWorkerStreamHandler>();
    var mockAgentHandler  = new Mock<IAgentHandler>();
    using var testServiceProvider = new TestTaskHandlerProvider(mockStreamHandler.Object,
                                                                mockAgentHandler.Object,
                                                                sqmh,
                                                                new CancellationTokenSource());

    var (_, _, _, sessionId) = await InitProviderRunnableTask(testServiceProvider)
                                 .ConfigureAwait(false);

    var taskData = new TaskData(sessionId,
                                Guid.NewGuid()
                                    .ToString(),
                                "ownerpodid",
                                "ownerpodname",
                                "payload",
                                new List<string>(),
                                new List<string>(),
                                new List<string>(),
                                "init",
                                new List<string>(),
                                status,
                                "",
                                new TaskOptions(new Dictionary<string, string>(),
                                                TimeSpan.FromMinutes(4),
                                                2,
                                                1,
                                                "part1",
                                                "",
                                                "",
                                                "",
                                                "",
                                                ""),
                                DateTime.Now,
                                null,
                                null,
                                null,
                                null,
                                null,
                                null,
                                new Output(false,
                                           ""));

    await testServiceProvider.TaskTable.CreateTasks(new[]
                                                    {
                                                      taskData,
                                                    })
                             .ConfigureAwait(false);


    sqmh.TaskId = taskData.TaskId;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.IsFalse(acquired);
    Assert.AreEqual(taskData.TaskId,
                    testServiceProvider.TaskHandler.GetAcquiredTask());
  }

  public class WaitTaskTable : ITaskTable
  {
    public enum WaitMethod
    {
      Read,
      Acquire,
    }

    private readonly int        delay_;
    private readonly WaitMethod waitMethod_;

    public WaitTaskTable(WaitMethod waitMethod,
                         int        delay)
    {
      waitMethod_ = waitMethod;
      delay_      = delay;
      Logger      = NullLogger.Instance;
    }

    public Task<HealthCheckResult> Check(HealthCheckTag tag)
      => Task.FromResult(HealthCheckResult.Healthy());

    public Task Init(CancellationToken cancellationToken)
      => Task.CompletedTask;

    public TimeSpan PollingDelayMin { get; }
    public TimeSpan PollingDelayMax { get; }
    public ILogger  Logger          { get; }

    public Task CreateTasks(IEnumerable<TaskData> tasks,
                            CancellationToken     cancellationToken = default)
      => Task.CompletedTask;

    public async Task<TaskData> ReadTaskAsync(string            taskId,
                                              CancellationToken cancellationToken = default)
    {
      if (waitMethod_ == WaitMethod.Read)
      {
        await Task.Delay(delay_)
                  .ConfigureAwait(false);
      }

      return new TaskData("SessionId",
                          "taskId",
                          "ownerpodid",
                          "ownerpodname",
                          "payload",
                          new List<string>(),
                          new List<string>(),
                          new List<string>(),
                          "taskId",
                          new List<string>(),
                          TaskStatus.Submitted,
                          "",
                          new TaskOptions(new Dictionary<string, string>(),
                                          TimeSpan.FromMinutes(2),
                                          2,
                                          3,
                                          "part",
                                          "",
                                          "",
                                          "",
                                          "",
                                          ""),
                          DateTime.Now,
                          DateTime.Now,
                          DateTime.Now,
                          DateTime.Now,
                          DateTime.Now,
                          DateTime.Now,
                          DateTime.Now,
                          new Output(false,
                                     ""));
    }

    public Task UpdateTaskStatusAsync(string            id,
                                      TaskStatus        status,
                                      CancellationToken cancellationToken = default)
      => throw new NotImplementedException();

    public Task<int> UpdateAllTaskStatusAsync(TaskFilter        filter,
                                              TaskStatus        status,
                                              CancellationToken cancellationToken = default)
      => throw new NotImplementedException();

    public Task<bool> IsTaskCancelledAsync(string            taskId,
                                           CancellationToken cancellationToken = default)
      => throw new NotImplementedException();

    public Task StartTask(string            taskId,
                          CancellationToken cancellationToken = default)
      => throw new NotImplementedException();

    public Task CancelSessionAsync(string            sessionId,
                                   CancellationToken cancellationToken = default)
      => throw new NotImplementedException();

    public Task<IList<TaskData>> CancelTaskAsync(ICollection<string> taskIds,
                                                 CancellationToken   cancellationToken = default)
      => throw new NotImplementedException();

    public Task<IEnumerable<TaskStatusCount>> CountTasksAsync(TaskFilter        filter,
                                                              CancellationToken cancellationToken = default)
      => throw new NotImplementedException();

    public Task<IEnumerable<TaskStatusCount>> CountTasksAsync(Expression<Func<TaskData, bool>> filter,
                                                              CancellationToken                cancellationToken = default)
      => throw new NotImplementedException();

    public Task<IEnumerable<PartitionTaskStatusCount>> CountPartitionTasksAsync(CancellationToken cancellationToken = default)
      => throw new NotImplementedException();

    public Task<int> CountAllTasksAsync(TaskStatus        status,
                                        CancellationToken cancellationToken = default)
      => throw new NotImplementedException();

    public Task DeleteTaskAsync(string            id,
                                CancellationToken cancellationToken = default)
      => throw new NotImplementedException();

    public IAsyncEnumerable<string> ListTasksAsync(TaskFilter        filter,
                                                   CancellationToken cancellationToken)
      => throw new NotImplementedException();

    public Task<(IEnumerable<TaskData> tasks, int totalCount)> ListTasksAsync(Expression<Func<TaskData, bool>>    filter,
                                                                              Expression<Func<TaskData, object?>> orderField,
                                                                              bool                                ascOrder,
                                                                              int                                 page,
                                                                              int                                 pageSize,
                                                                              CancellationToken                   cancellationToken = default)
      => throw new NotImplementedException();

    public Task<(IEnumerable<Application> applications, int totalCount)> ListApplicationsAsync(Expression<Func<TaskData, bool>> filter,
                                                                                               ICollection<Expression<Func<Application, object?>>> orderFields,
                                                                                               bool ascOrder,
                                                                                               int page,
                                                                                               int pageSize,
                                                                                               CancellationToken cancellationToken = default)
      => throw new NotImplementedException();

    public Task SetTaskSuccessAsync(string            taskId,
                                    CancellationToken cancellationToken)
      => throw new NotImplementedException();

    public Task SetTaskCanceledAsync(string            taskId,
                                     CancellationToken cancellationToken)
      => throw new NotImplementedException();

    public Task<bool> SetTaskErrorAsync(string            taskId,
                                        string            errorDetail,
                                        CancellationToken cancellationToken)
      => Task.FromResult(true);

    public Task<Output> GetTaskOutput(string            taskId,
                                      CancellationToken cancellationToken = default)
      => throw new NotImplementedException();

    public async Task<TaskData> AcquireTask(string            taskId,
                                            string            ownerPodId,
                                            string            ownerPodName,
                                            DateTime          receptionDate,
                                            CancellationToken cancellationToken = default)
    {
      if (waitMethod_ == WaitMethod.Acquire)
      {
        await Task.Delay(delay_)
                  .ConfigureAwait(false);
      }

      return new TaskData("SessionId",
                          taskId,
                          ownerPodId,
                          ownerPodName,
                          "payload",
                          new List<string>(),
                          new List<string>(),
                          new List<string>(),
                          "taskId",
                          new List<string>(),
                          TaskStatus.Dispatched,
                          "",
                          new TaskOptions(new Dictionary<string, string>(),
                                          TimeSpan.FromMinutes(2),
                                          2,
                                          3,
                                          "part",
                                          "",
                                          "",
                                          "",
                                          "",
                                          ""),
                          DateTime.Now,
                          DateTime.Now,
                          DateTime.Now,
                          DateTime.Now,
                          receptionDate,
                          DateTime.Now,
                          DateTime.Now,
                          new Output(false,
                                     ""));
    }

    public Task<TaskData> ReleaseTask(string            taskId,
                                      string            ownerPodId,
                                      CancellationToken cancellationToken = default)
      => Task.FromResult(new TaskData("SessionId",
                                      taskId,
                                      ownerPodId,
                                      ownerPodId,
                                      "payload",
                                      new List<string>(),
                                      new List<string>(),
                                      new List<string>(),
                                      "taskId",
                                      new List<string>(),
                                      TaskStatus.Submitted,
                                      "",
                                      new TaskOptions(new Dictionary<string, string>(),
                                                      TimeSpan.FromMinutes(2),
                                                      2,
                                                      3,
                                                      "part",
                                                      "",
                                                      "",
                                                      "",
                                                      "",
                                                      ""),
                                      DateTime.Now,
                                      DateTime.Now,
                                      DateTime.Now,
                                      DateTime.Now,
                                      DateTime.Now,
                                      DateTime.Now,
                                      DateTime.Now,
                                      new Output(false,
                                                 "")));

    public Task<IEnumerable<GetTaskStatusReply.Types.IdStatus>> GetTaskStatus(IEnumerable<string> taskId,
                                                                              CancellationToken   cancellationToken = default)
      => throw new NotImplementedException();

    public IAsyncEnumerable<(string taskId, IEnumerable<string> expectedOutputKeys)> GetTasksExpectedOutputKeys(IEnumerable<string> taskIds,
                                                                                                                CancellationToken   cancellationToken = default)
      => taskIds.Select(s => (s, new[]
                                 {
                                   "",
                                 }.AsEnumerable()))
                .ToAsyncEnumerable();

    public Task<IEnumerable<string>> GetParentTaskIds(string            taskId,
                                                      CancellationToken cancellationToken)
      => throw new NotImplementedException();

    public Task<string> RetryTask(TaskData          taskData,
                                  CancellationToken cancellationToken)
      => Task.FromResult(Guid.NewGuid()
                             .ToString());

    public Task<int> FinalizeTaskCreation(IEnumerable<string> taskIds,
                                          CancellationToken   cancellationToken = default)
      => Task.FromResult(1);
  }

  public class WaitSessionTable : ISessionTable
  {
    private readonly int          delay_;
    private          SessionData? sessionData_;

    public WaitSessionTable(int delay)
    {
      delay_ = delay;
      Logger = NullLogger.Instance;
    }

    public Task<HealthCheckResult> Check(HealthCheckTag tag)
      => Task.FromResult(HealthCheckResult.Healthy());

    public Task Init(CancellationToken cancellationToken)
      => Task.CompletedTask;

    public ILogger Logger { get; }

    public Task<string> SetSessionDataAsync(IEnumerable<string> partitionIds,
                                            TaskOptions         defaultOptions,
                                            CancellationToken   cancellationToken = default)
    {
      sessionData_ = new SessionData(Guid.NewGuid()
                                         .ToString(),
                                     SessionStatus.Running,
                                     DateTime.Now,
                                     null,
                                     partitionIds.ToList(),
                                     defaultOptions);
      return Task.FromResult(sessionData_.SessionId);
    }

    public async Task<SessionData> GetSessionAsync(string            sessionId,
                                                   CancellationToken cancellationToken = default)
    {
      await Task.Delay(delay_)
                .ConfigureAwait(false);
      return sessionData_!;
    }

    public Task<bool> IsSessionCancelledAsync(string            sessionId,
                                              CancellationToken cancellationToken = default)
      => throw new NotImplementedException();

    public Task<TaskOptions> GetDefaultTaskOptionAsync(string            sessionId,
                                                       CancellationToken cancellationToken = default)
      => throw new NotImplementedException();

    public Task<SessionData> CancelSessionAsync(string            sessionId,
                                                CancellationToken cancellationToken = default)
      => throw new NotImplementedException();

    public Task DeleteSessionAsync(string            sessionId,
                                   CancellationToken cancellationToken = default)
      => throw new NotImplementedException();

    public IAsyncEnumerable<string> ListSessionsAsync(SessionFilter     request,
                                                      CancellationToken cancellationToken = default)
      => throw new NotImplementedException();

    public Task<(IEnumerable<SessionData> sessions, int totalCount)> ListSessionsAsync(Expression<Func<SessionData, bool>>    filter,
                                                                                       Expression<Func<SessionData, object?>> orderField,
                                                                                       bool                                   ascOrder,
                                                                                       int                                    page,
                                                                                       int                                    pageSize,
                                                                                       CancellationToken                      cancellationToken = default)
      => throw new NotImplementedException();
  }

  [Test]
  [TestCase(WaitTaskTable.WaitMethod.Read,
            1000,
            0)]
  [TestCase(WaitTaskTable.WaitMethod.Acquire,
            1000,
            0)]
  [TestCase(WaitTaskTable.WaitMethod.Read,
            0,
            1000)]
  public async Task AcquireTaskWithCancellationWaitTaskTableShouldFail(WaitTaskTable.WaitMethod waitMethod,
                                                                       int                      delayTaskTable,
                                                                       int                      delaySessionTable)
  {
    var sqmh = new SimpleQueueMessageHandler
               {
                 CancellationToken = CancellationToken.None,
                 Status            = QueueMessageStatus.Waiting,
                 MessageId = Guid.NewGuid()
                                 .ToString(),
               };

    var mockStreamHandler       = new Mock<IWorkerStreamHandler>();
    var mockAgentHandler        = new Mock<IAgentHandler>();
    var cancellationTokenSource = new CancellationTokenSource();
    using var testServiceProvider = new TestTaskHandlerProvider(mockStreamHandler.Object,
                                                                mockAgentHandler.Object,
                                                                sqmh,
                                                                cancellationTokenSource,
                                                                new WaitTaskTable(waitMethod,
                                                                                  delayTaskTable),
                                                                new WaitSessionTable(delaySessionTable));

    var (taskId, _, _, _) = await InitProviderRunnableTask(testServiceProvider)
                              .ConfigureAwait(false);

    sqmh.TaskId = taskId;

    cancellationTokenSource.CancelAfter(TimeSpan.FromMilliseconds(10));
    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.IsFalse(acquired);
  }

  [Test]
  public async Task AcquireTaskFromCancelledSessionShouldFail()
  {
    var sqmh = new SimpleQueueMessageHandler
               {
                 CancellationToken = CancellationToken.None,
                 Status            = QueueMessageStatus.Waiting,
                 MessageId = Guid.NewGuid()
                                 .ToString(),
               };

    var mockStreamHandler = new Mock<IWorkerStreamHandler>();
    var mockAgentHandler  = new Mock<IAgentHandler>();
    using var testServiceProvider = new TestTaskHandlerProvider(mockStreamHandler.Object,
                                                                mockAgentHandler.Object,
                                                                sqmh,
                                                                new CancellationTokenSource());

    var (taskId, _, _, sessionId) = await InitProviderRunnableTask(testServiceProvider)
                                      .ConfigureAwait(false);

    await testServiceProvider.SessionTable.CancelSessionAsync(sessionId,
                                                              CancellationToken.None)
                             .ConfigureAwait(false);

    sqmh.TaskId = taskId;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.IsFalse(acquired);
    Assert.AreEqual(taskId,
                    testServiceProvider.TaskHandler.GetAcquiredTask());
  }

  [Test]
  public async Task AcquireNotReadyTaskShouldFail()
  {
    var sqmh = new SimpleQueueMessageHandler
               {
                 CancellationToken = CancellationToken.None,
                 Status            = QueueMessageStatus.Waiting,
                 MessageId = Guid.NewGuid()
                                 .ToString(),
               };

    var mockStreamHandler = new Mock<IWorkerStreamHandler>();
    var mockAgentHandler  = new Mock<IAgentHandler>();
    using var testServiceProvider = new TestTaskHandlerProvider(mockStreamHandler.Object,
                                                                mockAgentHandler.Object,
                                                                sqmh,
                                                                new CancellationTokenSource());

    var (_, unresolvedDependenciesTask, _, _) = await InitProviderRunnableTask(testServiceProvider)
                                                  .ConfigureAwait(false);

    sqmh.TaskId = unresolvedDependenciesTask;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.IsTrue(acquired);
  }

  public class ExceptionStartWorkerStreamHandler<T> : IWorkerStreamHandler
    where T : Exception, new()
  {
    public Task<HealthCheckResult> Check(HealthCheckTag tag)
      => Task.FromResult(HealthCheckResult.Healthy());

    public Task Init(CancellationToken cancellationToken)
      => Task.CompletedTask;

    public void Dispose()
    {
    }

    public IAsyncPipe<ProcessReply, ProcessRequest>? Pipe { get; private set; }

    public void StartTaskProcessing(TaskData          taskData,
                                    CancellationToken cancellationToken)
      => throw new T();
  }

  public class TestRpcException : RpcException
  {
    public TestRpcException()
      : base(new Status(StatusCode.Aborted,
                        ""))
    {
    }

    public TestRpcException(string message)
      : base(new Status(StatusCode.Aborted,
                        ""),
             message)
    {
    }

    public TestRpcException(Metadata trailers)
      : base(new Status(StatusCode.Aborted,
                        ""),
             trailers)
    {
    }

    public TestRpcException(Metadata trailers,
                            string   message)
      : base(new Status(StatusCode.Aborted,
                        ""),
             trailers,
             message)
    {
    }
  }

  public static IEnumerable TestCaseOuptut
  {
    get
    {
      // trigger error before cancellation so it is a legitimate error and therefor should be considered as such
      yield return new TestCaseData(new ExceptionWorkerStreamHandler<Exception>(0)).Returns((TaskStatus.Error, QueueMessageStatus.Cancelled))
                                                                                   .SetArgDisplayNames("ExceptionError"); // error
      yield return new TestCaseData(new ExceptionWorkerStreamHandler<TestRpcException>(0)).Returns((TaskStatus.Error, QueueMessageStatus.Cancelled))
                                                                                          .SetArgDisplayNames("RpcExceptionResubmit"); // error with resubmit

      // trigger error after cancellation and therefore should be considered as cancelled task and resend into queue
      yield return new TestCaseData(new ExceptionWorkerStreamHandler<Exception>(3000)).Returns((TaskStatus.Submitted, QueueMessageStatus.Postponed))
                                                                                      .SetArgDisplayNames("ExceptionTaskCancellation");
      yield return new TestCaseData(new ExceptionWorkerStreamHandler<TestRpcException>(3000)).Returns((TaskStatus.Submitted, QueueMessageStatus.Postponed))
                                                                                             .SetArgDisplayNames("RpcExceptionTaskCancellation");

      // Worker unavailable during execution should put the task in error
      yield return new TestCaseData(new ExceptionWorkerStreamHandler<TestUnavailableRpcException>(0)).Returns((TaskStatus.Error, QueueMessageStatus.Cancelled))
                                                                                                     .SetArgDisplayNames("UnavailableBeforeCancellation");

      // If the worker becomes unavailable during the task execution after cancellation, the task should be resubmitted
      yield return new TestCaseData(new ExceptionWorkerStreamHandler<TestUnavailableRpcException>(3000)).Returns((TaskStatus.Submitted, QueueMessageStatus.Postponed))
                                                                                                        .SetArgDisplayNames("UnavailableAfterCancellation");
    }
  }

  [Test]
  [TestCaseSource(nameof(TestCaseOuptut))]
  public async Task<(TaskStatus taskStatus, QueueMessageStatus messageStatus)> ExecuteTaskWithExceptionDuringCancellationShouldSucceed<TEx>(
    ExceptionWorkerStreamHandler<TEx> workerStreamHandler)
    where TEx : Exception, new()
  {
    var sqmh = new SimpleQueueMessageHandler
               {
                 CancellationToken = CancellationToken.None,
                 Status            = QueueMessageStatus.Waiting,
                 MessageId = Guid.NewGuid()
                                 .ToString(),
               };

    var agentHandler            = new SimpleAgentHandler();
    var cancellationTokenSource = new CancellationTokenSource();
    using var testServiceProvider = new TestTaskHandlerProvider(workerStreamHandler,
                                                                agentHandler,
                                                                sqmh,
                                                                cancellationTokenSource);

    var (taskId, _, _, _) = await InitProviderRunnableTask(testServiceProvider)
                              .ConfigureAwait(false);

    sqmh.TaskId = taskId;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.IsTrue(acquired);

    await testServiceProvider.TaskHandler.PreProcessing()
                             .ConfigureAwait(false);

    await testServiceProvider.TaskHandler.ExecuteTask()
                             .ConfigureAwait(false);

    cancellationTokenSource.CancelAfter(TimeSpan.FromMilliseconds(1500));

    Assert.ThrowsAsync<TEx>(() => testServiceProvider.TaskHandler.PostProcessing());

    return ((await testServiceProvider.TaskTable.GetTaskStatus(new[]
                                                               {
                                                                 taskId,
                                                               })
                                      .ConfigureAwait(false)).Single()
                                                             .Status, sqmh.Status);
  }

  [Test]
  public async Task ExecuteTaskShouldSucceed()
  {
    var sqmh = new SimpleQueueMessageHandler
               {
                 CancellationToken = CancellationToken.None,
                 Status            = QueueMessageStatus.Waiting,
                 MessageId = Guid.NewGuid()
                                 .ToString(),
               };

    var sh = new SimpleWorkerStreamHandler();

    var agentHandler = new SimpleAgentHandler();
    using var testServiceProvider = new TestTaskHandlerProvider(sh,
                                                                agentHandler,
                                                                sqmh,
                                                                new CancellationTokenSource());

    var (taskId, _, _, _) = await InitProviderRunnableTask(testServiceProvider)
                              .ConfigureAwait(false);

    sqmh.TaskId = taskId;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.IsTrue(acquired);

    await testServiceProvider.TaskHandler.PreProcessing()
                             .ConfigureAwait(false);

    await testServiceProvider.TaskHandler.ExecuteTask()
                             .ConfigureAwait(false);

    await testServiceProvider.TaskHandler.PostProcessing()
                             .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Completed,
                    (await testServiceProvider.TaskTable.GetTaskStatus(new[]
                                                                       {
                                                                         taskId,
                                                                       })
                                              .ConfigureAwait(false)).Single()
                                                                     .Status);

    Assert.AreEqual(QueueMessageStatus.Processed,
                    sqmh.Status);
  }

  public static IEnumerable TestCaseOutputExecuteTaskWithErrorDuringStartInWorkerHandlerShouldThrow
  {
    get
    {
      yield return new TestCaseData(new ExceptionStartWorkerStreamHandler<Exception>()).Returns((TaskStatus.Error, QueueMessageStatus.Processed))
                                                                                       .SetArgDisplayNames("Exception"); // error with resubmit
      yield return new TestCaseData(new ExceptionStartWorkerStreamHandler<TestRpcException>()).Returns((TaskStatus.Error, QueueMessageStatus.Processed))
                                                                                              .SetArgDisplayNames("RpcException"); // error with resubmit
      // error meaning that the worker is unavailable, therefore a requeue is made
      yield return new TestCaseData(new ExceptionStartWorkerStreamHandler<TestUnavailableRpcException>()).Returns((TaskStatus.Submitted, QueueMessageStatus.Postponed))
                                                                                                         .SetArgDisplayNames("RpcUnavailableException");
    }
  }

  [Test]
  [TestCaseSource(nameof(TestCaseOutputExecuteTaskWithErrorDuringStartInWorkerHandlerShouldThrow))]
  public async Task<(TaskStatus taskStatus, QueueMessageStatus messageStatus)> ExecuteTaskWithErrorDuringStartInWorkerHandlerShouldThrow<TEx>(
    ExceptionStartWorkerStreamHandler<TEx> sh)
    where TEx : Exception, new()
  {
    var sqmh = new SimpleQueueMessageHandler
               {
                 CancellationToken = CancellationToken.None,
                 Status            = QueueMessageStatus.Waiting,
                 MessageId = Guid.NewGuid()
                                 .ToString(),
               };

    var agentHandler = new SimpleAgentHandler();
    using var testServiceProvider = new TestTaskHandlerProvider(sh,
                                                                agentHandler,
                                                                sqmh,
                                                                new CancellationTokenSource());

    var (taskId, _, _, _) = await InitProviderRunnableTask(testServiceProvider)
                              .ConfigureAwait(false);

    sqmh.TaskId = taskId;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.IsTrue(acquired);

    await testServiceProvider.TaskHandler.PreProcessing()
                             .ConfigureAwait(false);

    Assert.ThrowsAsync<TEx>(async () => await testServiceProvider.TaskHandler.ExecuteTask()
                                                                 .ConfigureAwait(false));

    var taskData = await testServiceProvider.TaskTable.ReadTaskAsync(taskId,
                                                                     CancellationToken.None)
                                            .ConfigureAwait(false);


    return (taskData.Status, sqmh.Status);
  }


  public static IEnumerable TestCaseOutputExecuteTaskWithErrorDuringExecutionInWorkerHandlerShouldThrow
  {
    get
    {
      yield return new TestCaseData(new ExceptionWorkerStreamHandler<TestRpcException>(100)).SetArgDisplayNames("RpcExceptionResubmit"); // error with resubmit
      yield return
        new TestCaseData(new ExceptionWorkerStreamHandler<TestUnavailableRpcException>(100))
          .SetArgDisplayNames("RpcUnavailableExceptionResubmit"); // error with resubmit
    }
  }

  [Test]
  [TestCaseSource(nameof(TestCaseOutputExecuteTaskWithErrorDuringExecutionInWorkerHandlerShouldThrow))]
  public async Task ExecuteTaskWithErrorDuringExecutionInWorkerHandlerShouldThrow<TEx>(ExceptionWorkerStreamHandler<TEx> sh)
    where TEx : Exception, new()
  {
    var sqmh = new SimpleQueueMessageHandler
               {
                 CancellationToken = CancellationToken.None,
                 Status            = QueueMessageStatus.Waiting,
                 MessageId = Guid.NewGuid()
                                 .ToString(),
               };

    var agentHandler = new SimpleAgentHandler();
    using var testServiceProvider = new TestTaskHandlerProvider(sh,
                                                                agentHandler,
                                                                sqmh,
                                                                new CancellationTokenSource());

    var (taskId, _, _, _) = await InitProviderRunnableTask(testServiceProvider)
                              .ConfigureAwait(false);

    sqmh.TaskId = taskId;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.IsTrue(acquired);

    await testServiceProvider.TaskHandler.PreProcessing()
                             .ConfigureAwait(false);

    await testServiceProvider.TaskHandler.ExecuteTask()
                             .ConfigureAwait(false);

    Assert.ThrowsAsync<TEx>(async () => await testServiceProvider.TaskHandler.PostProcessing()
                                                                 .ConfigureAwait(false));

    var taskData = await testServiceProvider.TaskTable.ReadTaskAsync(taskId,
                                                                     CancellationToken.None)
                                            .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Error,
                    taskData.Status);

    Assert.AreEqual(QueueMessageStatus.Cancelled,
                    sqmh.Status);

    var taskDataRetry = await testServiceProvider.TaskTable.ReadTaskAsync(taskId + "###1",
                                                                          CancellationToken.None)
                                                 .ConfigureAwait(false);
    Assert.AreEqual(taskId,
                    taskDataRetry.InitialTaskId);
  }

  [Test]
  public async Task ExecuteTaskWithResultsShouldSucceed()
  {
    var sqmh = new SimpleQueueMessageHandler
               {
                 CancellationToken = CancellationToken.None,
                 Status            = QueueMessageStatus.Waiting,
                 MessageId = Guid.NewGuid()
                                 .ToString(),
               };

    var sh = new SimpleWorkerStreamHandler();

    var agentHandler = new SimpleAgentHandler();
    using var testServiceProvider = new TestTaskHandlerProvider(sh,
                                                                agentHandler,
                                                                sqmh,
                                                                new CancellationTokenSource());

    var (taskId, _, _, _) = await InitProviderRunnableTask(testServiceProvider)
                              .ConfigureAwait(false);

    sqmh.TaskId = taskId;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.IsTrue(acquired);

    await testServiceProvider.TaskHandler.PreProcessing()
                             .ConfigureAwait(false);

    await testServiceProvider.TaskHandler.ExecuteTask()
                             .ConfigureAwait(false);


    var taskStreamReader = new TestHelperAsyncStreamReader<CreateTaskRequest>(new[]
                                                                              {
                                                                                new CreateTaskRequest(),
                                                                              });
    if (agentHandler.Agent == null)
    {
      throw new NullReferenceException(nameof(agentHandler.Agent));
    }

    await agentHandler.Agent.CreateTask(taskStreamReader,
                                        CancellationToken.None)
                      .ConfigureAwait(false);


    var resultStreamReader = new TestHelperAsyncStreamReader<Result>(new[]
                                                                     {
                                                                       new Result(),
                                                                     });
    await agentHandler.Agent.SendResult(resultStreamReader,
                                        CancellationToken.None)
                      .ConfigureAwait(false);

    await testServiceProvider.TaskHandler.PostProcessing()
                             .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Completed,
                    (await testServiceProvider.TaskTable.GetTaskStatus(new[]
                                                                       {
                                                                         taskId,
                                                                       })
                                              .ConfigureAwait(false)).Single()
                                                                     .Status);
  }

  [Test]
  public async Task CancelLongTaskShouldSucceed()
  {
    var sqmh = new SimpleQueueMessageHandler
               {
                 CancellationToken = CancellationToken.None,
                 Status            = QueueMessageStatus.Waiting,
                 MessageId = Guid.NewGuid()
                                 .ToString(),
               };

    var sh = new ExceptionWorkerStreamHandler<Exception>(15000);

    var agentHandler = new SimpleAgentHandler();
    using var testServiceProvider = new TestTaskHandlerProvider(sh,
                                                                agentHandler,
                                                                sqmh,
                                                                new CancellationTokenSource());

    var (taskId, _, _, _) = await InitProviderRunnableTask(testServiceProvider)
                              .ConfigureAwait(false);

    sqmh.TaskId = taskId;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.IsTrue(acquired);

    await testServiceProvider.TaskHandler.PreProcessing()
                             .ConfigureAwait(false);

    await testServiceProvider.TaskHandler.ExecuteTask()
                             .ConfigureAwait(false);

    // Cancel task for test

    await Task.Delay(TimeSpan.FromMilliseconds(200))
              .ConfigureAwait(false);

    await testServiceProvider.TaskTable.CancelTaskAsync(new List<string>
                                                        {
                                                          taskId,
                                                        },
                                                        CancellationToken.None)
                             .ConfigureAwait(false);

    await Task.Delay(TimeSpan.FromMilliseconds(200))
              .ConfigureAwait(false);

    // Make several calls to ensure that it still works
    await testServiceProvider.TaskHandler.StopCancelledTask()
                             .ConfigureAwait(false);
    await testServiceProvider.TaskHandler.StopCancelledTask()
                             .ConfigureAwait(false);
    await testServiceProvider.TaskHandler.StopCancelledTask()
                             .ConfigureAwait(false);
    await testServiceProvider.TaskHandler.StopCancelledTask()
                             .ConfigureAwait(false);

    await testServiceProvider.TaskHandler.PostProcessing()
                             .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Cancelling,
                    (await testServiceProvider.TaskTable.GetTaskStatus(new[]
                                                                       {
                                                                         taskId,
                                                                       })
                                              .ConfigureAwait(false)).Single()
                                                                     .Status);

    Assert.AreEqual(QueueMessageStatus.Processed,
                    sqmh.Status);
  }
}
