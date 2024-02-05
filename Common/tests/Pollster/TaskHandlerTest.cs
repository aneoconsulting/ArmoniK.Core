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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.Pollster.TaskProcessingChecker;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Stream.Worker;
using ArmoniK.Core.Common.Tests.Helpers;

using Grpc.Core;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using NUnit.Framework;

using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

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

    Assert.AreEqual(21,
                    acquired);
  }

  private static async Task<(string taskId, string taskUnresolvedDepId, string taskErrorId, string taskRetriedId, string sessionId)> InitProviderRunnableTask(
    TestTaskHandlerProvider testServiceProvider)
  {
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
                                                                       new TaskOptions(new Dictionary<string, string>(),
                                                                                       TimeSpan.FromSeconds(1),
                                                                                       5,
                                                                                       1,
                                                                                       "part1",
                                                                                       "",
                                                                                       "",
                                                                                       "",
                                                                                       "",
                                                                                       ""),
                                                                       CancellationToken.None)
                                              .ConfigureAwait(false)).SessionId;

    await testServiceProvider.ResultTable.Create(new[]
                                                 {
                                                   new Result(sessionId,
                                                              "ExpectedOutput0",
                                                              "",
                                                              "",
                                                              ResultStatus.Created,
                                                              new List<string>(),
                                                              DateTime.UtcNow,
                                                              0,
                                                              Array.Empty<byte>()),
                                                   new Result(sessionId,
                                                              "DataDep",
                                                              "",
                                                              "",
                                                              ResultStatus.Created,
                                                              new List<string>(),
                                                              DateTime.UtcNow,
                                                              0,
                                                              Array.Empty<byte>()),
                                                   new Result(sessionId,
                                                              "ExpectedOutput1",
                                                              "",
                                                              "",
                                                              ResultStatus.Created,
                                                              new List<string>(),
                                                              DateTime.UtcNow,
                                                              0,
                                                              Array.Empty<byte>()),
                                                   new Result(sessionId,
                                                              "ExpectedOutput2",
                                                              "",
                                                              "",
                                                              ResultStatus.Created,
                                                              new List<string>(),
                                                              DateTime.UtcNow,
                                                              0,
                                                              Array.Empty<byte>()),
                                                   new Result(sessionId,
                                                              "ExpectedOutput3",
                                                              "",
                                                              "",
                                                              ResultStatus.Created,
                                                              new List<string>(),
                                                              DateTime.UtcNow,
                                                              0,
                                                              Array.Empty<byte>()),
                                                   new Result(sessionId,
                                                              "taskId",
                                                              "",
                                                              "",
                                                              ResultStatus.Created,
                                                              new List<string>(),
                                                              DateTime.UtcNow,
                                                              0,
                                                              Array.Empty<byte>()),
                                                   new Result(sessionId,
                                                              "payload",
                                                              "",
                                                              "",
                                                              ResultStatus.Created,
                                                              new List<string>(),
                                                              DateTime.UtcNow,
                                                              0,
                                                              Array.Empty<byte>()),
                                                 },
                                                 CancellationToken.None)
                             .ConfigureAwait(false);

    var taskRequests = new List<TaskRequest>
                       {
                         new(new List<string>
                             {
                               "ExpectedOutput0",
                             },
                             new List<string>(),
                             new List<ReadOnlyMemory<byte>>
                             {
                               ReadOnlyMemory<byte>.Empty,
                             }.ToAsyncEnumerable()),

                         new(new List<string>
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
                             }.ToAsyncEnumerable()),

                         new(new List<string>
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
                             }.ToAsyncEnumerable()),
                         new(new List<string>
                             {
                               "ExpectedOutput3",
                             },
                             new List<string>
                             {
                               "DataDep",
                             },
                             new List<ReadOnlyMemory<byte>>
                             {
                               ReadOnlyMemory<byte>.Empty,
                             }.ToAsyncEnumerable()),
                       };

    var requests = await testServiceProvider.Submitter.CreateTasks(sessionId,
                                                                   sessionId,
                                                                   new TaskOptions(new Dictionary<string, string>(),
                                                                                   TimeSpan.FromSeconds(1),
                                                                                   5,
                                                                                   1,
                                                                                   "part1",
                                                                                   "",
                                                                                   "",
                                                                                   "",
                                                                                   "",
                                                                                   ""),
                                                                   taskRequests.ToAsyncEnumerable(),
                                                                   CancellationToken.None)
                                            .ConfigureAwait(false);

    await testServiceProvider.Submitter.FinalizeTaskCreation(requests,
                                                             sessionId,
                                                             sessionId,
                                                             CancellationToken.None)
                             .ConfigureAwait(false);

    var taskId = requests.ElementAt(0)
                         .TaskId;

    var taskUnresolvedDepId = requests.ElementAt(1)
                                      .TaskId;

    var taskErrorId = requests.ElementAt(2)
                              .TaskId;

    var taskRetriedId = requests.ElementAt(3)
                                .TaskId;

    var taskErrorData = await testServiceProvider.TaskTable.ReadTaskAsync(taskErrorId,
                                                                          CancellationToken.None)
                                                 .ConfigureAwait(false);

    await testServiceProvider.Submitter.CompleteTaskAsync(taskErrorData,
                                                          false,
                                                          new Output(false,
                                                                     "Created for testing tasks in error"))
                             .ConfigureAwait(false);

    var taskRetriedData = await testServiceProvider.TaskTable.ReadTaskAsync(taskRetriedId,
                                                                            CancellationToken.None)
                                                   .ConfigureAwait(false);

    await testServiceProvider.Submitter.CompleteTaskAsync(taskRetriedData,
                                                          true,
                                                          new Output(false,
                                                                     "Created for testing tasks in error"))
                             .ConfigureAwait(false);

    return (taskId, taskUnresolvedDepId, taskErrorId, taskRetriedId, sessionId);
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

    var (taskId, _, _, _, sessionId) = await InitProviderRunnableTask(testServiceProvider)
                                         .ConfigureAwait(false);

    await testServiceProvider.Submitter.CancelSession(sessionId,
                                                      CancellationToken.None)
                             .ConfigureAwait(false);

    sqmh.TaskId = taskId;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.AreEqual(2,
                    acquired);
    Assert.AreEqual(taskId,
                    testServiceProvider.TaskHandler.GetAcquiredTaskInfo()
                                       .TaskId);
  }

  [Test]
  public async Task AcquireTaskWithCancelledTokenInHandlerShouldFail()
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
    var cts               = new CancellationTokenSource();
    using var testServiceProvider = new TestTaskHandlerProvider(mockStreamHandler.Object,
                                                                mockAgentHandler.Object,
                                                                sqmh,
                                                                cts);

    var (taskId, _, _, _, sessionId) = await InitProviderRunnableTask(testServiceProvider)
                                         .ConfigureAwait(false);

    sqmh.TaskId = taskId;
    cts.Cancel();

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.AreEqual(1,
                    acquired);
    Assert.AreEqual(taskId,
                    testServiceProvider.TaskHandler.GetAcquiredTaskInfo()
                                       .TaskId);
  }

  public static IEnumerable TestCaseAcquireTask
  {
    get
    {
      var taskData = new TaskData("session",
                                  Guid.NewGuid()
                                      .ToString(),
                                  "ownerpodid",
                                  "ownerpodname",
                                  "payload",
                                  new List<string>(),
                                  new List<string>(),
                                  new Dictionary<string, bool>(),
                                  new List<string>(),
                                  "init",
                                  new List<string>(),
                                  TaskStatus.Creating,
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
                                  null,
                                  null,
                                  null,
                                  null,
                                  new Output(false,
                                             ""));

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Dispatched,
                                    },
                                    true).Returns(0)
                                         .SetArgDisplayNames("Dispatched same owner"); // not sure this case should return true

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Submitted,
                                      OwnerPodId = "",
                                    },
                                    true).Returns(0)
                                         .SetArgDisplayNames("Submitted task");
      // 1 is tested
      // 2 is tested

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Completed,
                                    },
                                    true).Returns(3)
                                         .SetArgDisplayNames("Completed task");

      yield return new TestCaseData(taskData,
                                    true).Returns(4)
                                         .SetArgDisplayNames("Creating task");

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Error,
                                    },
                                    true).Returns(5)
                                         .SetArgDisplayNames("Error task");

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Cancelled,
                                    },
                                    true).Returns(6)
                                         .SetArgDisplayNames("Cancelled task");

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Processing,
                                      OwnerPodId = "",
                                    },
                                    true).Returns(7)
                                         .SetArgDisplayNames("Processing task");

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Processing,
                                      OwnerPodId = "another",
                                    },
                                    false).Returns(8)
                                          .SetArgDisplayNames("Processing task on another agent check false");

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Processing,
                                      OwnerPodId = "another",
                                    },
                                    true).Returns(9)
                                         .SetArgDisplayNames("Processing task on another agent check true");

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Processing,
                                    },
                                    true).Returns(10)
                                         .SetArgDisplayNames("Processing task same agent");

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Retried,
                                    },
                                    true).Returns(11)
                                         .SetArgDisplayNames("Retried task");

      // 12 is already tested

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Dispatched,
                                      OwnerPodId = "",
                                    },
                                    true).Returns(15)
                                         .SetArgDisplayNames("Dispatched empty owner");

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Dispatched,
                                      OwnerPodId = "anotherowner",
                                      AcquisitionDate = DateTime.UtcNow + TimeSpan.FromDays(1),
                                    },
                                    false).Returns(16)
                                          .SetArgDisplayNames("Dispatched different owner false check date later");

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Submitted,
                                      OwnerPodId = "anotherownerpodid",
                                    },
                                    true).Returns(18)
                                         .SetArgDisplayNames("Submitted task with another owner");

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Dispatched,
                                      OwnerPodId = "anotherowner",
                                    },
                                    true).Returns(18)
                                         .SetArgDisplayNames("Dispatched different owner");

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Dispatched,
                                      OwnerPodId = "anotherowner",
                                    },
                                    false).Returns(18)
                                          .SetArgDisplayNames("Dispatched different owner false check");

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Dispatched,
                                      OwnerPodId = "anotherowner",
                                      AcquisitionDate = DateTime.UtcNow - TimeSpan.FromSeconds(20),
                                    },
                                    false).Returns(18)
                                          .SetArgDisplayNames("Dispatched different owner false check date before");

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Submitted,
                                    },
                                    true).Returns(19)
                                         .SetArgDisplayNames("Submitted task with same owner");
    }
  }

  [Test]
  [TestCaseSource(nameof(TestCaseAcquireTask))]
  public async Task<int> AcquireStatusShouldFail(TaskData taskData,
                                                 bool     check)
  {
    var sqmh = new SimpleQueueMessageHandler
               {
                 CancellationToken = CancellationToken.None,
                 Status            = QueueMessageStatus.Waiting,
                 MessageId = Guid.NewGuid()
                                 .ToString(),
               };

    var mockStreamHandler         = new Mock<IWorkerStreamHandler>();
    var mockAgentHandler          = new Mock<IAgentHandler>();
    var mockTaskProcessingChecker = new Mock<ITaskProcessingChecker>();
    mockTaskProcessingChecker.Setup(checker => checker.Check(It.IsAny<string>(),
                                                             It.IsAny<string>(),
                                                             It.IsAny<CancellationToken>()))
                             .Returns(Task.FromResult(check));
    using var testServiceProvider = new TestTaskHandlerProvider(mockStreamHandler.Object,
                                                                mockAgentHandler.Object,
                                                                sqmh,
                                                                new CancellationTokenSource(),
                                                                taskProcessingChecker: mockTaskProcessingChecker.Object);

    var (_, _, _, _, sessionId) = await InitProviderRunnableTask(testServiceProvider)
                                    .ConfigureAwait(false);

    taskData = taskData with
               {
                 SessionId = sessionId,
               };

    await testServiceProvider.TaskTable.CreateTasks(new[]
                                                    {
                                                      taskData,
                                                    })
                             .ConfigureAwait(false);


    sqmh.TaskId = taskData.TaskId;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.AreEqual(taskData.TaskId,
                    testServiceProvider.TaskHandler.GetAcquiredTaskInfo()
                                       .TaskId);

    return acquired;
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

    public Task<(IEnumerable<T> tasks, long totalCount)> ListTasksAsync<T>(Expression<Func<TaskData, bool>>    filter,
                                                                           Expression<Func<TaskData, object?>> orderField,
                                                                           Expression<Func<TaskData, T>>       selector,
                                                                           bool                                ascOrder,
                                                                           int                                 page,
                                                                           int                                 pageSize,
                                                                           CancellationToken                   cancellationToken = default)
      => throw new NotImplementedException();

    public IAsyncEnumerable<T> FindTasksAsync<T>(Expression<Func<TaskData, bool>> filter,
                                                 Expression<Func<TaskData, T>>    selector,
                                                 CancellationToken                cancellationToken = default)
      => throw new NotImplementedException();

    public async Task<TaskData?> UpdateOneTask(string                                                                        taskId,
                                               Expression<Func<TaskData, bool>>?                                             filter,
                                               ICollection<(Expression<Func<TaskData, object?>> selector, object? newValue)> updates,
                                               bool                                                                          before,
                                               CancellationToken                                                             cancellationToken = default)
    {
      if (waitMethod_ == WaitMethod.Acquire)
      {
        await Task.Delay(delay_,
                         cancellationToken)
                  .ConfigureAwait(false);
      }

      return new TaskData("SessionId",
                          taskId,
                          "OwnerPodId",
                          "OwnerPodName",
                          "payload",
                          new List<string>(),
                          new List<string>(),
                          new Dictionary<string, bool>(),
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
                          DateTime.UtcNow,
                          DateTime.Now,
                          DateTime.Now,
                          DateTime.Now,
                          TimeSpan.FromSeconds(1),
                          TimeSpan.FromSeconds(2),
                          TimeSpan.FromSeconds(3),
                          new Output(false,
                                     ""));
    }

    public Task<long> UpdateManyTasks(Expression<Func<TaskData, bool>>                                              filter,
                                      ICollection<(Expression<Func<TaskData, object?>> selector, object? newValue)> updates,
                                      CancellationToken                                                             cancellationToken = default)
      => Task.FromResult<long>(1);

    public Task<(IEnumerable<Application> applications, int totalCount)> ListApplicationsAsync(Expression<Func<TaskData, bool>> filter,
                                                                                               ICollection<Expression<Func<Application, object?>>> orderFields,
                                                                                               bool ascOrder,
                                                                                               int page,
                                                                                               int pageSize,
                                                                                               CancellationToken cancellationToken = default)
      => throw new NotImplementedException();

    public Task RemoveRemainingDataDependenciesAsync(ICollection<string> taskId,
                                                     ICollection<string> dependenciesToRemove,
                                                     CancellationToken   cancellationToken = default)
      => Task.CompletedTask;
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
                                     true,
                                     true,
                                     DateTime.Now,
                                     null,
                                     null,
                                     null,
                                     null,
                                     partitionIds.ToList(),
                                     defaultOptions);
      return Task.FromResult(sessionData_.SessionId);
    }

    public async IAsyncEnumerable<T> FindSessionsAsync<T>(Expression<Func<SessionData, bool>>        filter,
                                                          Expression<Func<SessionData, T>>           selector,
                                                          [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
      await Task.Delay(delay_,
                       cancellationToken)
                .ConfigureAwait(false);
      var enumerable = new[]
        {
          sessionData_!,
        }.Select(selector.Compile())
         .ToAsyncEnumerable();

      await foreach (var d in enumerable.ConfigureAwait(false))
      {
        yield return d;
      }
    }

    public Task DeleteSessionAsync(string            sessionId,
                                   CancellationToken cancellationToken = default)
      => throw new NotImplementedException();

    public Task<(IEnumerable<SessionData> sessions, long totalCount)> ListSessionsAsync(Expression<Func<SessionData, bool>>    filter,
                                                                                        Expression<Func<SessionData, object?>> orderField,
                                                                                        bool                                   ascOrder,
                                                                                        int                                    page,
                                                                                        int                                    pageSize,
                                                                                        CancellationToken                      cancellationToken = default)
      => throw new NotImplementedException();

    public Task<SessionData?> UpdateOneSessionAsync(string                                                                           sessionId,
                                                    Expression<Func<SessionData, bool>>?                                             filter,
                                                    ICollection<(Expression<Func<SessionData, object?>> selector, object? newValue)> updates,
                                                    bool                                                                             before            = false,
                                                    CancellationToken                                                                cancellationToken = default)
      => throw new NotImplementedException();

    public Task<SessionData> CancelSessionAsync(string            sessionId,
                                                CancellationToken cancellationToken = default)
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

    var (taskId, _, _, _, _) = await InitProviderRunnableTask(testServiceProvider)
                                 .ConfigureAwait(false);

    sqmh.TaskId = taskId;

    cancellationTokenSource.CancelAfter(TimeSpan.FromMilliseconds(10));
    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.AreNotEqual(0,
                       acquired);
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

    var (taskId, _, _, _, sessionId) = await InitProviderRunnableTask(testServiceProvider)
                                         .ConfigureAwait(false);

    await testServiceProvider.SessionTable.CancelSessionAsync(sessionId,
                                                              CancellationToken.None)
                             .ConfigureAwait(false);

    sqmh.TaskId = taskId;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.AreEqual(12,
                    acquired);
    Assert.AreEqual(taskId,
                    testServiceProvider.TaskHandler.GetAcquiredTaskInfo()
                                       .TaskId);
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

    var (_, unresolvedDependenciesTask, _, _, _) = await InitProviderRunnableTask(testServiceProvider)
                                                     .ConfigureAwait(false);

    sqmh.TaskId = unresolvedDependenciesTask;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.AreEqual(4,
                    acquired);
  }

  public class TestRpcException : RpcException
  {
    public TestRpcException()
      : base(new Status(StatusCode.Internal,
                        ""))
    {
    }

    public TestRpcException(string message)
      : base(new Status(StatusCode.Internal,
                        ""),
             message)
    {
    }

    public TestRpcException(Metadata trailers)
      : base(new Status(StatusCode.Internal,
                        ""),
             trailers)
    {
    }

    public TestRpcException(Metadata trailers,
                            string   message)
      : base(new Status(StatusCode.Internal,
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
      yield return new TestCaseData(new ExceptionWorkerStreamHandler<Exception>(0)).Returns((TaskStatus.Retried, QueueMessageStatus.Cancelled))
                                                                                   .SetArgDisplayNames("ExceptionError"); // error
      yield return new TestCaseData(new ExceptionWorkerStreamHandler<TestRpcException>(0)).Returns((TaskStatus.Retried, QueueMessageStatus.Cancelled))
                                                                                          .SetArgDisplayNames("RpcExceptionResubmit"); // error with resubmit

      // trigger error after cancellation and therefore should be considered as cancelled task and resend into queue
      yield return new TestCaseData(new ExceptionWorkerStreamHandler<Exception>(1000,
                                                                                false)).Returns((TaskStatus.Submitted, QueueMessageStatus.Postponed))
                                                                                       .SetArgDisplayNames("ExceptionTaskCancellation");
      yield return new TestCaseData(new ExceptionWorkerStreamHandler<TestRpcException>(1000,
                                                                                       false)).Returns((TaskStatus.Submitted, QueueMessageStatus.Postponed))
                                                                                              .SetArgDisplayNames("RpcExceptionTaskCancellation");

      // If the worker becomes unavailable during the task execution after cancellation, the task should be resubmitted
      yield return new TestCaseData(new ExceptionWorkerStreamHandler<TestUnavailableRpcException>(1000,
                                                                                                  false)).Returns((TaskStatus.Submitted, QueueMessageStatus.Postponed))
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
                                                                cancellationTokenSource,
                                                                graceDelay: TimeSpan.FromMilliseconds(10));

    var (taskId, _, _, _, _) = await InitProviderRunnableTask(testServiceProvider)
                                 .ConfigureAwait(false);

    sqmh.TaskId = taskId;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.AreEqual(0,
                    acquired);

    await testServiceProvider.TaskHandler.PreProcessing()
                             .ConfigureAwait(false);

    cancellationTokenSource.CancelAfter(TimeSpan.FromMilliseconds(500));

    Assert.ThrowsAsync<TEx>(async () => await testServiceProvider.TaskHandler.ExecuteTask()
                                                                 .ConfigureAwait(false));

    return (await testServiceProvider.TaskTable.GetTaskStatus(taskId,
                                                              CancellationToken.None)
                                     .ConfigureAwait(false), sqmh.Status);
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

    var (taskId, _, _, _, _) = await InitProviderRunnableTask(testServiceProvider)
                                 .ConfigureAwait(false);

    sqmh.TaskId = taskId;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.AreEqual(0,
                    acquired);

    await testServiceProvider.TaskHandler.PreProcessing()
                             .ConfigureAwait(false);

    await testServiceProvider.TaskHandler.ExecuteTask()
                             .ConfigureAwait(false);

    await testServiceProvider.TaskHandler.PostProcessing()
                             .ConfigureAwait(false);

    var taskData = await testServiceProvider.TaskTable.ReadTaskAsync(taskId,
                                                                     CancellationToken.None)
                                            .ConfigureAwait(false);

    Console.WriteLine(taskData);

    Assert.AreEqual(TaskStatus.Completed,
                    taskData.Status);
    Assert.IsNotNull(taskData.StartDate);
    Assert.IsNotNull(taskData.EndDate);
    Assert.IsNotNull(taskData.ProcessingToEndDuration);
    Assert.IsNotNull(taskData.CreationToEndDuration);
    Assert.Greater(taskData.CreationToEndDuration,
                   taskData.ProcessingToEndDuration);

    Assert.AreEqual(QueueMessageStatus.Processed,
                    sqmh.Status);
  }


  [Test]
  public async Task ExecuteTaskUntilErrorShouldSucceed()
  {
    var sqmh = new SimpleQueueMessageHandler
               {
                 CancellationToken = CancellationToken.None,
                 Status            = QueueMessageStatus.Waiting,
                 MessageId = Guid.NewGuid()
                                 .ToString(),
               };

    var sh = new ExceptionWorkerStreamHandler<TestRpcException>(0);

    var agentHandler = new SimpleAgentHandler();
    using var testServiceProvider = new TestTaskHandlerProvider(sh,
                                                                agentHandler,
                                                                sqmh,
                                                                new CancellationTokenSource());

    var (taskId, _, _, _, _) = await InitProviderRunnableTask(testServiceProvider)
                                 .ConfigureAwait(false);

    var taskData = await testServiceProvider.TaskTable.ReadTaskAsync(taskId,
                                                                     CancellationToken.None)
                                            .ConfigureAwait(false);

    var maxRetries    = taskData.Options.MaxRetries;
    var initialTaskId = taskId;

    for (var i = 0; i < maxRetries + 1; i++)
    {
      sqmh.TaskId = taskId;

      var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                              .ConfigureAwait(false);

      Assert.AreEqual(0,
                      acquired);

      await testServiceProvider.TaskHandler.PreProcessing()
                               .ConfigureAwait(false);


      Assert.ThrowsAsync<TestRpcException>(async () =>
                                           {
                                             await testServiceProvider.TaskHandler.ExecuteTask()
                                                                      .ConfigureAwait(false);

                                             await testServiceProvider.TaskHandler.PostProcessing()
                                                                      .ConfigureAwait(false);
                                           });


      taskData = await testServiceProvider.TaskTable.ReadTaskAsync(taskId,
                                                                   CancellationToken.None)
                                          .ConfigureAwait(false);

      Console.WriteLine(taskData);

      Assert.AreEqual(i != maxRetries
                        ? TaskStatus.Retried
                        : TaskStatus.Error,
                      taskData.Status);
      Assert.Greater(taskData.CreationToEndDuration,
                     taskData.ProcessingToEndDuration);

      Assert.AreEqual(QueueMessageStatus.Cancelled,
                      sqmh.Status);

      var retries = await testServiceProvider.TaskTable.FindTasksAsync(data => data.InitialTaskId == initialTaskId,
                                                                       data => new
                                                                               {
                                                                                 data.RetryOfIds,
                                                                                 data.TaskId,
                                                                               })
                                             .ToListAsync()
                                             .ConfigureAwait(false);

      var lastRetry = retries.MaxBy(arg => arg.RetryOfIds.Count)!;

      // i == maxRetries means we are running the task that will be in error
      // therefore there is no new task that can be retry
      Assert.AreEqual(i != maxRetries
                        ? i + 1
                        : i,
                      lastRetry.RetryOfIds.Count);

      taskId = lastRetry.TaskId;
    }
  }

  [Test]
  public async Task ExecuteTaskWithErrorDuringExecutionInWorkerHandlerShouldThrow()
  {
    var sh = new ExceptionWorkerStreamHandler<TestRpcException>(100);

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

    var (taskId, _, _, _, _) = await InitProviderRunnableTask(testServiceProvider)
                                 .ConfigureAwait(false);

    sqmh.TaskId = taskId;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.AreEqual(0,
                    acquired);

    await testServiceProvider.TaskHandler.PreProcessing()
                             .ConfigureAwait(false);

    Assert.ThrowsAsync<TestRpcException>(async () => await testServiceProvider.TaskHandler.ExecuteTask()
                                                                              .ConfigureAwait(false));

    var taskData = await testServiceProvider.TaskTable.ReadTaskAsync(taskId,
                                                                     CancellationToken.None)
                                            .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Retried,
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

    var (taskId, _, _, _, _) = await InitProviderRunnableTask(testServiceProvider)
                                 .ConfigureAwait(false);

    sqmh.TaskId = taskId;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.AreEqual(0,
                    acquired);

    await testServiceProvider.TaskHandler.PreProcessing()
                             .ConfigureAwait(false);

    await testServiceProvider.TaskHandler.ExecuteTask()
                             .ConfigureAwait(false);

    if (agentHandler.Agent is null)
    {
      throw new NullReferenceException(nameof(agentHandler.Agent));
    }

    await testServiceProvider.TaskHandler.PostProcessing()
                             .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Completed,
                    await testServiceProvider.TaskTable.GetTaskStatus(taskId)
                                             .ConfigureAwait(false));
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

    var (taskId, _, _, _, _) = await InitProviderRunnableTask(testServiceProvider)
                                 .ConfigureAwait(false);

    sqmh.TaskId = taskId;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.AreEqual(0,
                    acquired);

    await testServiceProvider.TaskHandler.PreProcessing()
                             .ConfigureAwait(false);

    var exec = testServiceProvider.TaskHandler.ExecuteTask();

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

    Assert.ThrowsAsync<TaskCanceledException>(() => exec);

    Assert.AreEqual(TaskStatus.Cancelling,
                    await testServiceProvider.TaskTable.GetTaskStatus(taskId)
                                             .ConfigureAwait(false));

    Assert.AreEqual(QueueMessageStatus.Processed,
                    sqmh.Status);
  }
}
