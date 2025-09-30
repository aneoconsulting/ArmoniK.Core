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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Base.Exceptions;
using ArmoniK.Core.Common.Exceptions;
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
                                                                mockQueueMessageHandler.Object);
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
                                                                mockQueueMessageHandler.Object);

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.AreEqual(AcquisitionStatus.TaskNotFound,
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
                                                              "DataDep",
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
                                                              "ExpectedOutput1",
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
                                                              "ExpectedOutput2",
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
                                                              "ExpectedOutput3",
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
                                                              "taskId",
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
                                                              "payload",
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

    var sessionData = await testServiceProvider.SessionTable.GetSessionAsync(sessionId,
                                                                             CancellationToken.None)
                                               .ConfigureAwait(false);

    await testServiceProvider.Submitter.FinalizeTaskCreation(requests,
                                                             sessionData,
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
                                                          sessionData,
                                                          false,
                                                          new Output(OutputStatus.Error,
                                                                     "Created for testing tasks in error"))
                             .ConfigureAwait(false);

    var taskRetriedData = await testServiceProvider.TaskTable.ReadTaskAsync(taskRetriedId,
                                                                            CancellationToken.None)
                                                   .ConfigureAwait(false);

    await testServiceProvider.Submitter.CompleteTaskAsync(taskRetriedData,
                                                          sessionData,
                                                          true,
                                                          new Output(OutputStatus.Error,
                                                                     "Created for testing tasks in error"))
                             .ConfigureAwait(false);

    return (taskId, taskUnresolvedDepId, taskErrorId, taskRetriedId, sessionId);
  }

  private static async Task InitRetry(TestTaskHandlerProvider testServiceProvider,
                                      string                  sessionId)
  {
    var sessionData = await testServiceProvider.SessionTable.GetSessionAsync(sessionId,
                                                                             CancellationToken.None)
                                               .ConfigureAwait(false);

    var options = new TaskOptions(new Dictionary<string, string>(),
                                  TimeSpan.FromSeconds(1),
                                  5,
                                  1,
                                  "part1",
                                  "",
                                  "",
                                  "",
                                  "",
                                  "");

    var results = new List<Result>
                  {
                    new(sessionId,
                        Guid.NewGuid()
                            .ToString(),
                        "Payload1",
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
                        "Result1",
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

    await testServiceProvider.ResultTable.Create(results)
                             .ConfigureAwait(false);

    var tasks = new List<TaskCreationRequest>
                {
                  new("TaskRetry2",
                      results[0]
                        .ResultId,
                      options,
                      new List<string>
                      {
                        results[1]
                          .ResultId,
                      },
                      new List<string>()),
                  new("TaskRetry2+Creating",
                      results[0]
                        .ResultId,
                      options,
                      new List<string>
                      {
                        results[1]
                          .ResultId,
                      },
                      new List<string>()),
                  new("TaskRetry2+Submitted",
                      results[0]
                        .ResultId,
                      options,
                      new List<string>
                      {
                        results[1]
                          .ResultId,
                      },
                      new List<string>()),
                  new("TaskRetry2+NotFound",
                      results[0]
                        .ResultId,
                      options,
                      new List<string>
                      {
                        results[1]
                          .ResultId,
                      },
                      new List<string>()),
                  new("TaskRetry2+Pending",
                      results[0]
                        .ResultId,
                      options,
                      new List<string>
                      {
                        results[1]
                          .ResultId,
                      },
                      new List<string>()),
                };

    await TaskLifeCycleHelper.CreateTasks(testServiceProvider.TaskTable,
                                          testServiceProvider.ResultTable,
                                          sessionId,
                                          sessionId,
                                          tasks,
                                          testServiceProvider.Logger)
                             .ConfigureAwait(false);

    await TaskLifeCycleHelper.FinalizeTaskCreation(testServiceProvider.TaskTable,
                                                   testServiceProvider.ResultTable,
                                                   testServiceProvider.PushQueueStorage,
                                                   tasks,
                                                   sessionData,
                                                   sessionId,
                                                   testServiceProvider.Logger)
                             .ConfigureAwait(false);

    var taskData = await testServiceProvider.TaskTable.ReadTaskAsync("TaskRetry2")
                                            .ConfigureAwait(false);

    await testServiceProvider.TaskTable.SetTaskRetryAsync(taskData,
                                                          "Error for test : retried")
                             .ConfigureAwait(false);

    var newTaskId = await testServiceProvider.TaskTable.RetryTask(taskData)
                                             .ConfigureAwait(false);

    await TaskLifeCycleHelper.FinalizeTaskCreation(testServiceProvider.TaskTable,
                                                   testServiceProvider.ResultTable,
                                                   testServiceProvider.PushQueueStorage,
                                                   new List<TaskCreationRequest>
                                                   {
                                                     new(newTaskId,
                                                         taskData.PayloadId,
                                                         taskData.Options,
                                                         taskData.ExpectedOutputIds,
                                                         taskData.DataDependencies),
                                                   },
                                                   sessionData,
                                                   taskData.TaskId,
                                                   testServiceProvider.Logger)
                             .ConfigureAwait(false);

    var retryData = await testServiceProvider.TaskTable.ReadTaskAsync(newTaskId)
                                             .ConfigureAwait(false);
    await testServiceProvider.TaskTable.AcquireTask(retryData)
                             .ConfigureAwait(false);

    taskData = await testServiceProvider.TaskTable.ReadTaskAsync("TaskRetry2+Submitted")
                                        .ConfigureAwait(false);

    await testServiceProvider.TaskTable.SetTaskRetryAsync(taskData,
                                                          "Error for test : submitted")
                             .ConfigureAwait(false);

    newTaskId = await testServiceProvider.TaskTable.RetryTask(taskData)
                                         .ConfigureAwait(false);

    await TaskLifeCycleHelper.FinalizeTaskCreation(testServiceProvider.TaskTable,
                                                   testServiceProvider.ResultTable,
                                                   testServiceProvider.PushQueueStorage,
                                                   new List<TaskCreationRequest>
                                                   {
                                                     new(newTaskId,
                                                         taskData.PayloadId,
                                                         taskData.Options,
                                                         taskData.ExpectedOutputIds,
                                                         taskData.DataDependencies),
                                                   },
                                                   sessionData,
                                                   taskData.TaskId,
                                                   testServiceProvider.Logger)
                             .ConfigureAwait(false);


    taskData = await testServiceProvider.TaskTable.ReadTaskAsync("TaskRetry2+Creating")
                                        .ConfigureAwait(false);

    await testServiceProvider.TaskTable.SetTaskRetryAsync(taskData,
                                                          "Error for test : creating")
                             .ConfigureAwait(false);

    newTaskId = await testServiceProvider.TaskTable.RetryTask(taskData)
                                         .ConfigureAwait(false);


    taskData = await testServiceProvider.TaskTable.ReadTaskAsync("TaskRetry2+NotFound")
                                        .ConfigureAwait(false);

    await testServiceProvider.TaskTable.SetTaskRetryAsync(taskData,
                                                          "Error for test : not found")
                             .ConfigureAwait(false);


    taskData = await testServiceProvider.TaskTable.ReadTaskAsync("TaskRetry2+Pending")
                                        .ConfigureAwait(false);

    await testServiceProvider.TaskTable.SetTaskRetryAsync(taskData,
                                                          "Error for test : pending")
                             .ConfigureAwait(false);

    newTaskId = await testServiceProvider.TaskTable.RetryTask(taskData)
                                         .ConfigureAwait(false);

    await testServiceProvider.TaskTable.UpdateOneTask(newTaskId,
                                                      null,
                                                      new UpdateDefinition<TaskData>().Set(data => data.Status,
                                                                                           TaskStatus.Pending))
                             .ConfigureAwait(false);
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
                                                                sqmh);

    var (taskId, _, _, _, sessionId) = await InitProviderRunnableTask(testServiceProvider)
                                         .ConfigureAwait(false);

    await testServiceProvider.Submitter.CancelSession(sessionId,
                                                      CancellationToken.None)
                             .ConfigureAwait(false);

    sqmh.TaskId = taskId;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.AreEqual(AcquisitionStatus.SessionNotExecutable,
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
    using var testServiceProvider = new TestTaskHandlerProvider(mockStreamHandler.Object,
                                                                mockAgentHandler.Object,
                                                                sqmh);

    var (taskId, _, _, _, sessionId) = await InitProviderRunnableTask(testServiceProvider)
                                         .ConfigureAwait(false);

    sqmh.TaskId = taskId;
    testServiceProvider.Lifetime.StopApplication();

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.AreEqual(AcquisitionStatus.CancelledAfterFirstRead,
                    acquired);
    Assert.AreEqual(taskId,
                    testServiceProvider.TaskHandler.GetAcquiredTaskInfo()
                                       .TaskId);
  }

  [Test]
  public async Task AcquireTaskThenReleaseShouldSucceed()
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
                                                                sqmh);

    var (taskId, _, _, _, sessionId) = await InitProviderRunnableTask(testServiceProvider)
                                         .ConfigureAwait(false);

    sqmh.TaskId = taskId;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.AreEqual(AcquisitionStatus.Acquired,
                    acquired);
    Assert.AreEqual(taskId,
                    testServiceProvider.TaskHandler.GetAcquiredTaskInfo()
                                       .TaskId);
    Assert.AreEqual(TaskStatus.Dispatched,
                    testServiceProvider.TaskHandler.GetAcquiredTaskInfo()
                                       .TaskStatus);


    await testServiceProvider.TaskHandler.ReleaseAndPostponeTask()
                             .ConfigureAwait(false);
    Assert.AreEqual(TaskStatus.Submitted,
                    testServiceProvider.TaskHandler.GetAcquiredTaskInfo()
                                       .TaskStatus);
    Assert.AreEqual(QueueMessageStatus.Postponed,
                    sqmh.Status);

    var taskData = await testServiceProvider.TaskTable.ReadTaskAsync(taskId)
                                            .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Submitted,
                    taskData.Status);
    Assert.AreEqual(string.Empty,
                    taskData.OwnerPodId);
    Assert.AreEqual(string.Empty,
                    taskData.OwnerPodName);
  }

  public record struct AcquireTaskReturn(AcquisitionStatus  AcquisitionStatus,
                                         TaskStatus         TaskStatus,
                                         QueueMessageStatus MessageStatus);

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
                                  "createdby",
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
                                  null,
                                  new Output(OutputStatus.Error,
                                             ""));

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Dispatched,
                                    },
                                    true).Returns(new AcquireTaskReturn(AcquisitionStatus.Acquired,
                                                                        TaskStatus.Dispatched,
                                                                        QueueMessageStatus.Waiting))
                                         .SetArgDisplayNames("Dispatched same owner"); // not sure this case should return true

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Submitted,
                                      OwnerPodId = "",
                                    },
                                    true).Returns(new AcquireTaskReturn(AcquisitionStatus.Acquired,
                                                                        TaskStatus.Dispatched,
                                                                        QueueMessageStatus.Waiting))
                                         .SetArgDisplayNames("Submitted task");
      // 1 is tested
      // 2 is tested

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Completed,
                                    },
                                    true).Returns(new AcquireTaskReturn(AcquisitionStatus.TaskIsProcessed,
                                                                        TaskStatus.Completed,
                                                                        QueueMessageStatus.Processed))
                                         .SetArgDisplayNames("Completed task");

      yield return new TestCaseData(taskData,
                                    true).Returns(new AcquireTaskReturn(AcquisitionStatus.TaskIsCreating,
                                                                        TaskStatus.Creating,
                                                                        QueueMessageStatus.Postponed))
                                         .SetArgDisplayNames("Creating task");

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Error,
                                    },
                                    true).Returns(new AcquireTaskReturn(AcquisitionStatus.TaskIsError,
                                                                        TaskStatus.Error,
                                                                        QueueMessageStatus.Cancelled))
                                         .SetArgDisplayNames("Error task");

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Cancelled,
                                    },
                                    true).Returns(new AcquireTaskReturn(AcquisitionStatus.TaskIsCancelled,
                                                                        TaskStatus.Cancelled,
                                                                        QueueMessageStatus.Cancelled))
                                         .SetArgDisplayNames("Cancelled task");

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Processing,
                                      OwnerPodId = "",
                                    },
                                    true).Returns(new AcquireTaskReturn(AcquisitionStatus.TaskIsProcessingPodIdEmpty,
                                                                        TaskStatus.Retried,
                                                                        QueueMessageStatus.Cancelled))
                                         .SetArgDisplayNames("Processing task");

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Processing,
                                      OwnerPodId = "another",
                                    },
                                    false).Returns(new AcquireTaskReturn(AcquisitionStatus.TaskIsProcessingButSeemsCrashed,
                                                                         TaskStatus.Retried,
                                                                         QueueMessageStatus.Processed))
                                          .SetArgDisplayNames("Processing task on another agent check false");

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Processing,
                                      OwnerPodId = "another",
                                    },
                                    true).Returns(new AcquireTaskReturn(AcquisitionStatus.TaskIsProcessingElsewhere,
                                                                        TaskStatus.Processing,
                                                                        QueueMessageStatus.Postponed))
                                         .SetArgDisplayNames("Processing task on another agent check true");

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Processing,
                                    },
                                    true).Returns(new AcquireTaskReturn(AcquisitionStatus.TaskIsProcessingHere,
                                                                        TaskStatus.Processing,
                                                                        QueueMessageStatus.Postponed))
                                         .SetArgDisplayNames("Processing task same agent");

      // 12 is already tested

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Dispatched,
                                      OwnerPodId = "",
                                    },
                                    true).Returns(new AcquireTaskReturn(AcquisitionStatus.PodIdEmptyAfterAcquisition,
                                                                        TaskStatus.Dispatched,
                                                                        QueueMessageStatus.Postponed))
                                         .SetArgDisplayNames("Dispatched empty owner");

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Dispatched,
                                      OwnerPodId = "anotherowner",
                                      AcquisitionDate = DateTime.UtcNow + TimeSpan.FromDays(1),
                                    },
                                    false).Returns(new AcquireTaskReturn(AcquisitionStatus.AcquisitionFailedTimeoutNotExceeded,
                                                                         TaskStatus.Dispatched,
                                                                         QueueMessageStatus.Postponed))
                                          .SetArgDisplayNames("Dispatched different owner false check date later");

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Submitted,
                                      OwnerPodId = "anotherownerpodid",
                                    },
                                    true).Returns(new AcquireTaskReturn(AcquisitionStatus.AcquisitionFailedMessageDuplicated,
                                                                        TaskStatus.Submitted,
                                                                        QueueMessageStatus.Postponed))
                                         .SetArgDisplayNames("Submitted task with another owner");

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Dispatched,
                                      OwnerPodId = "anotherowner",
                                    },
                                    true).Returns(new AcquireTaskReturn(AcquisitionStatus.AcquisitionFailedMessageDuplicated,
                                                                        TaskStatus.Dispatched,
                                                                        QueueMessageStatus.Postponed))
                                         .SetArgDisplayNames("Dispatched different owner");

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Dispatched,
                                      OwnerPodId = "anotherowner",
                                    },
                                    false).Returns(new AcquireTaskReturn(AcquisitionStatus.AcquisitionFailedDispatchedCrashed,
                                                                         TaskStatus.Submitted,
                                                                         QueueMessageStatus.Postponed))
                                          .SetArgDisplayNames("Dispatched different owner false check");

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Dispatched,
                                      OwnerPodId = "anotherowner",
                                      AcquisitionDate = DateTime.UtcNow - TimeSpan.FromSeconds(20),
                                    },
                                    false).Returns(new AcquireTaskReturn(AcquisitionStatus.AcquisitionFailedDispatchedCrashed,
                                                                         TaskStatus.Submitted,
                                                                         QueueMessageStatus.Postponed))
                                          .SetArgDisplayNames("Dispatched different owner false check date before");

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Submitted,
                                    },
                                    true).Returns(new AcquireTaskReturn(AcquisitionStatus.AcquisitionFailedProcessingHere,
                                                                        TaskStatus.Submitted,
                                                                        QueueMessageStatus.Postponed))
                                         .SetArgDisplayNames("Submitted task with same owner");


      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Paused,
                                    },
                                    true).Returns(new AcquireTaskReturn(AcquisitionStatus.TaskIsPaused,
                                                                        TaskStatus.Paused,
                                                                        QueueMessageStatus.Processed))
                                         .SetArgDisplayNames("Paused task");

      yield return new TestCaseData(taskData with
                                    {
                                      Status = TaskStatus.Pending,
                                    },
                                    true).Returns(new AcquireTaskReturn(AcquisitionStatus.TaskIsPending,
                                                                        TaskStatus.Pending,
                                                                        QueueMessageStatus.Postponed))
                                         .SetArgDisplayNames("Pending task");
    }
  }

  [Test]
  [TestCaseSource(nameof(TestCaseAcquireTask))]
  public async Task<AcquireTaskReturn> AcquireStatusShouldFail(TaskData taskData,
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

    var dbStatus = await testServiceProvider.TaskTable.FindTasksAsync(t => t.TaskId == taskData.TaskId,
                                                                      t => t.Status,
                                                                      CancellationToken.None)
                                            .SingleAsync(CancellationToken.None)
                                            .ConfigureAwait(false);

    Assert.AreEqual(dbStatus,
                    testServiceProvider.TaskHandler.GetAcquiredTaskInfo()
                                       .TaskStatus);

    return new AcquireTaskReturn(acquired,
                                 dbStatus,
                                 sqmh.Status);
  }


  public static IEnumerable TestCaseAcquireRetriedTask
  {
    get
    {
      yield return new TestCaseData("TaskRetry2").Returns(new AcquireTaskReturn(AcquisitionStatus.TaskIsRetried,
                                                                                TaskStatus.Retried,
                                                                                QueueMessageStatus.Poisonous));
      yield return new TestCaseData("TaskRetry2+Creating").Returns(new AcquireTaskReturn(AcquisitionStatus.TaskIsRetriedAndRetryIsCreating,
                                                                                         TaskStatus.Retried,
                                                                                         QueueMessageStatus.Poisonous));
      yield return new TestCaseData("TaskRetry2+Submitted").Returns(new AcquireTaskReturn(AcquisitionStatus.TaskIsRetriedAndRetryIsSubmitted,
                                                                                          TaskStatus.Retried,
                                                                                          QueueMessageStatus.Poisonous));
      yield return new TestCaseData("TaskRetry2+NotFound").Returns(new AcquireTaskReturn(AcquisitionStatus.TaskIsRetriedAndRetryIsNotFound,
                                                                                         TaskStatus.Retried,
                                                                                         QueueMessageStatus.Poisonous));
      yield return new TestCaseData("TaskRetry2+Pending").Returns(new AcquireTaskReturn(AcquisitionStatus.TaskIsRetriedAndRetryIsPending,
                                                                                        TaskStatus.Retried,
                                                                                        QueueMessageStatus.Poisonous));
    }
  }

  [Test]
  [TestCaseSource(nameof(TestCaseAcquireRetriedTask))]
  public async Task<AcquireTaskReturn> AcquireRetriedShouldFail(string taskId)
  {
    var sqmh = new SimpleQueueMessageHandler
               {
                 CancellationToken = CancellationToken.None,
                 Status            = QueueMessageStatus.Waiting,
                 MessageId = Guid.NewGuid()
                                 .ToString(),
                 TaskId = taskId,
               };

    var mockStreamHandler = new Mock<IWorkerStreamHandler>();
    var mockAgentHandler  = new Mock<IAgentHandler>();

    using var testServiceProvider = new TestTaskHandlerProvider(mockStreamHandler.Object,
                                                                mockAgentHandler.Object,
                                                                sqmh);

    var (_, _, _, _, sessionId) = await InitProviderRunnableTask(testServiceProvider)
                                    .ConfigureAwait(false);

    await InitRetry(testServiceProvider,
                    sessionId)
      .ConfigureAwait(false);

    var taskData = await testServiceProvider.TaskTable.ReadTaskAsync(taskId)
                                            .ConfigureAwait(false);

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.AreEqual(taskData.TaskId,
                    testServiceProvider.TaskHandler.GetAcquiredTaskInfo()
                                       .TaskId);

    var dbStatus = await testServiceProvider.TaskTable.FindTasksAsync(t => t.TaskId == taskData.TaskId,
                                                                      t => t.Status,
                                                                      CancellationToken.None)
                                            .SingleAsync(CancellationToken.None)
                                            .ConfigureAwait(false);

    Assert.AreEqual(dbStatus,
                    testServiceProvider.TaskHandler.GetAcquiredTaskInfo()
                                       .TaskStatus);

    var retryData = await testServiceProvider.TaskTable.ReadTaskAsync(taskData.RetryId())
                                             .ConfigureAwait(false);

    Assert.Contains(retryData.Status,
                    new List<TaskStatus>
                    {
                      TaskStatus.Dispatched,
                      TaskStatus.Submitted,
                    });

    return new AcquireTaskReturn(acquired,
                                 dbStatus,
                                 sqmh.Status);
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

    public async Task<T> ReadTaskAsync<T>(string                        taskId,
                                          Expression<Func<TaskData, T>> selector,
                                          CancellationToken             cancellationToken = default)
    {
      if (waitMethod_ == WaitMethod.Read)
      {
        await Task.Delay(delay_,
                         cancellationToken)
                  .ConfigureAwait(false);
      }

      return selector.Compile()
                     .Invoke(new TaskData("SessionId",
                                          "taskId",
                                          "ownerpodid",
                                          "ownerpodname",
                                          "payload",
                                          new List<string>(),
                                          new List<string>(),
                                          new Dictionary<string, bool>(),
                                          new List<string>(),
                                          "taskId",
                                          "createdby",
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
                                          DateTime.Now,
                                          DateTime.Now,
                                          TimeSpan.FromSeconds(1),
                                          TimeSpan.FromSeconds(2),
                                          TimeSpan.FromSeconds(3),
                                          new Output(OutputStatus.Error,
                                                     "")));
    }

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

    public Task DeleteTasksAsync(string            sessionId,
                                 CancellationToken cancellationToken = default)
      => throw new NotImplementedException();

    public Task DeleteTasksAsync(ICollection<string> taskIds,
                                 CancellationToken   cancellationToken = default)
      => throw new NotImplementedException();

    public Task<(IEnumerable<T> tasks, long totalCount)> ListTasksAsync<T>(Expression<Func<TaskData, bool>>    filter,
                                                                           Expression<Func<TaskData, object?>> orderField,
                                                                           Expression<Func<TaskData, T>>       selector,
                                                                           bool                                ascOrder,
                                                                           int                                 page,
                                                                           int                                 pageSize,
                                                                           CancellationToken                   cancellationToken = default)
      => throw new NotImplementedException();

    public async IAsyncEnumerable<T> FindTasksAsync<T>(Expression<Func<TaskData, bool>>           filter,
                                                       Expression<Func<TaskData, T>>              selector,
                                                       [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
      yield return await ReadTaskAsync("taskId",
                                       selector,
                                       cancellationToken)
                     .ConfigureAwait(false);
    }

    public async Task<TaskData?> UpdateOneTask(string                            taskId,
                                               Expression<Func<TaskData, bool>>? filter,
                                               UpdateDefinition<TaskData>        updates,
                                               bool                              before,
                                               CancellationToken                 cancellationToken = default)
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
                          "createdby",
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
                          DateTime.Now,
                          TimeSpan.FromSeconds(1),
                          TimeSpan.FromSeconds(2),
                          TimeSpan.FromSeconds(3),
                          new Output(OutputStatus.Error,
                                     ""));
    }

    public Task<long> UpdateManyTasks(Expression<Func<TaskData, bool>> filter,
                                      UpdateDefinition<TaskData>       updates,
                                      CancellationToken                cancellationToken = default)
      => Task.FromResult<long>(1);

    public Task<(IEnumerable<Application> applications, int totalCount)> ListApplicationsAsync(Expression<Func<TaskData, bool>> filter,
                                                                                               ICollection<Expression<Func<Application, object?>>> orderFields,
                                                                                               bool ascOrder,
                                                                                               int page,
                                                                                               int pageSize,
                                                                                               CancellationToken cancellationToken = default)
      => throw new NotImplementedException();

    public IAsyncEnumerable<T> RemoveRemainingDataDependenciesAsync<T>(ICollection<string>           taskIds,
                                                                       ICollection<string>           dependenciesToRemove,
                                                                       Expression<Func<TaskData, T>> selector,
                                                                       CancellationToken             cancellationToken = default)
      => AsyncEnumerable.Empty<T>();
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

    public Task<SessionData?> UpdateOneSessionAsync(string                               sessionId,
                                                    Expression<Func<SessionData, bool>>? filter,
                                                    UpdateDefinition<SessionData>        updates,
                                                    bool                                 before            = false,
                                                    CancellationToken                    cancellationToken = default)
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
                                                                new WaitTaskTable(waitMethod,
                                                                                  delayTaskTable),
                                                                new WaitSessionTable(delaySessionTable));

    var (taskId, _, _, _, _) = await InitProviderRunnableTask(testServiceProvider)
                                 .ConfigureAwait(false);

    sqmh.TaskId = taskId;

    cancellationTokenSource.Token.Register(() => testServiceProvider.Lifetime.StopApplication());
    cancellationTokenSource.CancelAfter(TimeSpan.FromMilliseconds(10));
    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.AreNotEqual(0,
                       acquired);
  }

  [Test]
  [TestCase(SessionStatus.Cancelled,
            ExpectedResult = AcquisitionStatus.SessionNotExecutable)]
  [TestCase(SessionStatus.Paused,
            ExpectedResult = AcquisitionStatus.SessionPaused)]
  [TestCase(SessionStatus.Purged,
            ExpectedResult = AcquisitionStatus.SessionNotExecutable)]
  [TestCase(SessionStatus.Deleted,
            ExpectedResult = AcquisitionStatus.SessionNotExecutable)]
  [TestCase(SessionStatus.Closed,
            ExpectedResult = AcquisitionStatus.SessionNotExecutable)]
  public async Task<AcquisitionStatus> AcquireTaskFromSessionShouldFail(SessionStatus sessionStatus)
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
                                                                sqmh);

    var (taskId, _, _, _, sessionId) = await InitProviderRunnableTask(testServiceProvider)
                                         .ConfigureAwait(false);

    await testServiceProvider.SessionTable.UpdateOneSessionAsync(sessionId,
                                                                 null,
                                                                 new UpdateDefinition<SessionData>().Set(data => data.Status,
                                                                                                         sessionStatus))
                             .ConfigureAwait(false);

    sqmh.TaskId = taskId;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.AreEqual(taskId,
                    testServiceProvider.TaskHandler.GetAcquiredTaskInfo()
                                       .TaskId);
    return acquired;
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
                                                                sqmh);

    var (_, unresolvedDependenciesTask, _, _, _) = await InitProviderRunnableTask(testServiceProvider)
                                                     .ConfigureAwait(false);

    sqmh.TaskId = unresolvedDependenciesTask;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.AreEqual(AcquisitionStatus.TaskIsPending,
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
      yield return new TestCaseData(new ExceptionWorkerStreamHandler<Exception>(0),
                                    null,
                                    true).Returns((TaskStatus.Retried, QueueMessageStatus.Cancelled))
                                         .SetArgDisplayNames("ExceptionError"); // error
      yield return new TestCaseData(new ExceptionWorkerStreamHandler<TestRpcException>(0),
                                    null,
                                    true).Returns((TaskStatus.Retried, QueueMessageStatus.Cancelled))
                                         .SetArgDisplayNames("RpcExceptionResubmit"); // error with resubmit
      yield return new TestCaseData(new ExceptionWorkerStreamHandler<TestUnavailableResponseEndedRpcException>(0),
                                    null,
                                    true).Returns((TaskStatus.Retried, QueueMessageStatus.Cancelled))
                                         .SetArgDisplayNames("CrashRpcExceptionResubmit"); // crash worker with resubmit
      yield return new TestCaseData(new ExceptionWorkerStreamHandler<TestUnavailableRpcException>(0),
                                    null,
                                    true).Returns((TaskStatus.Retried, QueueMessageStatus.Cancelled))
                                         .SetArgDisplayNames("CrashingWorkerWithoutCancellation"); // worker crashed during the execution of the task

      yield return new TestCaseData(new ExceptionWorkerStreamHandler<TestUnavailableRpcException>(1000,
                                                                                                  true,
                                                                                                  false),
                                    typeof(WorkerDownException),
                                    true).Returns((TaskStatus.Submitted, QueueMessageStatus.Postponed))
                                         .SetArgDisplayNames("UnavailableWorkerWithoutCancellation"); // worker crashed before the execution of the task


      // trigger error after cancellation and therefore should be considered as cancelled task and resend into queue
      yield return new TestCaseData(new ExceptionWorkerStreamHandler<Exception>(1000,
                                                                                false),
                                    null,
                                    true).Returns((TaskStatus.Retried, QueueMessageStatus.Cancelled))
                                         .SetArgDisplayNames("ExceptionTaskCancellation");

      yield return new TestCaseData(new ExceptionWorkerStreamHandler<Exception>(1000),
                                    typeof(OperationCanceledException),
                                    true).Returns((TaskStatus.Submitted, QueueMessageStatus.Postponed))
                                         .SetArgDisplayNames("ExceptionTaskAcceptCancellationHealthy");

      yield return new TestCaseData(new ExceptionWorkerStreamHandler<Exception>(1000),
                                    typeof(OperationCanceledException),
                                    false).Returns((TaskStatus.Retried, QueueMessageStatus.Cancelled))
                                          .SetArgDisplayNames("ExceptionTaskAcceptCancellationUnhealthy");
      yield return new TestCaseData(new ExceptionWorkerStreamHandler<TestRpcException>(1000,
                                                                                       false),
                                    null,
                                    true).Returns((TaskStatus.Retried, QueueMessageStatus.Cancelled))
                                         .SetArgDisplayNames("RpcExceptionTaskCancellation");

      // If the worker becomes unavailable during the task execution after cancellation, the task should be resubmitted
      yield return new TestCaseData(new ExceptionWorkerStreamHandler<TestUnavailableRpcException>(1000,
                                                                                                  false),
                                    null,
                                    true).Returns((TaskStatus.Retried, QueueMessageStatus.Cancelled))
                                         .SetArgDisplayNames("UnavailableAfterCancellation");

      // If the worker crashes during the task execution after cancellation, the task should be put in error
      yield return new TestCaseData(new ExceptionWorkerStreamHandler<TestUnavailableResponseEndedRpcException>(1000,
                                                                                                               false),
                                    null,
                                    true).Returns((TaskStatus.Retried, QueueMessageStatus.Cancelled))
                                         .SetArgDisplayNames("CrashAfterCancellation");
    }
  }

  [Test]
  [TestCaseSource(nameof(TestCaseOuptut))]
  public async Task<(TaskStatus taskStatus, QueueMessageStatus messageStatus)> ExecuteTaskWithExceptionDuringCancellationShouldSucceed<TEx>(
    ExceptionWorkerStreamHandler<TEx> workerStreamHandler,
    Type?                             expectedException,
    bool                              healthy)
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
                                                                graceDelay: TimeSpan.FromMilliseconds(10));

    testServiceProvider.HealthCheckRecord.Record(HealthCheckTag.Liveness,
                                                 healthy
                                                   ? HealthStatus.Healthy
                                                   : HealthStatus.Unhealthy);

    var (taskId, _, _, _, _) = await InitProviderRunnableTask(testServiceProvider)
                                 .ConfigureAwait(false);

    sqmh.TaskId = taskId;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.AreEqual(AcquisitionStatus.Acquired,
                    acquired);

    await testServiceProvider.TaskHandler.PreProcessing()
                             .ConfigureAwait(false);

    cancellationTokenSource.Token.Register(() => testServiceProvider.Lifetime.StopApplication());
    cancellationTokenSource.CancelAfter(TimeSpan.FromMilliseconds(500));


    Assert.That(() => testServiceProvider.TaskHandler.ExecuteTask(),
                Throws.InstanceOf(expectedException ?? typeof(TEx)));

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
                                                                sqmh);

    var (taskId, _, _, _, _) = await InitProviderRunnableTask(testServiceProvider)
                                 .ConfigureAwait(false);

    sqmh.TaskId = taskId;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.AreEqual(AcquisitionStatus.Acquired,
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
  public async Task PreprocessingShouldThrow([Values] bool notFound)
  {
    Exception exception = notFound
                            ? new ObjectDataNotFoundException()
                            : new ApplicationException();
    var objectStorage = new Mock<IObjectStorage>();
    objectStorage.Setup(os => os.GetValuesAsync(It.IsAny<byte[]>(),
                                                It.IsAny<CancellationToken>()))
                 .Throws(exception);
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
                                                                objectStorage: objectStorage.Object);

    var (taskId, _, _, _, _) = await InitProviderRunnableTask(testServiceProvider)
                                 .ConfigureAwait(false);

    sqmh.TaskId = taskId;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.That(acquired,
                Is.EqualTo(AcquisitionStatus.Acquired));

    Assert.That(() => testServiceProvider.TaskHandler.PreProcessing(),
                Throws.TypeOf(exception.GetType()));

    var taskData = await testServiceProvider.TaskTable.ReadTaskAsync(taskId,
                                                                     CancellationToken.None)
                                            .ConfigureAwait(false);

    Console.WriteLine(taskData);

    Assert.Multiple(() =>
                    {
                      Assert.That(taskData.Status,
                                  Is.EqualTo(notFound
                                               ? TaskStatus.Error
                                               : TaskStatus.Retried));

                      Assert.That(sqmh.Status,
                                  Is.EqualTo(notFound
                                               ? QueueMessageStatus.Processed
                                               : QueueMessageStatus.Cancelled));
                    });
  }

  [Test]
  public async Task PauseSessionBeforeExecutionShouldSucceed()
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
                                                                sqmh);

    var (taskId, _, _, _, sessionId) = await InitProviderRunnableTask(testServiceProvider)
                                         .ConfigureAwait(false);

    sqmh.TaskId = taskId;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.AreEqual(AcquisitionStatus.Acquired,
                    acquired);

    await testServiceProvider.TaskHandler.PreProcessing()
                             .ConfigureAwait(false);

    await TaskLifeCycleHelper.PauseAsync(testServiceProvider.TaskTable,
                                         testServiceProvider.SessionTable,
                                         sessionId)
                             .ConfigureAwait(false);

    Assert.ThrowsAsync<TaskPausedException>(testServiceProvider.TaskHandler.ExecuteTask);

    var taskData = await testServiceProvider.TaskTable.ReadTaskAsync(taskId,
                                                                     CancellationToken.None)
                                            .ConfigureAwait(false);

    Console.WriteLine(taskData);

    Assert.AreEqual(TaskStatus.Paused,
                    taskData.Status);
    Assert.AreEqual(QueueMessageStatus.Processed,
                    sqmh.Status);
  }

  [Test]
  public async Task ExecuteTaskTimeoutShouldSucceed()
  {
    var sqmh = new SimpleQueueMessageHandler
               {
                 CancellationToken = CancellationToken.None,
                 Status            = QueueMessageStatus.Waiting,
                 MessageId = Guid.NewGuid()
                                 .ToString(),
               };

    var mock = new Mock<IWorkerStreamHandler>();
    mock.Setup(handler => handler.StartTaskProcessing(It.IsAny<TaskData>(),
                                                      It.IsAny<string>(),
                                                      It.IsAny<string>(),
                                                      It.IsAny<CancellationToken>()))
        .Returns(() => Task.FromResult(new Output(OutputStatus.Timeout,
                                                  "Deadline Exceeded")));
    mock.Setup(handler => handler.Check(It.IsAny<HealthCheckTag>()))
        .Returns(() => Task.FromResult(HealthCheckResult.Healthy()));

    var agentHandler = new SimpleAgentHandler();
    using var testServiceProvider = new TestTaskHandlerProvider(mock.Object,
                                                                agentHandler,
                                                                sqmh);

    var (taskId, _, _, _, _) = await InitProviderRunnableTask(testServiceProvider)
                                 .ConfigureAwait(false);

    sqmh.TaskId = taskId;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.AreEqual(AcquisitionStatus.Acquired,
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

    Assert.AreEqual(TaskStatus.Timeout,
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
  public async Task ExecuteTaskCancellationToken([Values] bool rpc,
                                                 [Values] bool healthy)
  {
    var sqmh = new SimpleQueueMessageHandler
               {
                 CancellationToken = CancellationToken.None,
                 Status            = QueueMessageStatus.Waiting,
                 MessageId = Guid.NewGuid()
                                 .ToString(),
               };

    Exception exception = new OperationCanceledException();
    if (rpc)
    {
      exception = new RpcException(new Status(StatusCode.Cancelled,
                                              "Cancelled",
                                              exception));
    }

    var mock = new Mock<IWorkerStreamHandler>();
    mock.Setup(handler => handler.StartTaskProcessing(It.IsAny<TaskData>(),
                                                      It.IsAny<string>(),
                                                      It.IsAny<string>(),
                                                      It.IsAny<CancellationToken>()))
        .Returns(() => Task.FromException<Output>(exception));
    mock.Setup(handler => handler.Check(It.IsAny<HealthCheckTag>()))
        .Returns(() => Task.FromResult(HealthCheckResult.Healthy()));

    var agentHandler = new SimpleAgentHandler();
    using var testServiceProvider = new TestTaskHandlerProvider(mock.Object,
                                                                agentHandler,
                                                                sqmh);

    testServiceProvider.HealthCheckRecord.Record(HealthCheckTag.Liveness,
                                                 healthy
                                                   ? HealthStatus.Healthy
                                                   : HealthStatus.Unhealthy);

    var (taskId, _, _, _, _) = await InitProviderRunnableTask(testServiceProvider)
                                 .ConfigureAwait(false);


    sqmh.TaskId = taskId;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.AreEqual(AcquisitionStatus.Acquired,
                    acquired);

    await testServiceProvider.TaskHandler.PreProcessing()
                             .ConfigureAwait(false);

    // Trigger early and later cancellation tokens
    testServiceProvider.Lifetime.StopApplication();

    Assert.That(() => testServiceProvider.TaskHandler.ExecuteTask(),
                Throws.InstanceOf(exception.GetType()));

    var taskData = await testServiceProvider.TaskTable.ReadTaskAsync(taskId,
                                                                     CancellationToken.None)
                                            .ConfigureAwait(false);

    Console.WriteLine(taskData);

    Assert.Multiple(() =>
                    {
                      Assert.That(taskData.Status,
                                  Is.EqualTo(healthy
                                               ? TaskStatus.Submitted
                                               : TaskStatus.Retried));
                      Assert.That(sqmh.Status,
                                  Is.EqualTo(healthy
                                               ? QueueMessageStatus.Postponed
                                               : QueueMessageStatus.Cancelled));
                    });
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
                                                                sqmh);

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

      Assert.AreEqual(AcquisitionStatus.Acquired,
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
                                                                sqmh);

    var (taskId, _, _, _, _) = await InitProviderRunnableTask(testServiceProvider)
                                 .ConfigureAwait(false);

    sqmh.TaskId = taskId;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.AreEqual(AcquisitionStatus.Acquired,
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
                                                                sqmh);

    var (taskId, _, _, _, _) = await InitProviderRunnableTask(testServiceProvider)
                                 .ConfigureAwait(false);

    sqmh.TaskId = taskId;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.AreEqual(AcquisitionStatus.Acquired,
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
  public async Task CancelLongTaskShouldSucceed([Values] bool before)
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
                                                                sqmh);

    var (taskId, _, _, _, _) = await InitProviderRunnableTask(testServiceProvider)
                                 .ConfigureAwait(false);

    sqmh.TaskId = taskId;

    var acquired = await testServiceProvider.TaskHandler.AcquireTask()
                                            .ConfigureAwait(false);

    Assert.AreEqual(AcquisitionStatus.Acquired,
                    acquired);

    await testServiceProvider.TaskHandler.PreProcessing()
                             .ConfigureAwait(false);

    var exec = before
                 ? testServiceProvider.TaskHandler.ExecuteTask()
                 : Task.CompletedTask;

    // Cancel task for test

    await Task.Delay(TimeSpan.FromMilliseconds(200))
              .ConfigureAwait(false);

    await testServiceProvider.TaskTable.CancelTaskAsync(new List<string>
                                                        {
                                                          taskId,
                                                        },
                                                        CancellationToken.None)
                             .ConfigureAwait(false);

    if (!before)
    {
      exec = testServiceProvider.TaskHandler.ExecuteTask();
    }

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

    Assert.That(() => exec,
                Throws.InstanceOf<OperationCanceledException>());

    Assert.AreEqual(TaskStatus.Cancelling,
                    await testServiceProvider.TaskTable.GetTaskStatus(taskId)
                                             .ConfigureAwait(false));

    Assert.AreEqual(QueueMessageStatus.Cancelled,
                    sqmh.Status);
  }
}
