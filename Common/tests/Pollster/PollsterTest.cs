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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Stream.Worker;
using ArmoniK.Core.Common.Tests.Helpers;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using Moq;

using NUnit.Framework;

using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

namespace ArmoniK.Core.Common.Tests.Pollster;

[TestFixture]
public class PollsterTest
{
  [SetUp]
  public void SetUp()
  {
  }

  [TearDown]
  public virtual void TearDown()
  {
  }

  private static readonly string ExpectedOutput1 = "ExpectedOutput1";
  private static readonly string ExpectedOutput2 = "ExpectedOutput2";

  private static async Task<(string sessionId, string taskCreating, string taskSubmitted)> InitSubmitter(ISubmitter        submitter,
                                                                                                         IPartitionTable   partitionTable,
                                                                                                         IResultTable      resultTable,
                                                                                                         ISessionTable     sessionTable,
                                                                                                         CancellationToken token)
  {
    var defaultTaskOptions = new TaskOptions(new Dictionary<string, string>(),
                                             TimeSpan.FromSeconds(2),
                                             2,
                                             1,
                                             "part1",
                                             "",
                                             "",
                                             "",
                                             "",
                                             "");

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

    await resultTable.Create(new[]
                             {
                               new Result(sessionId,
                                          ExpectedOutput1,
                                          "",
                                          "",
                                          "",
                                          ResultStatus.Created,
                                          new List<string>(),
                                          DateTime.UtcNow,
                                          0,
                                          Array.Empty<byte>()),
                               new Result(sessionId,
                                          ExpectedOutput2,
                                          "",
                                          "",
                                          "",
                                          ResultStatus.Created,
                                          new List<string>(),
                                          DateTime.UtcNow,
                                          0,
                                          Array.Empty<byte>()),
                             },
                             token)
                     .ConfigureAwait(false);

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
                                       .ConfigureAwait(false)).First()
                                                              .TaskId;

    var requests = await submitter.CreateTasks(sessionId,
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

    var taskSubmitted = requests.First()
                                .TaskId;

    var sessionData = await sessionTable.GetSessionAsync(sessionId,
                                                         CancellationToken.None)
                                        .ConfigureAwait(false);

    await submitter.FinalizeTaskCreation(requests,
                                         sessionData,
                                         sessionId,
                                         CancellationToken.None)
                   .ConfigureAwait(false);

    return (sessionId, taskCreating, taskSubmitted);
  }


  [Test]
  [Timeout(15000)]
  public void InitializePollster()
  {
    var mockStreamHandler    = new Mock<IWorkerStreamHandler>();
    var mockPullQueueStorage = new Mock<IPullQueueStorage>();
    var mockAgentHandler     = new Mock<IAgentHandler>();
    using var testServiceProvider = new TestPollsterProvider(mockStreamHandler.Object,
                                                             mockAgentHandler.Object,
                                                             mockPullQueueStorage.Object);

    Assert.NotNull(testServiceProvider.Pollster);
  }

  private class MockWorkerStreamHandler : IWorkerStreamHandler
  {
    private bool isInitialized_;

    public Task Init(CancellationToken cancellationToken)
    {
      isInitialized_ = true;
      return Task.CompletedTask;
    }

    public Task<HealthCheckResult> Check(HealthCheckTag tag)
      => Task.FromResult(isInitialized_
                           ? HealthCheckResult.Healthy()
                           : HealthCheckResult.Unhealthy());

    public void Dispose()
    {
    }

    public Task<Output> StartTaskProcessing(TaskData          taskData,
                                            string            token,
                                            string            dataFolder,
                                            CancellationToken cancellationToken)
      => throw new NotImplementedException();
  }

  private class ReturnHealthCheckWorkerStreamHandler : IWorkerStreamHandler
  {
    private readonly HealthCheckResult healthCheckResult_;

    private bool isInitialized_;

    public ReturnHealthCheckWorkerStreamHandler(HealthCheckResult healthCheckResult)
      => healthCheckResult_ = healthCheckResult;


    public Task Init(CancellationToken cancellationToken)
    {
      isInitialized_ = true;
      return Task.CompletedTask;
    }

    public Task<HealthCheckResult> Check(HealthCheckTag tag)
      => Task.FromResult(isInitialized_
                           ? healthCheckResult_
                           : HealthCheckResult.Healthy());

    public void Dispose()
    {
    }

    public Task<Output> StartTaskProcessing(TaskData          taskData,
                                            string            token,
                                            string            dataFolder,
                                            CancellationToken cancellationToken)
      => throw new NotImplementedException();
  }

  private class MockPullQueueStorage : IPullQueueStorage
  {
    private bool isInitialized_;

    public Task Init(CancellationToken cancellationToken)
    {
      isInitialized_ = true;
      return Task.CompletedTask;
    }

    public Task<HealthCheckResult> Check(HealthCheckTag tag)
      => Task.FromResult(isInitialized_
                           ? HealthCheckResult.Healthy()
                           : HealthCheckResult.Unhealthy());

    public int MaxPriority { get; }

    public IAsyncEnumerable<IQueueMessageHandler> PullMessagesAsync(int               nbMessages,
                                                                    CancellationToken cancellationToken = default)
      => throw new NotImplementedException();
  }

  [Test]
  [Timeout(15000)]
  public async Task InitShouldSucceed()
  {
    var mockAgentHandler = new Mock<IAgentHandler>();
    using var testServiceProvider = new TestPollsterProvider(new MockWorkerStreamHandler(),
                                                             mockAgentHandler.Object,
                                                             new MockPullQueueStorage());

    Assert.AreNotEqual(HealthStatus.Healthy,
                       (await testServiceProvider.Pollster.Check(HealthCheckTag.Readiness)
                                                 .ConfigureAwait(false)).Status);
    Assert.AreNotEqual(HealthStatus.Healthy,
                       (await testServiceProvider.Pollster.Check(HealthCheckTag.Startup)
                                                 .ConfigureAwait(false)).Status);

    await testServiceProvider.Pollster.Init(CancellationToken.None)
                             .ConfigureAwait(false);

    var res = await testServiceProvider.Pollster.Check(HealthCheckTag.Liveness)
                                       .ConfigureAwait(false);

    Console.WriteLine(res.Description);

    Assert.AreEqual(HealthStatus.Healthy,
                    (await testServiceProvider.Pollster.Check(HealthCheckTag.Liveness)
                                              .ConfigureAwait(false)).Status);
    // Unhealthy because there are no tasks in queue
    Assert.AreEqual(HealthStatus.Unhealthy,
                    (await testServiceProvider.Pollster.Check(HealthCheckTag.Readiness)
                                              .ConfigureAwait(false)).Status);
    Assert.AreEqual(HealthStatus.Healthy,
                    (await testServiceProvider.Pollster.Check(HealthCheckTag.Startup)
                                              .ConfigureAwait(false)).Status);

    testServiceProvider.AssertFailAfterError(6);
  }

  [Test]
  [Timeout(10000)]
  public async Task InitShouldFail()
  {
    var mockAgentHandler = new Mock<IAgentHandler>();

    var desc = "desc";
    var ex   = new ArmoniKException();
    var data = new Dictionary<string, object>
               {
                 {
                   "key1", "val1"
                 },
               };

    using var testServiceProvider = new TestPollsterProvider(new ReturnHealthCheckWorkerStreamHandler(HealthCheckResult.Unhealthy(desc,
                                                                                                                                  ex,
                                                                                                                                  data)),
                                                             mockAgentHandler.Object,
                                                             new MockPullQueueStorage());

    Assert.AreNotEqual(HealthStatus.Healthy,
                       (await testServiceProvider.Pollster.Check(HealthCheckTag.Readiness)
                                                 .ConfigureAwait(false)).Status);
    Assert.AreNotEqual(HealthStatus.Healthy,
                       (await testServiceProvider.Pollster.Check(HealthCheckTag.Startup)
                                                 .ConfigureAwait(false)).Status);

    await testServiceProvider.Pollster.Init(CancellationToken.None)
                             .ConfigureAwait(false);

    var res = await testServiceProvider.Pollster.Check(HealthCheckTag.Liveness)
                                       .ConfigureAwait(false);

    var healthResult = await testServiceProvider.Pollster.Check(HealthCheckTag.Liveness)
                                                .ConfigureAwait(false);

    Console.WriteLine(res.Description);

    Assert.AreEqual(new StringBuilder().AppendLine(desc)
                                       .ToString(),
                    healthResult.Description);
    Assert.AreEqual(new AggregateException(ex).Message,
                    healthResult.Exception?.Message);
    Assert.AreEqual(HealthStatus.Unhealthy,
                    healthResult.Status);
    Assert.AreEqual(data,
                    healthResult.Data);

    Assert.AreEqual(HealthStatus.Unhealthy,
                    (await testServiceProvider.Pollster.Check(HealthCheckTag.Readiness)
                                              .ConfigureAwait(false)).Status);
    Assert.AreEqual(HealthStatus.Unhealthy,
                    (await testServiceProvider.Pollster.Check(HealthCheckTag.Startup)
                                              .ConfigureAwait(false)).Status);

    // This test that we return from the mainloop after the health check is unhealthy
    await testServiceProvider.Pollster.MainLoop()
                             .ConfigureAwait(false);
    Assert.True(testServiceProvider.ExceptionManager.Failed);
  }

  [Test]
  [Timeout(10000)]
  public async Task RunThenCancelPollster()
  {
    var mockStreamHandler    = new Mock<IWorkerStreamHandler>();
    var mockPullQueueStorage = new Mock<IPullQueueStorage>();
    var mockAgentHandler     = new Mock<IAgentHandler>();

    mockPullQueueStorage.Setup(storage => storage.PullMessagesAsync(It.IsAny<int>(),
                                                                    It.IsAny<CancellationToken>()))
                        .Returns(() => new List<IQueueMessageHandler>
                                       {
                                         new SimpleQueueMessageHandler
                                         {
                                           CancellationToken = CancellationToken.None,
                                           Status            = QueueMessageStatus.Waiting,
                                           MessageId = Guid.NewGuid()
                                                           .ToString(),
                                         },
                                       }.ToAsyncEnumerable());

    using var testServiceProvider = new TestPollsterProvider(mockStreamHandler.Object,
                                                             mockAgentHandler.Object,
                                                             mockPullQueueStorage.Object);

    await testServiceProvider.Pollster.Init(CancellationToken.None)
                             .ConfigureAwait(false);

    var stop = testServiceProvider.StopApplicationAfter(TimeSpan.FromMicroseconds(105));

    Assert.DoesNotThrowAsync(() => testServiceProvider.Pollster.MainLoop());
    Assert.DoesNotThrowAsync(() => stop);
    Assert.AreEqual(Array.Empty<string>(),
                    testServiceProvider.Pollster.TaskProcessing);

    testServiceProvider.AssertFailAfterError(6);
  }

  public class WaitWorkerStreamHandler : IWorkerStreamHandler
  {
    private readonly double delay_;

    public WaitWorkerStreamHandler(double delay)
      => delay_ = delay;

    public Task<HealthCheckResult> Check(HealthCheckTag tag)
      => Task.FromResult(HealthCheckResult.Healthy());

    public Task Init(CancellationToken cancellationToken)
      => Task.CompletedTask;

    public void Dispose()
      => GC.SuppressFinalize(this);

    public async Task<Output> StartTaskProcessing(TaskData          taskData,
                                                  string            token,
                                                  string            dataFolder,
                                                  CancellationToken cancellationToken)
    {
      await Task.Delay(TimeSpan.FromMilliseconds(delay_),
                       cancellationToken)
                .ConfigureAwait(false);
      return new Output(OutputStatus.Success,
                        "");
    }
  }


  [Test]
  [Timeout(10000)]
  public async Task ExecuteTaskShouldSucceed()
  {
    var mockPullQueueStorage    = new SimplePullQueueStorageChannel();
    var waitWorkerStreamHandler = new SimpleWorkerStreamHandler();
    var simpleAgentHandler      = new SimpleAgentHandler();

    using var testServiceProvider = new TestPollsterProvider(waitWorkerStreamHandler,
                                                             simpleAgentHandler,
                                                             mockPullQueueStorage);

    var (_, _, taskSubmitted) = await InitSubmitter(testServiceProvider.Submitter,
                                                    testServiceProvider.PartitionTable,
                                                    testServiceProvider.ResultTable,
                                                    testServiceProvider.SessionTable,
                                                    CancellationToken.None)
                                  .ConfigureAwait(false);

    await mockPullQueueStorage.Channel.Writer.WriteAsync(new SimpleQueueMessageHandler
                                                         {
                                                           CancellationToken = CancellationToken.None,
                                                           Status            = QueueMessageStatus.Waiting,
                                                           MessageId = Guid.NewGuid()
                                                                           .ToString(),
                                                           TaskId = taskSubmitted,
                                                         })
                              .ConfigureAwait(false);

    await testServiceProvider.Pollster.Init(CancellationToken.None)
                             .ConfigureAwait(false);

    var stop = testServiceProvider.StopApplicationAfter(TimeSpan.FromSeconds(1));

    Assert.DoesNotThrowAsync(() => testServiceProvider.Pollster.MainLoop());
    Assert.DoesNotThrowAsync(() => stop);

    Assert.AreEqual(TaskStatus.Completed,
                    await testServiceProvider.TaskTable.GetTaskStatus(taskSubmitted,
                                                                      CancellationToken.None)
                                             .ConfigureAwait(false));

    testServiceProvider.AssertFailAfterError(6);
  }

  [Test]
  [Timeout(10000)]
  public async Task ExecuteTaskTimeoutAcquire()
  {
    var mockPullQueueStorage    = new SimplePullQueueStorageChannel();
    var waitWorkerStreamHandler = new WaitWorkerStreamHandler(1000);
    var simpleAgentHandler      = new SimpleAgentHandler();

    using var testServiceProvider = new TestPollsterProvider(waitWorkerStreamHandler,
                                                             simpleAgentHandler,
                                                             mockPullQueueStorage,
                                                             TimeSpan.FromMilliseconds(100),
                                                             TimeSpan.FromMilliseconds(100),
                                                             0);

    var (sessionId, _, taskSubmitted) = await InitSubmitter(testServiceProvider.Submitter,
                                                            testServiceProvider.PartitionTable,
                                                            testServiceProvider.ResultTable,
                                                            testServiceProvider.SessionTable,
                                                            CancellationToken.None)
                                          .ConfigureAwait(false);

    await mockPullQueueStorage.Channel.Writer.WriteAsync(new SimpleQueueMessageHandler
                                                         {
                                                           CancellationToken = CancellationToken.None,
                                                           Status            = QueueMessageStatus.Waiting,
                                                           MessageId = Guid.NewGuid()
                                                                           .ToString(),
                                                           TaskId = taskSubmitted,
                                                         })
                              .ConfigureAwait(false);

    var expectedOutput3 = "ExpectedOutput3";
    await testServiceProvider.ResultTable.Create(new[]
                                                 {
                                                   new Result(sessionId,
                                                              expectedOutput3,
                                                              "",
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

    var requests = await testServiceProvider.Submitter.CreateTasks(sessionId,
                                                                   sessionId,
                                                                   new TaskOptions(),
                                                                   new List<TaskRequest>
                                                                   {
                                                                     new(new[]
                                                                         {
                                                                           expectedOutput3,
                                                                         },
                                                                         new List<string>(),
                                                                         new List<ReadOnlyMemory<byte>>
                                                                         {
                                                                           new(Encoding.ASCII.GetBytes("AAAA")),
                                                                         }.ToAsyncEnumerable()),
                                                                   }.ToAsyncEnumerable(),
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

    var taskSubmitted2 = requests.First()
                                 .TaskId;

    await mockPullQueueStorage.Channel.Writer.WriteAsync(new SimpleQueueMessageHandler
                                                         {
                                                           CancellationToken = CancellationToken.None,
                                                           Status            = QueueMessageStatus.Waiting,
                                                           MessageId = Guid.NewGuid()
                                                                           .ToString(),
                                                           TaskId = taskSubmitted2,
                                                         })
                              .ConfigureAwait(false);

    await testServiceProvider.Pollster.Init(CancellationToken.None)
                             .ConfigureAwait(false);

    var stop = testServiceProvider.StopApplicationAfter(TimeSpan.FromSeconds(2));

    Assert.DoesNotThrowAsync(() => testServiceProvider.Pollster.MainLoop());
    Assert.That(() => stop,
                Throws.InstanceOf<OperationCanceledException>());

    Assert.AreEqual(TaskStatus.Submitted,
                    await testServiceProvider.TaskTable.GetTaskStatus(taskSubmitted2,
                                                                      CancellationToken.None)
                                             .ConfigureAwait(false));

    testServiceProvider.AssertFailAfterError();
  }

  [Test]
  [Timeout(10000)]
  public async Task ExecuteTaskThatExceedsGraceDelayShouldResubmit()
  {
    var mockPullQueueStorage    = new SimplePullQueueStorageChannel();
    var waitWorkerStreamHandler = new WaitWorkerStreamHandler(1000000);
    var simpleAgentHandler      = new SimpleAgentHandler();

    using var testServiceProvider = new TestPollsterProvider(waitWorkerStreamHandler,
                                                             simpleAgentHandler,
                                                             mockPullQueueStorage,
                                                             TimeSpan.FromMilliseconds(100));

    var (_, _, taskSubmitted) = await InitSubmitter(testServiceProvider.Submitter,
                                                    testServiceProvider.PartitionTable,
                                                    testServiceProvider.ResultTable,
                                                    testServiceProvider.SessionTable,
                                                    CancellationToken.None)
                                  .ConfigureAwait(false);

    await mockPullQueueStorage.Channel.Writer.WriteAsync(new SimpleQueueMessageHandler
                                                         {
                                                           CancellationToken = CancellationToken.None,
                                                           Status            = QueueMessageStatus.Waiting,
                                                           MessageId = Guid.NewGuid()
                                                                           .ToString(),
                                                           TaskId = taskSubmitted,
                                                         })
                              .ConfigureAwait(false);

    await testServiceProvider.Pollster.Init(CancellationToken.None)
                             .ConfigureAwait(false);

    var stop = testServiceProvider.StopApplicationAfter(TimeSpan.FromMilliseconds(300));

    Assert.DoesNotThrowAsync(() => testServiceProvider.Pollster.MainLoop());
    Assert.DoesNotThrowAsync(() => stop);
    Assert.False(testServiceProvider.ExceptionManager.Failed);

    // wait to exceed grace delay and see that task is properly resubmitted
    await Task.Delay(TimeSpan.FromMilliseconds(200),
                     CancellationToken.None)
              .ConfigureAwait(false);

    Assert.AreEqual(TaskStatus.Submitted,
                    await testServiceProvider.TaskTable.GetTaskStatus(taskSubmitted,
                                                                      CancellationToken.None)
                                             .ConfigureAwait(false));

    testServiceProvider.AssertFailAfterError(5);
  }

  [Test]
  [Timeout(30000)]
  public async Task CancelLongTaskShouldSucceed()
  {
    var mockPullQueueStorage    = new SimplePullQueueStorageChannel();
    var waitWorkerStreamHandler = new ExceptionWorkerStreamHandler<Exception>(15000);
    var simpleAgentHandler      = new SimpleAgentHandler();

    using var testServiceProvider = new TestPollsterProvider(waitWorkerStreamHandler,
                                                             simpleAgentHandler,
                                                             mockPullQueueStorage);

    var (_, _, taskSubmitted) = await InitSubmitter(testServiceProvider.Submitter,
                                                    testServiceProvider.PartitionTable,
                                                    testServiceProvider.ResultTable,
                                                    testServiceProvider.SessionTable,
                                                    CancellationToken.None)
                                  .ConfigureAwait(false);

    await mockPullQueueStorage.Channel.Writer.WriteAsync(new SimpleQueueMessageHandler
                                                         {
                                                           CancellationToken = CancellationToken.None,
                                                           Status            = QueueMessageStatus.Waiting,
                                                           MessageId = Guid.NewGuid()
                                                                           .ToString(),
                                                           TaskId = taskSubmitted,
                                                         })
                              .ConfigureAwait(false);

    await testServiceProvider.Pollster.Init(CancellationToken.None)
                             .ConfigureAwait(false);

    var stop = testServiceProvider.StopApplicationAfter(TimeSpan.FromSeconds(5));

    var mainLoopTask = testServiceProvider.Pollster.MainLoop();

    await Task.Delay(TimeSpan.FromMilliseconds(200),
                     CancellationToken.None)
              .ConfigureAwait(false);

    await testServiceProvider.TaskTable.CancelTaskAsync(new List<string>
                                                        {
                                                          taskSubmitted,
                                                        },
                                                        CancellationToken.None)
                             .ConfigureAwait(false);

    await Task.Delay(TimeSpan.FromMilliseconds(200),
                     CancellationToken.None)
              .ConfigureAwait(false);

    await testServiceProvider.Pollster.StopCancelledTask()
                             .ConfigureAwait(false);

    Assert.DoesNotThrowAsync(() => mainLoopTask);
    Assert.DoesNotThrowAsync(() => stop);

    Assert.That(await testServiceProvider.TaskTable.GetTaskStatus(taskSubmitted,
                                                                  CancellationToken.None)
                                         .ConfigureAwait(false),
                Is.AnyOf(TaskStatus.Cancelled,
                         TaskStatus.Cancelling));

    Assert.AreEqual(Array.Empty<string>(),
                    testServiceProvider.Pollster.TaskProcessing);

    testServiceProvider.AssertFailAfterError(5);
  }

  public static IEnumerable ExecuteTooManyErrorShouldFailTestCase
  {
    get
    {
      var mockStreamHandler    = new Mock<IWorkerStreamHandler>();
      var mockPullQueueStorage = new Mock<IPullQueueStorage>();
      var mockAgentHandler     = new Mock<IAgentHandler>();

      {
        // Failing WorkerStreamHandler
        var mockStreamHandlerFail = new Mock<IWorkerStreamHandler>();
        mockStreamHandlerFail.Setup(streamHandler => streamHandler.StartTaskProcessing(It.IsAny<TaskData>(),
                                                                                       It.IsAny<string>(),
                                                                                       It.IsAny<string>(),
                                                                                       It.IsAny<CancellationToken>()))
                             .Throws(new ApplicationException("Failed WorkerStreamHandler"));
        yield return new TestCaseData(mockStreamHandlerFail,
                                      mockPullQueueStorage,
                                      mockAgentHandler).SetArgDisplayNames("WorkerStreamHandler");
      }

      {
        // Failing PullQueueStorage
        var mockPullQueueStorageFail = new Mock<IPullQueueStorage>();
        mockPullQueueStorageFail.Setup(storage => storage.PullMessagesAsync(It.IsAny<int>(),
                                                                            It.IsAny<CancellationToken>()))
                                .Throws(new ApplicationException("Failed queue"));

        yield return new TestCaseData(mockStreamHandler,
                                      mockPullQueueStorageFail,
                                      mockAgentHandler).SetArgDisplayNames("PullQueueStorage");
      }

      {
        // Failing AgentHandler
        var mockAgentHandlerFail = new Mock<IAgentHandler>();
        mockAgentHandlerFail.Setup(agent => agent.Start(It.IsAny<string>(),
                                                        It.IsAny<ILogger>(),
                                                        It.IsAny<SessionData>(),
                                                        It.IsAny<TaskData>(),
                                                        It.IsAny<string>(),
                                                        It.IsAny<CancellationToken>()))
                            .Throws(new ApplicationException("Failed agent"));

        yield return new TestCaseData(mockStreamHandler,
                                      mockPullQueueStorage,
                                      mockAgentHandlerFail).SetArgDisplayNames("AgentHandler");
      }
    }
  }

  [Test]
  [TestCaseSource(nameof(ExecuteTooManyErrorShouldFailTestCase))]
  [Timeout(10000)]
  public async Task ExecuteTooManyErrorShouldFail(Mock<IWorkerStreamHandler> mockStreamHandler,
                                                  Mock<IPullQueueStorage>    mockPullQueueStorage,
                                                  Mock<IAgentHandler>        mockAgentHandler)
  {
    using var testServiceProvider = new TestPollsterProvider(mockStreamHandler.Object,
                                                             mockAgentHandler.Object,
                                                             mockPullQueueStorage.Object);

    var pollster = testServiceProvider.Pollster;

    await pollster.Init(CancellationToken.None)
                  .ConfigureAwait(false);

    var stop = testServiceProvider.StopApplicationAfter(TimeSpan.FromSeconds(10));

    Assert.DoesNotThrowAsync(() => pollster.MainLoop());
    Assert.That(() => stop,
                Throws.InstanceOf<OperationCanceledException>());
    Assert.True(testServiceProvider.ExceptionManager.Failed);
    Assert.AreEqual(Array.Empty<string>(),
                    testServiceProvider.Pollster.TaskProcessing);
  }


  [Test]
  [Timeout(10000)]
  public async Task UnavailableWorkerShouldFail()
  {
    var mockPullQueueStorage = new SimplePullQueueStorageChannel();
    var simpleAgentHandler   = new SimpleAgentHandler();

    var mockStreamHandlerFail = new Mock<IWorkerStreamHandler>();
    mockStreamHandlerFail.Setup(streamHandler => streamHandler.StartTaskProcessing(It.IsAny<TaskData>(),
                                                                                   It.IsAny<string>(),
                                                                                   It.IsAny<string>(),
                                                                                   It.IsAny<CancellationToken>()))
                         .Throws(new TestUnavailableRpcException("Unavailable worker"));


    using var testServiceProvider = new TestPollsterProvider(mockStreamHandlerFail.Object,
                                                             simpleAgentHandler,
                                                             mockPullQueueStorage);

    var (_, _, taskSubmitted) = await InitSubmitter(testServiceProvider.Submitter,
                                                    testServiceProvider.PartitionTable,
                                                    testServiceProvider.ResultTable,
                                                    testServiceProvider.SessionTable,
                                                    CancellationToken.None)
                                  .ConfigureAwait(false);

    await mockPullQueueStorage.Channel.Writer.WriteAsync(new SimpleQueueMessageHandler
                                                         {
                                                           CancellationToken = CancellationToken.None,
                                                           Status            = QueueMessageStatus.Waiting,
                                                           MessageId = Guid.NewGuid()
                                                                           .ToString(),
                                                           TaskId = taskSubmitted,
                                                         })
                              .ConfigureAwait(false);

    await testServiceProvider.Pollster.Init(CancellationToken.None)
                             .ConfigureAwait(false);

    var stop = testServiceProvider.StopApplicationAfter(TimeSpan.FromMilliseconds(300));

    Assert.DoesNotThrowAsync(() => testServiceProvider.Pollster.MainLoop());
    Assert.DoesNotThrowAsync(() => stop);

    Assert.AreEqual(TaskStatus.Submitted,
                    await testServiceProvider.TaskTable.GetTaskStatus(taskSubmitted,
                                                                      CancellationToken.None)
                                             .ConfigureAwait(false));
    Assert.AreEqual(Array.Empty<string>(),
                    testServiceProvider.Pollster.TaskProcessing);

    testServiceProvider.AssertFailAfterError(5);
  }
}
