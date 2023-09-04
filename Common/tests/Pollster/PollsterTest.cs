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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Worker;
using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Stream.Worker;
using ArmoniK.Core.Common.Tests.Helpers;
using ArmoniK.Core.Common.Utils;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using Moq;

using NUnit.Framework;

using Output = ArmoniK.Api.gRPC.V1.Output;
using TaskOptions = ArmoniK.Core.Base.DataStructures.TaskOptions;
using TaskRequest = ArmoniK.Core.Common.gRPC.Services.TaskRequest;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

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
                                          ResultStatus.Created,
                                          new List<string>(),
                                          DateTime.UtcNow,
                                          Array.Empty<byte>()),
                               new Result(sessionId,
                                          ExpectedOutput2,
                                          "",
                                          "",
                                          ResultStatus.Created,
                                          new List<string>(),
                                          DateTime.UtcNow,
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

    await submitter.FinalizeTaskCreation(requests,
                                         sessionId,
                                         sessionId,
                                         CancellationToken.None)
                   .ConfigureAwait(false);

    return (sessionId, taskCreating, taskSubmitted);
  }


  [Test]
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

    public IAsyncPipe<ProcessReply, ProcessRequest>? Pipe { get; }

    public void StartTaskProcessing(TaskData          taskData,
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

    public IAsyncPipe<ProcessReply, ProcessRequest>? Pipe { get; }

    public void StartTaskProcessing(TaskData          taskData,
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
    Assert.AreEqual(HealthStatus.Healthy,
                    (await testServiceProvider.Pollster.Check(HealthCheckTag.Readiness)
                                              .ConfigureAwait(false)).Status);
    Assert.AreEqual(HealthStatus.Healthy,
                    (await testServiceProvider.Pollster.Check(HealthCheckTag.Startup)
                                              .ConfigureAwait(false)).Status);
  }

  [Test]
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
    var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
    await testServiceProvider.Pollster.MainLoop(cancellationTokenSource.Token)
                             .ConfigureAwait(false);
    Assert.False(testServiceProvider.Pollster.Failed);
    Assert.IsFalse(cancellationTokenSource.IsCancellationRequested);
  }

  [Test]
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

    var source = new CancellationTokenSource(TimeSpan.FromMilliseconds(105));

    Assert.DoesNotThrowAsync(() => testServiceProvider.Pollster.MainLoop(source.Token));
    Assert.True(source.Token.IsCancellationRequested);
    Assert.AreEqual(string.Empty,
                    testServiceProvider.Pollster.TaskProcessing);
    Assert.AreSame(string.Empty,
                   testServiceProvider.Pollster.TaskProcessing);
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

    public IAsyncPipe<ProcessReply, ProcessRequest>? Pipe { get; private set; }

    public void StartTaskProcessing(TaskData          taskData,
                                    CancellationToken cancellationToken)
      => Pipe = new WaitAsyncPipe(delay_);
  }

  public class WaitAsyncPipe : IAsyncPipe<ProcessReply, ProcessRequest>
  {
    private readonly double delay_;

    public WaitAsyncPipe(double delay)
      => delay_ = delay;

    public async Task<ProcessReply> ReadAsync(CancellationToken cancellationToken)
    {
      await Task.Delay(TimeSpan.FromMilliseconds(delay_),
                       CancellationToken.None)
                .ConfigureAwait(false);
      return new ProcessReply
             {
               CommunicationToken = "",
               Output = new Output
                        {
                          Ok = new Empty(),
                        },
             };
    }

    public Task WriteAsync(ProcessRequest message)
      => Task.CompletedTask;

    public Task WriteAsync(IEnumerable<ProcessRequest> message)
      => Task.CompletedTask;

    public Task CompleteAsync()
      => Task.CompletedTask;
  }

  [Test]
  [TestCase(100)]
  [TestCase(5000)] // task should be longer than the grace delay
  public async Task ExecuteTaskShouldSucceed(double delay)
  {
    var mockPullQueueStorage    = new SimplePullQueueStorageChannel();
    var waitWorkerStreamHandler = new WaitWorkerStreamHandler(delay);
    var simpleAgentHandler      = new SimpleAgentHandler();

    using var testServiceProvider = new TestPollsterProvider(waitWorkerStreamHandler,
                                                             simpleAgentHandler,
                                                             mockPullQueueStorage);

    var (_, _, taskSubmitted) = await InitSubmitter(testServiceProvider.Submitter,
                                                    testServiceProvider.PartitionTable,
                                                    testServiceProvider.ResultTable,
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

    var source = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));

    Assert.DoesNotThrowAsync(() => testServiceProvider.Pollster.MainLoop(source.Token));
    Assert.False(testServiceProvider.Pollster.Failed);
    Assert.True(source.Token.IsCancellationRequested);

    Assert.AreEqual(delay < 1000
                      ? TaskStatus.Completed
                      : TaskStatus.Processing,
                    (await testServiceProvider.TaskTable.GetTaskStatus(new[]
                                                                       {
                                                                         taskSubmitted,
                                                                       },
                                                                       CancellationToken.None)
                                              .ConfigureAwait(false)).Single()
                                                                     .Status);
    Assert.AreEqual(string.Empty,
                    testServiceProvider.Pollster.TaskProcessing);
    Assert.AreSame(string.Empty,
                   testServiceProvider.Pollster.TaskProcessing);
  }

  [Test]
  public async Task CancelLongTaskShouldSucceed()
  {
    var mockPullQueueStorage    = new Mock<IPullQueueStorage>();
    var waitWorkerStreamHandler = new ExceptionWorkerStreamHandler<Exception>(15000);
    var simpleAgentHandler      = new SimpleAgentHandler();

    using var testServiceProvider = new TestPollsterProvider(waitWorkerStreamHandler,
                                                             simpleAgentHandler,
                                                             mockPullQueueStorage.Object);

    var (_, _, taskSubmitted) = await InitSubmitter(testServiceProvider.Submitter,
                                                    testServiceProvider.PartitionTable,
                                                    testServiceProvider.ResultTable,
                                                    CancellationToken.None)
                                  .ConfigureAwait(false);

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
                                           TaskId = taskSubmitted,
                                         },
                                       }.ToAsyncEnumerable());

    await testServiceProvider.Pollster.Init(CancellationToken.None)
                             .ConfigureAwait(false);

    var source = new CancellationTokenSource(TimeSpan.FromSeconds(5));

    var mainLoopTask = testServiceProvider.Pollster.MainLoop(source.Token);

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

    await testServiceProvider.Pollster.StopCancelledTask!.Invoke()
                             .ConfigureAwait(false);

    Assert.DoesNotThrowAsync(() => mainLoopTask);
    Assert.False(testServiceProvider.Pollster.Failed);
    Assert.True(source.Token.IsCancellationRequested);

    Assert.That((await testServiceProvider.TaskTable.GetTaskStatus(new[]
                                                                   {
                                                                     taskSubmitted,
                                                                   },
                                                                   CancellationToken.None)
                                          .ConfigureAwait(false)).Single()
                                                                 .Status,
                Is.AnyOf(TaskStatus.Cancelled,
                         TaskStatus.Cancelling));
    Assert.AreEqual(string.Empty,
                    testServiceProvider.Pollster.TaskProcessing);
    Assert.AreSame(string.Empty,
                   testServiceProvider.Pollster.TaskProcessing);
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

    var source = new CancellationTokenSource(TimeSpan.FromSeconds(10));


    Assert.DoesNotThrowAsync(() => pollster.MainLoop(source.Token));
    Assert.True(pollster.Failed);
    Assert.False(source.Token.IsCancellationRequested);
    Assert.AreEqual(string.Empty,
                    pollster.TaskProcessing);
    Assert.AreSame(string.Empty,
                   pollster.TaskProcessing);
  }


  [Test]
  public async Task UnavailableWorkerShouldFail()
  {
    var mockPullQueueStorage = new SimplePullQueueStorageChannel();
    var simpleAgentHandler   = new SimpleAgentHandler();

    var mockStreamHandlerFail = new Mock<IWorkerStreamHandler>();
    mockStreamHandlerFail.Setup(streamHandler => streamHandler.StartTaskProcessing(It.IsAny<TaskData>(),
                                                                                   It.IsAny<CancellationToken>()))
                         .Throws(new TestUnavailableRpcException("Unavailable worker"));


    using var testServiceProvider = new TestPollsterProvider(mockStreamHandlerFail.Object,
                                                             simpleAgentHandler,
                                                             mockPullQueueStorage);

    var (_, _, taskSubmitted) = await InitSubmitter(testServiceProvider.Submitter,
                                                    testServiceProvider.PartitionTable,
                                                    testServiceProvider.ResultTable,
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

    var source = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));

    Assert.DoesNotThrowAsync(() => testServiceProvider.Pollster.MainLoop(source.Token));

    Assert.AreEqual(TaskStatus.Submitted,
                    (await testServiceProvider.TaskTable.GetTaskStatus(new[]
                                                                       {
                                                                         taskSubmitted,
                                                                       },
                                                                       CancellationToken.None)
                                              .ConfigureAwait(false)).Single()
                                                                     .Status);
    Assert.AreEqual(string.Empty,
                    testServiceProvider.Pollster.TaskProcessing);
    Assert.AreSame(string.Empty,
                   testServiceProvider.Pollster.TaskProcessing);
  }
}
