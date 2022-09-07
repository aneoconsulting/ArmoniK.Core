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
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Worker;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Stream.Worker;
using ArmoniK.Core.Common.Tests.Helpers;
using ArmoniK.Core.Common.Utils;

using Google.Protobuf.WellKnownTypes;

using Moq;

using NUnit.Framework;

using Empty = ArmoniK.Api.gRPC.V1.Empty;
using Output = ArmoniK.Api.gRPC.V1.Output;
using TaskOptions = ArmoniK.Api.gRPC.V1.TaskOptions;
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
    Assert.AreEqual("",
                    testServiceProvider.Pollster.TaskProcessing);
  }

  public class WaitWorkerStreamHandler : IWorkerStreamHandler
  {
    private readonly double delay_;

    public WaitWorkerStreamHandler(double delay)
      => delay_ = delay;

    public ValueTask<bool> Check(HealthCheckTag tag)
      => new(true);

    public Task Init(CancellationToken cancellationToken)
      => Task.CompletedTask;

    public void Dispose()
    {
    }

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
      await Task.Delay(TimeSpan.FromMilliseconds(delay_))
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
    var mockPullQueueStorage    = new Mock<IPullQueueStorage>();
    var waitWorkerStreamHandler = new WaitWorkerStreamHandler(delay);
    var simpleAgentHandler      = new SimpleAgentHandler();

    using var testServiceProvider = new TestPollsterProvider(waitWorkerStreamHandler,
                                                             simpleAgentHandler,
                                                             mockPullQueueStorage.Object);

    var tuple = await InitSubmitter(testServiceProvider.Submitter,
                                    testServiceProvider.PartitionTable,
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
                                           TaskId = tuple.taskSubmitted,
                                         },
                                       }.ToAsyncEnumerable());

    await testServiceProvider.Pollster.Init(CancellationToken.None)
                             .ConfigureAwait(false);

    var source = new CancellationTokenSource(TimeSpan.FromMilliseconds(120));

    Assert.DoesNotThrowAsync(() => testServiceProvider.Pollster.MainLoop(source.Token));
    Assert.True(source.Token.IsCancellationRequested);

    Assert.AreEqual(TaskStatus.Completed,
                    (await testServiceProvider.TaskTable.GetTaskStatus(new[]
                                                                       {
                                                                         tuple.taskSubmitted,
                                                                       },
                                                                       CancellationToken.None)
                                              .ConfigureAwait(false)).Single()
                                                                     .Status);
    Assert.AreEqual("",
                    testServiceProvider.Pollster.TaskProcessing);
  }
}
