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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.Helpers;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using NUnit.Framework;

using Output = ArmoniK.Core.Common.Storage.Output;
using TaskOptions = ArmoniK.Core.Common.Storage.TaskOptions;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Tests;

[TestFixture]
public class DependencyResolverTest
{
  private const string TaskSubmittedWithDependencies          = nameof(TaskSubmittedWithDependencies);
  private const string TaskSubmittedWithAvailableDependencies = nameof(TaskSubmittedWithAvailableDependencies);
  private const string TaskSubmittedWithAbortedDependencies   = nameof(TaskSubmittedWithAbortedDependencies);
  private const string CreatedData1                           = nameof(CreatedData1);
  private const string AbortedData1                           = nameof(AbortedData1);
  private const string AvailableData1                         = nameof(AvailableData1);
  private const string AvailableData2                         = nameof(AvailableData2);
  private const string TaskSubmittedNoDependencies            = nameof(TaskSubmittedNoDependencies);

  [SetUp]
  public void SetUp()
  {
  }

  [TearDown]
  public void TearDown()
  {
  }

  public class MyMessageHandler : IQueueMessageHandler
  {
    public MyMessageHandler(CancellationToken  cancellationToken,
                            string             messageId,
                            string             taskId,
                            QueueMessageStatus status,
                            DateTime           receptionDateTime)
    {
      CancellationToken = cancellationToken;
      MessageId         = messageId;
      TaskId            = taskId;
      Status            = status;
      ReceptionDateTime = receptionDateTime;
    }

    public ValueTask DisposeAsync()
      => ValueTask.CompletedTask;

    public CancellationToken  CancellationToken { get; set; }
    public string             MessageId         { get; }
    public string             TaskId            { get; }
    public QueueMessageStatus Status            { get; set; }
    public DateTime           ReceptionDateTime { get; init; }
  }

  public class MyPullQueueStorage : IPullQueueStorage
  {
    public readonly ConcurrentBag<string> Messages;

    public MyPullQueueStorage()
    {
      Messages = new ConcurrentBag<string>();
    }

    public Task<HealthCheckResult> Check(HealthCheckTag tag)
      => throw new System.NotImplementedException();

    public Task Init(CancellationToken cancellationToken)
      => throw new System.NotImplementedException();

    public int MaxPriority { get; } = 10;

    public async IAsyncEnumerable<IQueueMessageHandler> PullMessagesAsync(int                                        nbMessages,
                                                                          [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
      int i = 0;
      while (i < nbMessages && Messages.TryTake(out var m))
      {
        i++;
        cancellationToken.ThrowIfCancellationRequested();
        yield return new MyMessageHandler(CancellationToken.None,
                                          Guid.NewGuid()
                                              .ToString(),
                                          m,
                                          QueueMessageStatus.Running,
                                          DateTime.UtcNow);
      }
    }
  }

  public class MyPushQueueStorage : IPushQueueStorage
  {
    public readonly ConcurrentBag<string> Messages;

    public MyPushQueueStorage()
    {
      Messages = new ConcurrentBag<string>();
    }

    public Task<HealthCheckResult> Check(HealthCheckTag tag)
      => throw new System.NotImplementedException();

    public Task Init(CancellationToken cancellationToken)
      => throw new System.NotImplementedException();

    public int MaxPriority { get; } = 10;

    public Task PushMessagesAsync(IEnumerable<string> messages,
                                  string              partitionId,
                                  int                 priority          = 1,
                                  CancellationToken   cancellationToken = default)
    {
      foreach (var message in messages)
      {
        Messages.Add(message);
      }

      return Task.CompletedTask;
    }
  }

  private static async Task<TestDatabaseProvider> Populate()
  {
    var pushQueueStorage = new MyPushQueueStorage();
    var pullQueueStorage = new MyPullQueueStorage();

    var provider = new TestDatabaseProvider(collection =>
                                            {
                                              collection.AddSingleton<DependencyResolver.DependencyResolver>();
                                              collection.AddSingleton<IPushQueueStorage>(pushQueueStorage);
                                              collection.AddSingleton<IPullQueueStorage>(pullQueueStorage);
                                              collection.AddSingleton(pullQueueStorage);
                                              collection.AddSingleton(pushQueueStorage);
                                            });

    var resultTable = provider.GetRequiredService<IResultTable>();
    var taskTable   = provider.GetRequiredService<ITaskTable>();

    TaskOptions options = new(new Dictionary<string, string>
                              {
                                {
                                  "key1", "val1"
                                },
                                {
                                  "key2", "val2"
                                },
                              },
                              TimeSpan.MaxValue,
                              5,
                              1,
                              "part1",
                              "applicationName",
                              "applicationVersion",
                              "applicationNamespace",
                              "applicationService",
                              "engineType");

    await resultTable.Create(new[]
                             {
                               new Result("SessionId",
                                          AvailableData1,
                                          TaskSubmittedWithAvailableDependencies,
                                          ResultStatus.Completed,
                                          DateTime.UtcNow,
                                          Array.Empty<byte>()),
                               new Result("SessionId",
                                          AvailableData2,
                                          TaskSubmittedWithAvailableDependencies,
                                          ResultStatus.Completed,
                                          DateTime.UtcNow,
                                          Array.Empty<byte>()),
                               new Result("SessionId",
                                          CreatedData1,
                                          TaskSubmittedWithDependencies,
                                          ResultStatus.Created,
                                          DateTime.UtcNow,
                                          Array.Empty<byte>()),
                               new Result("SessionId",
                                          AbortedData1,
                                          TaskSubmittedWithAbortedDependencies,
                                          ResultStatus.Aborted,
                                          DateTime.UtcNow,
                                          Array.Empty<byte>()),
                             })
                     .ConfigureAwait(false);

    await taskTable.CreateTasks(new[]
                                {
                                  new TaskData("SessionId",
                                               "TaskCompletedId",
                                               "OwnerPodId",
                                               "OwnerPodName",
                                               "PayloadId",
                                               new[]
                                               {
                                                 "parent1",
                                               },
                                               new[]
                                               {
                                                 CreatedData1,
                                               },
                                               new[]
                                               {
                                                 "output1",
                                                 "output2",
                                               },
                                               Array.Empty<string>(),
                                               TaskStatus.Completed,
                                               options,
                                               new Output(true,
                                                          "")),
                                  new TaskData("SessionId",
                                               "TaskCreatingId",
                                               "OwnerPodId",
                                               "OwnerPodName",
                                               "PayloadId",
                                               new[]
                                               {
                                                 "parent1",
                                               },
                                               new[]
                                               {
                                                 CreatedData1,
                                               },
                                               new[]
                                               {
                                                 "output1",
                                               },
                                               Array.Empty<string>(),
                                               TaskStatus.Creating,
                                               options,
                                               new Output(false,
                                                          "")),
                                  new TaskData("SessionId",
                                               TaskSubmittedWithAvailableDependencies,
                                               "OwnerPodId",
                                               "OwnerPodName",
                                               "PayloadId",
                                               new[]
                                               {
                                                 "parent1",
                                               },
                                               new[]
                                               {
                                                 AvailableData1,
                                                 AvailableData2,
                                               },
                                               new[]
                                               {
                                                 "output1",
                                               },
                                               Array.Empty<string>(),
                                               TaskStatus.Submitted,
                                               options,
                                               new Output(false,
                                                          "")),
                                  new TaskData("SessionId",
                                               TaskSubmittedWithAbortedDependencies,
                                               "OwnerPodId",
                                               "OwnerPodName",
                                               "PayloadId",
                                               new[]
                                               {
                                                 "parent1",
                                               },
                                               new[]
                                               {
                                                 AbortedData1,
                                               },
                                               new[]
                                               {
                                                 "output1",
                                               },
                                               Array.Empty<string>(),
                                               TaskStatus.Creating,
                                               options,
                                               new Output(false,
                                                          "")),
                                  new TaskData("SessionId",
                                               "TaskProcessingId",
                                               "OwnerPodId",
                                               "OwnerPodName",
                                               "PayloadId",
                                               new[]
                                               {
                                                 "parent1",
                                               },
                                               new[]
                                               {
                                                 CreatedData1,
                                               },
                                               new[]
                                               {
                                                 "output1",
                                               },
                                               Array.Empty<string>(),
                                               TaskStatus.Processing,
                                               options,
                                               new Output(false,
                                                          "")),
                                  new TaskData("SessionId",
                                               "TaskAnotherProcessingId",
                                               "OwnerPodId",
                                               "OwnerPodName",
                                               "PayloadId",
                                               new[]
                                               {
                                                 "parent1",
                                               },
                                               new[]
                                               {
                                                 CreatedData1,
                                               },
                                               new[]
                                               {
                                                 "output1",
                                               },
                                               Array.Empty<string>(),
                                               TaskStatus.Processing,
                                               options,
                                               new Output(false,
                                                          "")),
                                  new TaskData("SessionId",
                                               TaskSubmittedNoDependencies,
                                               "",
                                               "",
                                               "PayloadId",
                                               new[]
                                               {
                                                 "parent1",
                                               },
                                               new List<string>(),
                                               new[]
                                               {
                                                 "output1",
                                               },
                                               Array.Empty<string>(),
                                               TaskStatus.Submitted,
                                               options with
                                               {
                                                 PartitionId = "part2",
                                               },
                                               new Output(false,
                                                          "")),
                                  new TaskData("SessionId",
                                               TaskSubmittedWithDependencies,
                                               "",
                                               "",
                                               "PayloadId",
                                               new[]
                                               {
                                                 "parent1",
                                               },
                                               new[]
                                               {
                                                 CreatedData1,
                                               },
                                               new[]
                                               {
                                                 "output1",
                                               },
                                               Array.Empty<string>(),
                                               TaskStatus.Submitted,
                                               options with
                                               {
                                                 PartitionId = "part2",
                                               },
                                               new Output(false,
                                                          "")),
                                  new TaskData("SessionId",
                                               "TaskFailedId",
                                               "OwnerPodId",
                                               "OwnerPodName",
                                               "PayloadId",
                                               new[]
                                               {
                                                 "parent1",
                                               },
                                               new[]
                                               {
                                                 CreatedData1,
                                               },
                                               new[]
                                               {
                                                 "output1",
                                               },
                                               Array.Empty<string>(),
                                               TaskStatus.Error,
                                               options,
                                               new Output(false,
                                                          "sad task")),
                                })
                   .ConfigureAwait(false);

    return provider;
  }


  [Test]
  public async Task Init()
  {
    using var provider = await Populate()
                           .ConfigureAwait(false);
    var dp = provider.GetRequiredService<DependencyResolver.DependencyResolver>();
    await dp.Init(CancellationToken.None)
            .ConfigureAwait(false);
  }


  [Test]
  [TestCase(TaskSubmittedNoDependencies,
            ExpectedResult = TaskLifeCycleHelper.DependenciesStatus.Available)]
  [TestCase(TaskSubmittedWithAbortedDependencies,
            ExpectedResult = TaskLifeCycleHelper.DependenciesStatus.Aborted)]
  [TestCase(TaskSubmittedWithAvailableDependencies,
            ExpectedResult = TaskLifeCycleHelper.DependenciesStatus.Available)]
  [TestCase(TaskSubmittedWithDependencies,
            ExpectedResult = TaskLifeCycleHelper.DependenciesStatus.Processing)]
  public async Task<TaskLifeCycleHelper.DependenciesStatus> ValidateDependenciesStatus(string taskId)
  {
    using var provider = await Populate()
                           .ConfigureAwait(false);

    var logger      = provider.GetRequiredService<ILogger>();
    var resultTable = provider.GetRequiredService<IResultTable>();
    var taskTable   = provider.GetRequiredService<ITaskTable>();

    var taskData = await taskTable.ReadTaskAsync(taskId,
                                                 CancellationToken.None)
                                  .ConfigureAwait(false);

    return await TaskLifeCycleHelper.CheckTaskDependencies(taskData,
                                                           resultTable,
                                                           logger,
                                                           CancellationToken.None)
                                    .ConfigureAwait(false);
  }


  [Test]
  public async Task CheckForDependenciesShouldSucceed()
  {
    using var provider = await Populate()
                           .ConfigureAwait(false);
    var dp = provider.GetRequiredService<DependencyResolver.DependencyResolver>();
    await dp.Init(CancellationToken.None)
            .ConfigureAwait(false);

    var pullQueueStorage = provider.GetRequiredService<MyPullQueueStorage>();
    pullQueueStorage.Messages.Add(TaskSubmittedNoDependencies);
    pullQueueStorage.Messages.Add(TaskSubmittedWithDependencies);
    pullQueueStorage.Messages.Add(TaskSubmittedWithAbortedDependencies);
    pullQueueStorage.Messages.Add(TaskSubmittedWithAvailableDependencies);

    var cts = new CancellationTokenSource();
    cts.CancelAfter(TimeSpan.FromSeconds(2));

    await dp.StartAsync(cts.Token)
            .ConfigureAwait(false);

    await Task.Delay(TimeSpan.FromSeconds(1))
              .ConfigureAwait(false);

    await dp.StopAsync(cts.Token)
            .ConfigureAwait(false);

    var pushQueueStorage = provider.GetRequiredService<MyPushQueueStorage>();
    Assert.AreEqual(2,
                    pushQueueStorage.Messages.Count);
    Assert.Contains(TaskSubmittedWithAvailableDependencies,
                    pushQueueStorage.Messages);
    Assert.Contains(TaskSubmittedNoDependencies,
                    pushQueueStorage.Messages);

    var taskTable = provider.GetRequiredService<ITaskTable>();
    Assert.AreEqual(TaskStatus.Error,
                    taskTable.ReadTaskAsync(TaskSubmittedWithAbortedDependencies,
                                            CancellationToken.None)
                             .Result.Status);
    Assert.AreEqual(TaskStatus.Submitted,
                    taskTable.ReadTaskAsync(TaskSubmittedNoDependencies,
                                            CancellationToken.None)
                             .Result.Status);
    Assert.AreEqual(TaskStatus.Submitted,
                    taskTable.ReadTaskAsync(TaskSubmittedWithAvailableDependencies,
                                            CancellationToken.None)
                             .Result.Status);
    Assert.AreEqual(TaskStatus.Submitted,
                    taskTable.ReadTaskAsync(TaskSubmittedWithDependencies,
                                            CancellationToken.None)
                             .Result.Status);
  }
}
