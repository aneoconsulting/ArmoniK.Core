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
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.Helpers;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using NUnit.Framework;

using Output = ArmoniK.Core.Common.Storage.Output;
using TaskOptions = ArmoniK.Core.Common.Storage.TaskOptions;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Tests;

[TestFixture]
public class DependencyResolverTest
{
  [SetUp]
  public void SetUp()
  {
  }

  [TearDown]
  public void TearDown()
  {
  }

  private const string TaskCreatingWithDependencies          = nameof(TaskCreatingWithDependencies);
  private const string TaskCreatingWithAvailableDependencies = nameof(TaskCreatingWithAvailableDependencies);
  private const string TaskCreatingWithAbortedDependencies   = nameof(TaskCreatingWithAbortedDependencies);
  private const string CreatedData1                          = nameof(CreatedData1);
  private const string AbortedData1                          = nameof(AbortedData1);
  private const string AvailableData1                        = nameof(AvailableData1);
  private const string AvailableData2                        = nameof(AvailableData2);
  private const string TaskCreatingNoDependencies            = nameof(TaskCreatingNoDependencies);
  private const string TaskSubmitted                         = nameof(TaskSubmitted);
  private const string TaskCancelling                        = nameof(TaskCancelling);
  private const string TaskCancelled                         = nameof(TaskCancelled);

  private static async Task<TestDatabaseProvider> Populate()
  {
    var pushQueueStorage = new SimplePushQueueStorage();
    var pullQueueStorage = new SimplePullQueueStorage();

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
                                          TaskCreatingWithAvailableDependencies,
                                          ResultStatus.Completed,
                                          new List<string>(),
                                          DateTime.UtcNow,
                                          Array.Empty<byte>()),
                               new Result("SessionId",
                                          AvailableData2,
                                          TaskCreatingWithAvailableDependencies,
                                          ResultStatus.Completed,
                                          new List<string>(),
                                          DateTime.UtcNow,
                                          Array.Empty<byte>()),
                               new Result("SessionId",
                                          CreatedData1,
                                          TaskCreatingWithDependencies,
                                          ResultStatus.Created,
                                          new List<string>(),
                                          DateTime.UtcNow,
                                          Array.Empty<byte>()),
                               new Result("SessionId",
                                          AbortedData1,
                                          TaskCreatingWithAbortedDependencies,
                                          ResultStatus.Aborted,
                                          new List<string>(),
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
                                               TaskSubmitted,
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
                                               TaskStatus.Submitted,
                                               options,
                                               new Output(false,
                                                          "")),
                                  new TaskData("SessionId",
                                               TaskCreatingWithAvailableDependencies,
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
                                               TaskStatus.Creating,
                                               options,
                                               new Output(false,
                                                          "")),
                                  new TaskData("SessionId",
                                               TaskCreatingWithAbortedDependencies,
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
                                               TaskCreatingNoDependencies,
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
                                               TaskStatus.Creating,
                                               options with
                                               {
                                                 PartitionId = "part2",
                                               },
                                               new Output(false,
                                                          "")),
                                  new TaskData("SessionId",
                                               TaskCreatingWithDependencies,
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
                                               TaskStatus.Creating,
                                               options with
                                               {
                                                 PartitionId = "part2",
                                               },
                                               new Output(false,
                                                          "")),
                                  new TaskData("SessionId",
                                               TaskCancelled,
                                               "",
                                               "",
                                               "PayloadId",
                                               new[]
                                               {
                                                 "parent1",
                                               },
                                               Array.Empty<string>(),
                                               new[]
                                               {
                                                 "output1",
                                               },
                                               Array.Empty<string>(),
                                               TaskStatus.Cancelled,
                                               options with
                                               {
                                                 PartitionId = "part2",
                                               },
                                               new Output(false,
                                                          "")),
                                  new TaskData("SessionId",
                                               TaskCancelling,
                                               "",
                                               "",
                                               "PayloadId",
                                               new[]
                                               {
                                                 "parent1",
                                               },
                                               Array.Empty<string>(),
                                               new[]
                                               {
                                                 "output1",
                                               },
                                               Array.Empty<string>(),
                                               TaskStatus.Cancelling,
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
  [TestCase(TaskCreatingNoDependencies,
            ExpectedResult = TaskLifeCycleHelper.DependenciesStatus.Available)]
  [TestCase(TaskCreatingWithAbortedDependencies,
            ExpectedResult = TaskLifeCycleHelper.DependenciesStatus.Aborted)]
  [TestCase(TaskCreatingWithAvailableDependencies,
            ExpectedResult = TaskLifeCycleHelper.DependenciesStatus.Available)]
  [TestCase(TaskCreatingWithDependencies,
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

    var pullQueueStorage = provider.GetRequiredService<SimplePullQueueStorage>();
    pullQueueStorage.Messages.Add(TaskCreatingNoDependencies);
    pullQueueStorage.Messages.Add(TaskCreatingWithDependencies);
    pullQueueStorage.Messages.Add(TaskCreatingWithAbortedDependencies);
    pullQueueStorage.Messages.Add(TaskCreatingWithAvailableDependencies);
    pullQueueStorage.Messages.Add(TaskSubmitted);
    pullQueueStorage.Messages.Add(TaskCancelled);
    pullQueueStorage.Messages.Add(TaskCancelling);

    var cts = new CancellationTokenSource();
    cts.CancelAfter(TimeSpan.FromSeconds(2));

    await dp.ExecuteAsync(cts.Token)
            .ConfigureAwait(false);

    await Task.Delay(TimeSpan.FromSeconds(1))
              .ConfigureAwait(false);

    var pushQueueStorage = provider.GetRequiredService<SimplePushQueueStorage>();
    Assert.AreEqual(2,
                    pushQueueStorage.Messages.Count);
    Assert.Contains(TaskCreatingWithAvailableDependencies,
                    pushQueueStorage.Messages);
    Assert.Contains(TaskCreatingNoDependencies,
                    pushQueueStorage.Messages);
    Assert.IsFalse(pushQueueStorage.Messages.Contains(TaskSubmitted));
    Assert.IsFalse(pushQueueStorage.Messages.Contains(TaskCancelled));
    Assert.IsFalse(pushQueueStorage.Messages.Contains(TaskCancelling));

    var taskTable = provider.GetRequiredService<ITaskTable>();
    Assert.AreEqual(TaskStatus.Error,
                    taskTable.ReadTaskAsync(TaskCreatingWithAbortedDependencies,
                                            CancellationToken.None)
                             .Result.Status);
    Assert.AreEqual(TaskStatus.Submitted,
                    taskTable.ReadTaskAsync(TaskCreatingNoDependencies,
                                            CancellationToken.None)
                             .Result.Status);
    Assert.AreEqual(TaskStatus.Submitted,
                    taskTable.ReadTaskAsync(TaskCreatingWithAvailableDependencies,
                                            CancellationToken.None)
                             .Result.Status);
    Assert.AreEqual(TaskStatus.Creating,
                    taskTable.ReadTaskAsync(TaskCreatingWithDependencies,
                                            CancellationToken.None)
                             .Result.Status);
    Assert.AreEqual(TaskStatus.Cancelled,
                    taskTable.ReadTaskAsync(TaskCancelled,
                                            CancellationToken.None)
                             .Result.Status);
    Assert.AreEqual(TaskStatus.Cancelled,
                    taskTable.ReadTaskAsync(TaskCancelling,
                                            CancellationToken.None)
                             .Result.Status);
  }
}
