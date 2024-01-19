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
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Events;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.Helpers;
using ArmoniK.Utils;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

using NUnit.Framework;

using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

namespace ArmoniK.Core.Common.Tests;

[TestFixture]
public class WatchToGrpcTests
{
  [Test]
  public void WatchShouldSucceed()
  {
    var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));

    var watchToGrpcInstance = new WatchToGrpc(new SimpleTaskTable(),
                                              new SimpleTaskWatcher(),
                                              new SimpleResultTable(),
                                              new SimpleResultWatcher(),
                                              NullLogger.Instance);


    var list = new List<EventSubscriptionResponse>();

    Assert.That(async () =>
                {
                  // Simple* that are used to create this instance return static events
                  await foreach (var eventSubscriptionResponse in watchToGrpcInstance.GetEvents("",
                                                                                                new List<EventsEnum>(),
                                                                                                new Filters(),
                                                                                                new Api.gRPC.V1.Results.Filters(),
                                                                                                cts.Token)
                                                                                     .ConfigureAwait(false))
                  {
                    Console.WriteLine(eventSubscriptionResponse);
                    list.Add(eventSubscriptionResponse);
                  }
                },
                Throws.InstanceOf<OperationCanceledException>());

    Assert.AreEqual(8,
                    list.Count);
  }

  [Test]
  [TestCase(3)]
  [TestCase(6)]
  public async Task MultipleWatchShouldSucceed(int nTries)
  {
    var cts      = new CancellationTokenSource(TimeSpan.FromSeconds(3));
    var list     = new List<EventSubscriptionResponse>();
    var taskList = new List<Task>();

    for (var i = 0; i < nTries; i++)
    {
      taskList.Add(Task.Factory.StartNew(() =>
                                         {
                                           var watchToGrpcInstance = new WatchToGrpc(new SimpleTaskTable(),
                                                                                     new SimpleTaskWatcher(),
                                                                                     new SimpleResultTable(),
                                                                                     new SimpleResultWatcher(),
                                                                                     NullLogger.Instance);

                                           Assert.That(async () =>
                                                       {
                                                         // Simple* that are used to create this instance return static events
                                                         await foreach (var eventSubscriptionResponse in watchToGrpcInstance.GetEvents("",
                                                                                                                                       new List<EventsEnum>(),
                                                                                                                                       new Filters(),
                                                                                                                                       new Api.gRPC.V1.Results.Filters(),
                                                                                                                                       cts.Token)
                                                                                                                            .ConfigureAwait(false))
                                                         {
                                                           Console.WriteLine(eventSubscriptionResponse);
                                                           list.Add(eventSubscriptionResponse);
                                                         }
                                                       },
                                                       Throws.InstanceOf<OperationCanceledException>());
                                         },
                                         CancellationToken.None));
    }

    await taskList.WhenAll()
                  .ConfigureAwait(false);

    Assert.That(list,
                Has.Count.EqualTo(8 * nTries));
  }


  [Test]
  public async Task UseMongoForTasksShouldSucceed()
  {
    using var helper = new TestDatabaseProvider(collection => collection.AddSingleton<WatchToGrpc>(),
                                                useSingleNodeReplicaSet: true);

    var taskTable = helper.GetRequiredService<ITaskTable>();

    await taskTable.CreateTasks(new[]
                                {
                                  taskCompletedData_,
                                  taskCreatingData_,
                                  TaskProcessingData,
                                  taskProcessingData2_,
                                  TaskSubmittedData,
                                  taskFailedData_,
                                })
                   .ConfigureAwait(false);

    var wtg = helper.GetRequiredService<WatchToGrpc>();

    var cts  = new CancellationTokenSource(TimeSpan.FromSeconds(2));
    var list = new List<EventSubscriptionResponse>();

    Assert.That(async () =>
                {
                  // Simple* that are used to create this instance return static events
                  await foreach (var eventSubscriptionResponse in wtg.GetEvents("SessionId",
                                                                                new List<EventsEnum>
                                                                                {
                                                                                  EventsEnum.NewTask,
                                                                                },
                                                                                null,
                                                                                null,
                                                                                cts.Token)
                                                                     .ConfigureAwait(false))
                  {
                    Console.WriteLine(eventSubscriptionResponse);
                    list.Add(eventSubscriptionResponse);
                  }
                },
                Throws.InstanceOf<OperationCanceledException>());

    Assert.That(list,
                Has.Count.EqualTo(6));
  }

  [Test]
  public async Task UseMongoForResultsShouldSucceed()
  {
    using var helper = new TestDatabaseProvider(collection => collection.AddSingleton<WatchToGrpc>(),
                                                useSingleNodeReplicaSet: true);

    var resultTable = helper.GetRequiredService<IResultTable>();

    await resultTable.Create(new[]
                             {
                               new Result("SessionId",
                                          "ResultIsAvailable",
                                          "",
                                          "OwnerId",
                                          ResultStatus.Completed,
                                          new List<string>(),
                                          DateTime.Today,
                                          1,
                                          new[]
                                          {
                                            (byte)1,
                                          }),
                               new Result("SessionId",
                                          "ResultIsNotAvailable",
                                          "",
                                          "OwnerId",
                                          ResultStatus.Aborted,
                                          new List<string>(),
                                          DateTime.Today,
                                          1,
                                          new[]
                                          {
                                            (byte)1,
                                          }),
                               new Result("SessionId",
                                          "ResultIsCreated",
                                          "",
                                          "OwnerId",
                                          ResultStatus.Created,
                                          new List<string>(),
                                          DateTime.Today,
                                          1,
                                          new[]
                                          {
                                            (byte)1,
                                          }),
                               new Result("SessionId",
                                          "ResultIsCreated2",
                                          "",
                                          "OwnerId",
                                          ResultStatus.Created,
                                          new List<string>(),
                                          DateTime.Today,
                                          1,
                                          new[]
                                          {
                                            (byte)1,
                                          }),
                               new Result("SessionId",
                                          "ResultIsCompletedWithDependents",
                                          "",
                                          "OwnerId",
                                          ResultStatus.Completed,
                                          new List<string>
                                          {
                                            "Dependent1",
                                            "Dependent2",
                                          },
                                          DateTime.Today,
                                          1,
                                          new[]
                                          {
                                            (byte)1,
                                          }),
                             })
                     .ConfigureAwait(false);

    var wtg = helper.GetRequiredService<WatchToGrpc>();

    var cts  = new CancellationTokenSource(TimeSpan.FromSeconds(2));
    var list = new List<EventSubscriptionResponse>();

    Assert.That(async () =>
                {
                  // Simple* that are used to create this instance return static events
                  await foreach (var eventSubscriptionResponse in wtg.GetEvents("SessionId",
                                                                                new List<EventsEnum>
                                                                                {
                                                                                  EventsEnum.NewResult,
                                                                                },
                                                                                null,
                                                                                null,
                                                                                cts.Token)
                                                                     .ConfigureAwait(false))
                  {
                    Console.WriteLine(eventSubscriptionResponse);
                    list.Add(eventSubscriptionResponse);
                  }
                },
                Throws.InstanceOf<OperationCanceledException>());

    Assert.That(list,
                Has.Count.EqualTo(5));
  }

  private static readonly TaskOptions Options = new(new Dictionary<string, string>
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

  private static readonly TaskData TaskSubmittedData = new("SessionId",
                                                           "TaskSubmittedId",
                                                           "",
                                                           "",
                                                           "PayloadId",
                                                           new[]
                                                           {
                                                             "parent1",
                                                           },
                                                           new[]
                                                           {
                                                             "dependency1",
                                                           },
                                                           new[]
                                                           {
                                                             "output1",
                                                           },
                                                           Array.Empty<string>(),
                                                           TaskStatus.Submitted,
                                                           Options with
                                                           {
                                                             PartitionId = "part2",
                                                           },
                                                           new Output(false,
                                                                      ""));

  private static readonly TaskData TaskProcessingData = new("SessionId",
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
                                                              "dependency1",
                                                            },
                                                            new[]
                                                            {
                                                              "output1",
                                                            },
                                                            Array.Empty<string>(),
                                                            TaskStatus.Processing,
                                                            Options,
                                                            new Output(false,
                                                                       ""));

  private readonly TaskData taskProcessingData2_ = new("SessionId",
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
                                                         "dependency1",
                                                       },
                                                       new[]
                                                       {
                                                         "output1",
                                                       },
                                                       Array.Empty<string>(),
                                                       TaskStatus.Processing,
                                                       Options,
                                                       new Output(false,
                                                                  ""));

  private readonly TaskData taskFailedData_ = new("SessionId",
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
                                                    "dependency1",
                                                  },
                                                  new[]
                                                  {
                                                    "output1",
                                                  },
                                                  Array.Empty<string>(),
                                                  TaskStatus.Error,
                                                  Options,
                                                  new Output(false,
                                                             "sad task"));

  private readonly TaskData taskCompletedData_ = new("SessionId",
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
                                                       "dependency1",
                                                     },
                                                     new[]
                                                     {
                                                       "output1",
                                                       "output2",
                                                     },
                                                     Array.Empty<string>(),
                                                     TaskStatus.Completed,
                                                     Options,
                                                     new Output(true,
                                                                ""));

  private readonly TaskData taskCreatingData_ = new("SessionId",
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
                                                      "dependency1",
                                                    },
                                                    new[]
                                                    {
                                                      "output1",
                                                    },
                                                    Array.Empty<string>(),
                                                    TaskStatus.Creating,
                                                    Options,
                                                    new Output(false,
                                                               ""));
}
