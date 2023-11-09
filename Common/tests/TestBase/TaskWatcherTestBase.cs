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

using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Storage.Events;

using Microsoft.Extensions.Diagnostics.HealthChecks;

using NUnit.Framework;

using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

namespace ArmoniK.Core.Common.Tests.TestBase;

[TestFixture]
public class TaskWatcherTestBase
{
  [SetUp]
  public async Task SetUp()
  {
    GetInstance();

    if (!RunTests || CheckForSkipSetup())
    {
      return;
    }

    await TaskTable!.Init(CancellationToken.None)
                    .ConfigureAwait(false);

    await TaskTable!.CreateTasks(new[]
                                 {
                                   taskCompletedData_,
                                   taskCreatingData_,
                                   TaskProcessingData,
                                   taskProcessingData2_,
                                   TaskSubmittedData,
                                   taskFailedData_,
                                 })
                    .ConfigureAwait(false);
  }

  [TearDown]
  public virtual void TearDown()
  {
    TaskTable   = null;
    TaskWatcher = null;
    RunTests    = false;
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

  private static readonly TaskData TaskEventCreating1 = new("SessionId",
                                                            "TaskEventCreating1",
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
                                                            TaskStatus.Creating,
                                                            Options,
                                                            new Output(true,
                                                                       ""));

  private static readonly TaskData TaskEventCreating2 = new("SessionId",
                                                            "TaskEventCreating2",
                                                            "OwnerPodId",
                                                            "OwnerPodName",
                                                            "PayloadId",
                                                            new[]
                                                            {
                                                              "parent2",
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
                                                            TaskStatus.Creating,
                                                            Options,
                                                            new Output(true,
                                                                       ""));

  private static bool CheckForSkipSetup()
  {
    var category = TestContext.CurrentContext.Test.Properties.Get("Category") as string;
    return category is "SkipSetUp";
  }


  /* Interface to test */
  protected ITaskTable?   TaskTable;
  protected ITaskWatcher? TaskWatcher;

  /* Boolean to control that tests are executed in
   * an instance of this class */
  protected bool RunTests;

  /* Function be override so it returns the suitable instance
   * of ResultTable and ResultWatcher to the corresponding interface implementation */
  public virtual void GetInstance()
  {
  }

  [Test]
  [Category("SkipSetUp")]
  public async Task InitShouldSucceed()
  {
    if (RunTests)
    {
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await TaskWatcher!.Check(HealthCheckTag.Liveness)
                                            .ConfigureAwait(false)).Status);
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await TaskWatcher.Check(HealthCheckTag.Readiness)
                                           .ConfigureAwait(false)).Status);
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await TaskWatcher.Check(HealthCheckTag.Startup)
                                           .ConfigureAwait(false)).Status);

      await TaskWatcher.Init(CancellationToken.None)
                       .ConfigureAwait(false);

      Assert.AreEqual(HealthStatus.Healthy,
                      (await TaskWatcher.Check(HealthCheckTag.Liveness)
                                        .ConfigureAwait(false)).Status);
      Assert.AreEqual(HealthStatus.Healthy,
                      (await TaskWatcher.Check(HealthCheckTag.Readiness)
                                        .ConfigureAwait(false)).Status);
      Assert.AreEqual(HealthStatus.Healthy,
                      (await TaskWatcher.Check(HealthCheckTag.Startup)
                                        .ConfigureAwait(false)).Status);
    }
  }

  /// <summary>
  ///   This method produces the events (new task and status update)
  ///   that will be used to test the ITaskWatcher interface.
  /// </summary>
  /// <param name="taskTable">Task table interface</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  private static async Task ProduceEvents(ITaskTable        taskTable,
                                          CancellationToken cancellationToken)
  {
    await taskTable.CreateTasks(new[]
                                {
                                  TaskEventCreating1,
                                  TaskEventCreating2,
                                },
                                cancellationToken)
                   .ConfigureAwait(false);

    await taskTable.SetTaskErrorAsync(TaskProcessingData,
                                      "Testing SetTaskError",
                                      CancellationToken.None)
                   .ConfigureAwait(false);

    await taskTable.AcquireTask(TaskSubmittedData with
                                {
                                  AcquisitionDate = DateTime.UtcNow,
                                  OwnerPodId = "OwnerPodId",
                                  OwnerPodName = "OwnerPodName",
                                },
                                CancellationToken.None)
                   .ConfigureAwait(false);

    await taskTable.StartTask(TaskSubmittedData,
                              CancellationToken.None)
                   .ConfigureAwait(false);

    await taskTable.CancelTaskAsync(new List<string>
                                    {
                                      "TaskSubmittedId",
                                    },
                                    CancellationToken.None)
                   .ConfigureAwait(false);
  }

  private static NewTask TaskDataToNewTask(TaskData taskData)
    => new(taskData.SessionId,
           taskData.TaskId,
           taskData.InitialTaskId,
           taskData.PayloadId,
           taskData.ParentTaskIds.ToList(),
           taskData.ExpectedOutputIds.ToList(),
           taskData.DataDependencies.ToList(),
           taskData.RetryOfIds.ToList(),
           taskData.Status);

  [Test]
  public async Task WatchNewTaskShouldSucceed()
  {
    if (RunTests)
    {
      var cts = new CancellationTokenSource();

      var watchEnumerator = await TaskWatcher!.GetNewTasks(data => data.SessionId == "SessionId",
                                                           cts.Token)
                                              .ConfigureAwait(false);

      var newResults = new List<NewTask>();
      var watch = Task.Run(async () =>
                           {
                             await foreach (var cur in watchEnumerator.WithCancellation(cts.Token)
                                                                      .ConfigureAwait(false))
                             {
                               Console.WriteLine(cur);
                               newResults.Add(cur);
                             }
                           },
                           CancellationToken.None);

      await Task.Delay(TimeSpan.FromMilliseconds(20),
                       CancellationToken.None)
                .ConfigureAwait(false);

      await ProduceEvents(TaskTable!,
                          cts.Token)
        .ConfigureAwait(false);

      cts.CancelAfter(TimeSpan.FromSeconds(1));

      Assert.ThrowsAsync<OperationCanceledException>(async () => await watch.ConfigureAwait(false));

      Assert.AreEqual(2,
                      newResults.Count);
      Assert.AreEqual(TaskDataToNewTask(TaskEventCreating1),
                      newResults[0]);
      Assert.AreEqual(TaskDataToNewTask(TaskEventCreating2),
                      newResults[1]);
    }
  }

  [Test]
  public async Task WatchTaskStatusUpdateShouldSucceed()
  {
    if (RunTests)
    {
      var cts = new CancellationTokenSource();

      var watchEnumerator = await TaskWatcher!.GetTaskStatusUpdates(data => data.SessionId == "SessionId",
                                                                    cts.Token)
                                              .ConfigureAwait(false);

      var newResults = new List<TaskStatusUpdate>();
      var watch = Task.Run(async () =>
                           {
                             await foreach (var cur in watchEnumerator.WithCancellation(cts.Token)
                                                                      .ConfigureAwait(false))
                             {
                               Console.WriteLine(cur);
                               newResults.Add(cur);
                             }
                           },
                           CancellationToken.None);

      await Task.Delay(TimeSpan.FromMilliseconds(20),
                       CancellationToken.None)
                .ConfigureAwait(false);

      await ProduceEvents(TaskTable!,
                          cts.Token)
        .ConfigureAwait(false);

      cts.CancelAfter(TimeSpan.FromMilliseconds(100));

      Assert.ThrowsAsync<OperationCanceledException>(async () => await watch.ConfigureAwait(false));

      Assert.AreEqual(4,
                      newResults.Count);
      Assert.AreEqual(new TaskStatusUpdate("SessionId",
                                           TaskProcessingData.TaskId,
                                           TaskStatus.Error),
                      newResults[0]);
      Assert.AreEqual(new TaskStatusUpdate("SessionId",
                                           TaskSubmittedData.TaskId,
                                           TaskStatus.Dispatched),
                      newResults[1]);
      Assert.AreEqual(new TaskStatusUpdate("SessionId",
                                           TaskSubmittedData.TaskId,
                                           TaskStatus.Processing),
                      newResults[2]);
      Assert.AreEqual(new TaskStatusUpdate("SessionId",
                                           TaskSubmittedData.TaskId,
                                           TaskStatus.Cancelling),
                      newResults[3]);
    }
  }

  [Test]
  public async Task WatchTaskStatusUpdateWithComplexFilterShouldSucceed()
  {
    if (RunTests)
    {
      var cts = new CancellationTokenSource();

      var watchEnumerator = await TaskWatcher!
                                  .GetTaskStatusUpdates(data => data.SessionId               == "SessionId" && data.Options.PartitionId == "part1" &&
                                                                data.Options.Options["key1"] == "val1",
                                                        cts.Token)
                                  .ConfigureAwait(false);

      var newResults = new List<TaskStatusUpdate>();
      var watch = Task.Run(async () =>
                           {
                             await foreach (var cur in watchEnumerator.WithCancellation(cts.Token)
                                                                      .ConfigureAwait(false))
                             {
                               Console.WriteLine(cur);
                               newResults.Add(cur);
                             }
                           },
                           CancellationToken.None);

      await Task.Delay(TimeSpan.FromMilliseconds(20),
                       CancellationToken.None)
                .ConfigureAwait(false);

      await ProduceEvents(TaskTable!,
                          cts.Token)
        .ConfigureAwait(false);

      cts.CancelAfter(TimeSpan.FromMilliseconds(100));

      Assert.ThrowsAsync<OperationCanceledException>(async () => await watch.ConfigureAwait(false));

      Assert.AreEqual(1,
                      newResults.Count);
      Assert.AreEqual(new TaskStatusUpdate("SessionId",
                                           TaskProcessingData.TaskId,
                                           TaskStatus.Error),
                      newResults[0]);
    }
  }
}
