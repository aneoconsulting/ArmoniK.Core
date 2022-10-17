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
// but WITHOUT ANY WARRANTY; without even the implied warranty of
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

using ArmoniK.Api.gRPC.V1.Applications;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC.Validators;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;

using Microsoft.Extensions.Diagnostics.HealthChecks;

using NUnit.Framework;

using static Google.Protobuf.WellKnownTypes.Timestamp;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Tests;

[TestFixture]
public class TaskTableTestBase
{
  [SetUp]
  public void SetUp()
  {
    GetTaskTableInstance();

    if (RunTests)
    {
      TaskTable!.CreateTasks(new[]
                             {
                               new TaskData("SessionId",
                                            "TaskCompletedId",
                                            "OwnerPodId",
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
                                            options_,
                                            new Output(true,
                                                       "")),
                               new TaskData("SessionId",
                                            "TaskCreatingId",
                                            "OwnerPodId",
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
                                            options_,
                                            new Output(false,
                                                       "")),
                               new TaskData("SessionId",
                                            "TaskProcessingId",
                                            "OwnerPodId",
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
                                            options_,
                                            new Output(false,
                                                       "")),
                               new TaskData("SessionId",
                                            "TaskAnotherProcessingId",
                                            "OwnerPodId",
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
                                            options_,
                                            new Output(false,
                                                       "")),
                               new TaskData("SessionId",
                                            "TaskSubmittedId",
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
                                            options_ with
                                            {
                                              PartitionId = "part2",
                                            },
                                            new Output(false,
                                                       "")),
                               new TaskData("SessionId",
                                            "TaskFailedId",
                                            "OwnerPodId",
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
                                            options_,
                                            new Output(false,
                                                       "sad task")),
                             })
                .Wait();
    }
  }

  [TearDown]
  public virtual void TearDown()
  {
    TaskTable = null;
    RunTests  = false;
  }

  private readonly TaskOptions options_ = new(new Dictionary<string, string>
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

  /* Interface to test */
  protected ITaskTable? TaskTable;

  /* Boolean to control that tests are executed in
   * an instance of this class */
  protected bool RunTests;

  /* Function be override so it returns the suitable instance
   * of TaskTable to the corresponding interface implementation */
  public virtual void GetTaskTableInstance()
  {
  }

  [Test]
  public async Task InitShouldSucceed()
  {
    if (RunTests)
    {
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await TaskTable!.Check(HealthCheckTag.Liveness)
                                          .ConfigureAwait(false)).Status);
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await TaskTable.Check(HealthCheckTag.Readiness)
                                         .ConfigureAwait(false)).Status);
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await TaskTable.Check(HealthCheckTag.Startup)
                                         .ConfigureAwait(false)).Status);

      await TaskTable.Init(CancellationToken.None)
                     .ConfigureAwait(false);

      Assert.AreEqual(HealthStatus.Healthy,
                      (await TaskTable.Check(HealthCheckTag.Liveness)
                                      .ConfigureAwait(false)).Status);
      Assert.AreEqual(HealthStatus.Healthy,
                      (await TaskTable.Check(HealthCheckTag.Readiness)
                                      .ConfigureAwait(false)).Status);
      Assert.AreEqual(HealthStatus.Healthy,
                      (await TaskTable.Check(HealthCheckTag.Startup)
                                      .ConfigureAwait(false)).Status);
    }
  }

  [Test]
  public async Task ReadTaskAsyncShouldSucceed()
  {
    if (RunTests)
    {
      var result = await TaskTable!.ReadTaskAsync("TaskCompletedId",
                                                  CancellationToken.None)
                                   .ConfigureAwait(false);

      Assert.AreEqual("TaskCompletedId",
                      result.TaskId);
    }
  }

  [Test]
  public async Task OptionsAreEqual()
  {
    if (RunTests)
    {
      var result = await TaskTable!.ReadTaskAsync("TaskCompletedId",
                                                  CancellationToken.None)
                                   .ConfigureAwait(false);

      Assert.AreEqual(options_.Options,
                      result.Options.Options);

      var optDic = new Dictionary<string, string>();
      Assert.AreEqual(options_ with
                      {
                        Options = optDic,
                      },
                      result.Options with
                      {
                        Options = optDic,
                      });
    }
  }

  [Test]
  public void ReadTaskAsyncShouldFail()
  {
    if (RunTests)
    {
      Assert.ThrowsAsync<TaskNotFoundException>(async () => await TaskTable!.ReadTaskAsync("TaskDoNotExists",
                                                                                           CancellationToken.None)
                                                                            .ConfigureAwait(false));
    }
  }


  [Test]
  public async Task UpdateTaskStatusAsyncShouldSucceed()
  {
    if (RunTests)
    {
      await TaskTable!.UpdateTaskStatusAsync("TaskProcessingId",
                                             TaskStatus.Processed,
                                             CancellationToken.None)
                      .ConfigureAwait(false);
      var result = await TaskTable.GetTaskStatus(new[]
                                                 {
                                                   "TaskProcessingId",
                                                 },
                                                 CancellationToken.None)
                                  .ConfigureAwait(false);

      Assert.IsTrue(result.Single()
                          .Status == TaskStatus.Processed);
    }
  }

  [Test(Description = "Forbidden update: Task on final status")]
  public void UpdateTaskStatusAsyncShouldFail()
  {
    if (RunTests)
    {
      Assert.ThrowsAsync<ArmoniKException>(async () =>
                                           {
                                             await TaskTable!.UpdateTaskStatusAsync("TaskCompletedId",
                                                                                    TaskStatus.Unspecified,
                                                                                    CancellationToken.None)
                                                             .ConfigureAwait(false);
                                           });
    }
  }

  [Test]
  public async Task UpdateAllTaskStatusAsyncShouldSucceed()
  {
    if (RunTests)
    {
      var testFilter = new TaskFilter
                       {
                         Included = new TaskFilter.Types.StatusesRequest
                                    {
                                      Statuses =
                                      {
                                        TaskStatus.Creating,
                                        TaskStatus.Processing,
                                      },
                                    },
                         Session = new TaskFilter.Types.IdsRequest
                                   {
                                     Ids =
                                     {
                                       "SessionId", /* Task with TaskStatus.Processing was given this Id,
                            * for the Memory interface, adding it here is necessary for the test
                            * to succeed. For the MongoDB interface it may be ignored.
                            * TODO: Check filter definitions */
                                     },
                                   },
                       };
      await TaskTable!.UpdateAllTaskStatusAsync(testFilter,
                                                TaskStatus.Timeout,
                                                CancellationToken.None)
                      .ConfigureAwait(false);
      var resCreating = await TaskTable.GetTaskStatus(new[]
                                                      {
                                                        "TaskCreatingId",
                                                      },
                                                      CancellationToken.None)
                                       .ConfigureAwait(false);
      var resProcessing = await TaskTable.GetTaskStatus(new[]
                                                        {
                                                          "TaskProcessingId",
                                                        },
                                                        CancellationToken.None)
                                         .ConfigureAwait(false);

      Assert.IsTrue(resCreating.Single()
                               .Status == TaskStatus.Timeout && resProcessing.Single()
                                                                             .Status == TaskStatus.Timeout);
    }
  }

  [Test]
  public async Task UpdateAllTaskStatusAsyncShouldSucceedIfNoStatusGiven()
  {
    if (RunTests)
    {
      var testFilter = new TaskFilter
                       {
                         Task = new TaskFilter.Types.IdsRequest
                                {
                                  Ids =
                                  {
                                    "TaskProcessingId",
                                    "TaskCreatingId",
                                  },
                                },
                       };
      await TaskTable!.UpdateAllTaskStatusAsync(testFilter,
                                                TaskStatus.Timeout,
                                                CancellationToken.None)
                      .ConfigureAwait(false);
      var resCreating = await TaskTable.GetTaskStatus(new[]
                                                      {
                                                        "TaskCreatingId",
                                                      },
                                                      CancellationToken.None)
                                       .ConfigureAwait(false);
      var resProcessing = await TaskTable.GetTaskStatus(new[]
                                                        {
                                                          "TaskProcessingId",
                                                        },
                                                        CancellationToken.None)
                                         .ConfigureAwait(false);

      Assert.IsTrue(resCreating.Single()
                               .Status == TaskStatus.Timeout && resProcessing.Single()
                                                                             .Status == TaskStatus.Timeout);
    }
  }

  [Test]
  public async Task CancelTasksShouldSucceed()
  {
    if (RunTests)
    {
      var testFilter = new TaskFilter
                       {
                         Task = new TaskFilter.Types.IdsRequest
                                {
                                  Ids =
                                  {
                                    "TaskProcessingId",
                                    "TaskCreatingId",
                                  },
                                },
                       };
      await TaskTable!.CancelTasks(testFilter,
                                   CancellationToken.None)
                      .ConfigureAwait(false);
      var resCreating = await TaskTable!.GetTaskStatus(new[]
                                                       {
                                                         "TaskCreatingId",
                                                       },
                                                       CancellationToken.None)
                                        .ConfigureAwait(false);
      var resProcessing = await TaskTable.GetTaskStatus(new[]
                                                        {
                                                          "TaskProcessingId",
                                                        },
                                                        CancellationToken.None)
                                         .ConfigureAwait(false);

      Assert.AreEqual(TaskStatus.Cancelling,
                      resCreating.Single()
                                 .Status);
      Assert.AreEqual(TaskStatus.Cancelling,
                      resProcessing.Single()
                                   .Status);

      var resAnotherProcessing = await TaskTable.GetTaskStatus(new[]
                                                               {
                                                                 "TaskAnotherProcessingId",
                                                               },
                                                               CancellationToken.None)
                                                .ConfigureAwait(false);

      Assert.AreNotEqual(TaskStatus.Cancelling,
                         resAnotherProcessing.Single()
                                             .Status);
    }
  }

  [Test(Description = "Forbidden update: A given Task its on a final status")]
  [TestCase(TaskStatus.Error)]
  [TestCase(TaskStatus.Cancelled)]
  [TestCase(TaskStatus.Completed)]
  public void UpdateAllTaskStatusAsyncShouldFailOnSomeStatus(TaskStatus status)
  {
    if (RunTests)
    {
      var testFilter = new TaskFilter
                       {
                         Included = new TaskFilter.Types.StatusesRequest
                                    {
                                      Statuses =
                                      {
                                        status, // Presence of this status should generate an exception
                                      },
                                    },
                         Session = new TaskFilter.Types.IdsRequest
                                   {
                                     Ids =
                                     {
                                       "SessionId",
                                     },
                                   },
                       };
      Assert.ThrowsAsync<ArmoniKException>(async () =>
                                           {
                                             await TaskTable!.UpdateAllTaskStatusAsync(testFilter,
                                                                                       TaskStatus.Timeout,
                                                                                       CancellationToken.None)
                                                             .ConfigureAwait(false);
                                           });
    }
  }

  [Test]
  public async Task IsTaskCanceledShouldSucceed()
  {
    if (RunTests)
    {
      var result = await TaskTable!.IsTaskCancelledAsync("TaskCreatingId",
                                                         CancellationToken.None)
                                   .ConfigureAwait(false);

      Assert.IsFalse(result);
    }
  }

  [Test]
  public void IsTaskCanceledShouldFail()
  {
    if (RunTests)
    {
      Assert.ThrowsAsync<TaskNotFoundException>(async () =>
                                                {
                                                  await TaskTable!.IsTaskCancelledAsync("TaskDoesNotExist",
                                                                                        CancellationToken.None)
                                                                  .ConfigureAwait(false);
                                                });
    }
  }

  [Test]
  public async Task CancelSessionAsyncShouldSucceed()
  {
    if (RunTests)
    {
      var result = TaskTable!.CancelSessionAsync("SessionId",
                                                 CancellationToken.None);
      await result.ConfigureAwait(false);

      Assert.IsTrue(result.IsCompletedSuccessfully);
    }
  }

  [Test]
  public void CancelSessionAsyncShouldFail()
  {
    if (RunTests)
    {
      Assert.ThrowsAsync<SessionNotFoundException>(async () =>
                                                   {
                                                     await TaskTable!.CancelSessionAsync("NonExistingSessionId",
                                                                                         CancellationToken.None)
                                                                     .ConfigureAwait(false);
                                                   });
    }
  }

  [Test]
  public async Task CountPartitionTasksAsyncShouldSucceed()
  {
    if (RunTests)
    {
      var result = (await TaskTable!.CountPartitionTasksAsync(CancellationToken.None)
                                    .ConfigureAwait(false)).OrderBy(r => r.Status)
                                                           .ThenBy(r => r.PartitionId)
                                                           .ToIList();

      Assert.AreEqual(5,
                      result.Count);
      Assert.AreEqual(new PartitionTaskStatusCount("part1",
                                                   TaskStatus.Creating,
                                                   1),
                      result[0]);
      Assert.AreEqual(new PartitionTaskStatusCount("part2",
                                                   TaskStatus.Submitted,
                                                   1),
                      result[1]);
      Assert.AreEqual(new PartitionTaskStatusCount("part1",
                                                   TaskStatus.Completed,
                                                   1),
                      result[2]);
      Assert.AreEqual(new PartitionTaskStatusCount("part1",
                                                   TaskStatus.Error,
                                                   1),
                      result[3]);
      Assert.AreEqual(new PartitionTaskStatusCount("part1",
                                                   TaskStatus.Processing,
                                                   2),
                      result[4]);
    }
  }

  [Test]
  public async Task CountTasksAsyncShouldSucceed()
  {
    if (RunTests)
    {
      var testFilter = new TaskFilter
                       {
                         Included = new TaskFilter.Types.StatusesRequest
                                    {
                                      Statuses =
                                      {
                                        TaskStatus.Completed,
                                        TaskStatus.Creating,
                                      },
                                    },
                         Session = new TaskFilter.Types.IdsRequest
                                   {
                                     Ids =
                                     {
                                       "SessionId",
                                     },
                                   },
                       };

      var result = await TaskTable!.CountTasksAsync(testFilter,
                                                    CancellationToken.None)
                                   .ConfigureAwait(false);

      foreach (var taskStatusCount in result)
      {
        Console.WriteLine(taskStatusCount);
        Assert.AreEqual(1,
                        taskStatusCount.Count);
      }
    }
  }

  [Test]
  public async Task CountAllTasksAsyncShouldSucceed()
  {
    if (RunTests)
    {
      var result = await TaskTable!.CountAllTasksAsync(TaskStatus.Processing,
                                                       CancellationToken.None)
                                   .ConfigureAwait(false);
      Assert.IsTrue(result == 2);
    }
  }

  [Test]
  public async Task SetTaskSuccessAsyncShouldSucceed()
  {
    if (RunTests)
    {
      var result = TaskTable!.SetTaskSuccessAsync("TaskProcessingId",
                                                  CancellationToken.None);
      await result.ConfigureAwait(false);

      var resStatus = await TaskTable.GetTaskStatus(new[]
                                                    {
                                                      "TaskProcessingId",
                                                    },
                                                    CancellationToken.None)
                                     .ConfigureAwait(false);

      Assert.IsTrue(result.IsCompletedSuccessfully && resStatus.Single()
                                                               .Status == TaskStatus.Completed);
    }
  }

  [Test]
  public async Task SetTaskErrorAsyncShouldSucceed()
  {
    if (RunTests)
    {
      var result = TaskTable!.SetTaskErrorAsync("TaskProcessingId",
                                                "Testing SetTaskError",
                                                CancellationToken.None);
      await result.ConfigureAwait(false);

      var resStatus = await TaskTable.GetTaskStatus(new[]
                                                    {
                                                      "TaskProcessingId",
                                                    },
                                                    CancellationToken.None)
                                     .ConfigureAwait(false);

      Assert.AreEqual(TaskStatus.Error,
                      resStatus.Single()
                               .Status);

      var output = await TaskTable.GetTaskOutput("TaskProcessingId",
                                                 CancellationToken.None)
                                  .ConfigureAwait(false);

      Assert.AreEqual("Testing SetTaskError",
                      output.Error);
    }
  }

  [Test]
  public async Task SetTaskCanceledAsyncShouldSucceed()
  {
    if (RunTests)
    {
      var result = TaskTable!.SetTaskCanceledAsync("TaskProcessingId",
                                                   CancellationToken.None);
      await result.ConfigureAwait(false);

      var resStatus = await TaskTable.GetTaskStatus(new[]
                                                    {
                                                      "TaskProcessingId",
                                                    },
                                                    CancellationToken.None)
                                     .ConfigureAwait(false);

      Assert.IsTrue(result.IsCompletedSuccessfully && resStatus.Single()
                                                               .Status == TaskStatus.Cancelled);
    }
  }

  [Test]
  public async Task GetTaskOutputShouldSucceed()
  {
    if (RunTests)
    {
      var expectedOutput = new Output(true,
                                      "");
      var result = await TaskTable!.GetTaskOutput("TaskCompletedId",
                                                  CancellationToken.None)
                                   .ConfigureAwait(false);

      Assert.AreEqual(expectedOutput,
                      result);
    }
  }

  [Test]
  public void GetTaskOutputShouldFail()
  {
    if (RunTests)
    {
      Assert.ThrowsAsync<TaskNotFoundException>(async () =>
                                                {
                                                  await TaskTable!.GetTaskOutput("NonExistingTaskId",
                                                                                 CancellationToken.None)
                                                                  .ConfigureAwait(false);
                                                });
    }
  }

  [Test]
  public async Task GetTaskExpectedOutputKeysShouldSucceed()
  {
    if (RunTests)
    {
      var expectedOutput = new[]
                           {
                             "output1",
                             "output2",
                           };
      var result = await TaskTable!.GetTaskExpectedOutputKeys("TaskCompletedId",
                                                              CancellationToken.None)
                                   .ConfigureAwait(false);

      Assert.AreEqual(expectedOutput,
                      result.ToArray());
    }
  }

  [Test]
  public void GetTaskExpectedOutputKeysShouldFail()
  {
    if (RunTests)
    {
      Assert.ThrowsAsync<TaskNotFoundException>(async () =>
                                                {
                                                  await TaskTable!.GetTaskExpectedOutputKeys("NonExistingTaskId",
                                                                                             CancellationToken.None)
                                                                  .ConfigureAwait(false);
                                                });
    }
  }

  [Test]
  public async Task GetParentTaskIdsShouldSucceed()
  {
    if (RunTests)
    {
      var parentTaskIds = new[]
                          {
                            "parent1",
                          };
      var result = await TaskTable!.GetParentTaskIds("TaskCompletedId",
                                                     CancellationToken.None)
                                   .ConfigureAwait(false);

      Assert.AreEqual(parentTaskIds,
                      result.ToArray());
    }
  }

  [Test]
  public void GetParentTaskIdsShouldFail()
  {
    if (RunTests)
    {
      Assert.ThrowsAsync<TaskNotFoundException>(async () =>
                                                {
                                                  await TaskTable!.GetParentTaskIds("NonExistingTaskId",
                                                                                    CancellationToken.None)
                                                                  .ConfigureAwait(false);
                                                });
    }
  }

  [Test]
  public void GetExpectedOutputKeysShouldSucceed()
  {
    if (RunTests)
    {
      var parentTaskIds = new[]
                          {
                            "output1",
                            "output2",
                          };
      var result = TaskTable!.GetTasksExpectedOutputKeys(new[]
                                                         {
                                                           "TaskCompletedId",
                                                         },
                                                         CancellationToken.None)
                             .ToListAsync(CancellationToken.None)
                             .Result.Single()
                             .expectedOutputKeys;

      Assert.AreEqual(parentTaskIds,
                      result);
    }
  }

  [Test]
  public async Task AcquireTaskShouldSucceed()
  {
    if (RunTests)
    {
      var hostname = LocalIPv4.GetLocalIPv4Ethernet();

      var result = await TaskTable!.AcquireTask("TaskSubmittedId",
                                                hostname,
                                                CancellationToken.None)
                                   .ConfigureAwait(false);

      Assert.AreEqual("TaskSubmittedId",
                      result.TaskId);
      Assert.AreEqual(hostname,
                      result.OwnerPodId);
    }
  }

  [Test]
  public async Task AcquireAcquiredTaskShouldReturnSame()
  {
    if (RunTests)
    {
      var hostname = LocalIPv4.GetLocalIPv4Ethernet();

      var result1 = await TaskTable!.AcquireTask("TaskSubmittedId",
                                                 hostname,
                                                 CancellationToken.None)
                                    .ConfigureAwait(false);

      Assert.AreEqual("TaskSubmittedId",
                      result1.TaskId);
      Assert.AreEqual(hostname,
                      result1.OwnerPodId);

      var result2 = await TaskTable.AcquireTask("TaskSubmittedId",
                                                hostname,
                                                CancellationToken.None)
                                   .ConfigureAwait(false);
      Assert.AreEqual(result1.Status,
                      result2.Status);

      Assert.AreEqual(result1.OwnerPodId,
                      result2.OwnerPodId);
    }
  }

  [Test]
  public async Task AcquireTaskShouldFail()
  {
    if (RunTests)
    {
      var result = await TaskTable!.AcquireTask("TaskFailedId",
                                                LocalIPv4.GetLocalIPv4Ethernet(),
                                                CancellationToken.None)
                                   .ConfigureAwait(false);

      Assert.AreNotEqual(LocalIPv4.GetLocalIPv4Ethernet(),
                         result.OwnerPodId);
    }
  }

  [Test]
  public async Task FinalizeTaskCreationShouldSucceed()
  {
    if (RunTests)
    {
      var result = await TaskTable!.FinalizeTaskCreation(new List<string>
                                                         {
                                                           "TaskCreatingId",
                                                         },
                                                         CancellationToken.None)
                                   .ConfigureAwait(false);

      Assert.AreEqual(1,
                      result);
    }
  }

  [Test]
  public async Task FinalizeTaskCreationShouldFail()
  {
    if (RunTests)
    {
      var result = await TaskTable!.FinalizeTaskCreation(new List<string>
                                                         {
                                                           "TaskFailedId",
                                                         },
                                                         CancellationToken.None)
                                   .ConfigureAwait(false);

      Assert.AreNotEqual(1,
                         result);
    }
  }

  [Test]
  public async Task StartTaskShouldSucceed()
  {
    if (RunTests)
    {
      await TaskTable!.StartTask("TaskSubmittedId",
                                 CancellationToken.None)
                      .ConfigureAwait(false);

      var taskData = await TaskTable.ReadTaskAsync("TaskSubmittedId",
                                                   CancellationToken.None)
                                    .ConfigureAwait(false);

      Assert.AreEqual(TaskStatus.Processing,
                      taskData.Status);
    }
  }

  [Test]
  public void StartTaskShouldFail()
  {
    if (RunTests)
    {
      Assert.ThrowsAsync<TaskNotFoundException>(async () =>
                                                {
                                                  await TaskTable!.StartTask("NonExistingTaskId",
                                                                             CancellationToken.None)
                                                                  .ConfigureAwait(false);
                                                });
    }
  }

  [Test]
  public void DeleteTaskShouldFail()
  {
    if (RunTests)
    {
      Assert.ThrowsAsync<TaskNotFoundException>(async () =>
                                                {
                                                  await TaskTable!.DeleteTaskAsync("NonExistingTaskId",
                                                                                   CancellationToken.None)
                                                                  .ConfigureAwait(false);
                                                });
    }
  }

  [Test]
  public async Task DeleteTaskShouldSucceed()
  {
    if (RunTests)
    {
      await TaskTable!.DeleteTaskAsync("TaskSubmittedId",
                                       CancellationToken.None)
                      .ConfigureAwait(false);

      Assert.ThrowsAsync<TaskNotFoundException>(async () =>
                                                {
                                                  await TaskTable.ReadTaskAsync("TaskSubmittedId",
                                                                                CancellationToken.None)
                                                                 .ConfigureAwait(false);
                                                });
    }
  }

  [Test]
  public async Task ListTaskShouldSucceed()
  {
    if (RunTests)
    {
      var taskList = await TaskTable!.ListTasksAsync(new TaskFilter
                                                     {
                                                       Included = new TaskFilter.Types.StatusesRequest
                                                                  {
                                                                    Statuses =
                                                                    {
                                                                      TaskStatus.Completed,
                                                                    },
                                                                  },
                                                       Session = new TaskFilter.Types.IdsRequest
                                                                 {
                                                                   Ids =
                                                                   {
                                                                     "SessionId",
                                                                   },
                                                                 },
                                                     },
                                                     CancellationToken.None)
                                     .ToListAsync()
                                     .ConfigureAwait(false);

      Assert.AreEqual(1,
                      taskList.Count);
    }
  }

  [Test]
  public async Task RetryTaskShouldSucceed()
  {
    if (RunTests)
    {
      var taskToRetry = await TaskTable!.ReadTaskAsync("TaskFailedId",
                                                       CancellationToken.None)
                                        .ConfigureAwait(false);

      var expectedNewId = taskToRetry.InitialTaskId + $"###{taskToRetry.RetryOfIds.Count + 1}";

      var newTaskId = await TaskTable.RetryTask(taskToRetry,
                                                CancellationToken.None)
                                     .ConfigureAwait(false);

      Assert.AreEqual(expectedNewId,
                      newTaskId);
    }
  }

  [Test]
  public async Task PayloadIdAfterRetryShouldBeCorrect()
  {
    if (RunTests)
    {
      var taskToRetry = await TaskTable!.ReadTaskAsync("TaskFailedId",
                                                       CancellationToken.None)
                                        .ConfigureAwait(false);
      for (var i = 0; i < 3; i++)
      {
        var newTaskId = await TaskTable.RetryTask(taskToRetry,
                                                  CancellationToken.None)
                                       .ConfigureAwait(false);

        var retriedTask = await TaskTable.ReadTaskAsync(newTaskId,
                                                        CancellationToken.None)
                                         .ConfigureAwait(false);

        Assert.AreEqual(taskToRetry.PayloadId,
                        retriedTask.PayloadId);

        taskToRetry = retriedTask;
      }
    }
  }

  [Test]
  public async Task ListApplicationFromTasksShouldSucceed()
  {
    if (RunTests)
    {
      var req = new ListApplicationsRequest
                {
                  Page     = 0,
                  PageSize = 2,
                  Filter = new ListApplicationsRequest.Types.Filter
                           {
                             Name = options_.ApplicationName,
                           },
                  Sort = new ListApplicationsRequest.Types.Sort
                         {
                           Direction = ListApplicationsRequest.Types.OrderDirection.Desc,
                           Field     = ListApplicationsRequest.Types.OrderByField.Name,
                         },
                };

      var validator = new ListApplicationsRequestValidator();
      Assert.IsTrue(validator.Validate(req)
                             .IsValid);

      var listTasks = await TaskTable!.ListTasksAsync(req,
                                                      CancellationToken.None)
                                      .ConfigureAwait(false);

      var listTasksResponseTaskData = listTasks.ToList();
      foreach (var task in listTasksResponseTaskData)
      {
        Console.WriteLine(task);
      }

      Assert.AreEqual(2,
                      listTasksResponseTaskData.Count());
    }
  }

  [Test]
  public async Task ListTaskWithRequestShouldSucceed()
  {
    if (RunTests)
    {
      var req = new ListTasksRequest
                {
                  Page     = 0,
                  PageSize = 20,
                  Filter = new ListTasksRequest.Types.Filter
                           {
                             SessionId = "SessionId",
                           },
                  Sort = new ListTasksRequest.Types.Sort
                         {
                           Direction = ListTasksRequest.Types.OrderDirection.Desc,
                           Field     = ListTasksRequest.Types.OrderByField.SessionId,
                         },
                };

      var validator = new ListTasksRequestValidator();
      Assert.IsTrue(validator.Validate(req)
                             .IsValid);

      var listTasks = await TaskTable!.ListTasksAsync(req,
                                                      CancellationToken.None)
                                      .ConfigureAwait(false);

      Assert.AreEqual(6,
                      listTasks.Count());
    }
  }

  [Test]
  public async Task ListTaskWithRequestLimit2ShouldSucceed()
  {
    if (RunTests)
    {
      var req = new ListTasksRequest
                {
                  Page     = 0,
                  PageSize = 2,
                  Filter = new ListTasksRequest.Types.Filter
                           {
                             SessionId = "SessionId",
                           },
                  Sort = new ListTasksRequest.Types.Sort
                         {
                           Direction = ListTasksRequest.Types.OrderDirection.Desc,
                           Field     = ListTasksRequest.Types.OrderByField.SessionId,
                         },
                };

      var validator = new ListTasksRequestValidator();
      Assert.IsTrue(validator.Validate(req)
                             .IsValid);

      var listTasks = await TaskTable!.ListTasksAsync(req,
                                                      CancellationToken.None)
                                      .ConfigureAwait(false);

      Assert.AreEqual(2,
                      listTasks.Count());
    }
  }

  [Test]
  public async Task ListTaskWithRequestTaskStatusShouldSucceed()
  {
    if (RunTests)
    {
      var req = new ListTasksRequest
                {
                  Page     = 0,
                  PageSize = 2,
                  Filter = new ListTasksRequest.Types.Filter
                           {
                             SessionId = "SessionId",
                             Status    = TaskStatus.Completed,
                           },
                  Sort = new ListTasksRequest.Types.Sort
                         {
                           Direction = ListTasksRequest.Types.OrderDirection.Desc,
                           Field     = ListTasksRequest.Types.OrderByField.SessionId,
                         },
                };

      var validator = new ListTasksRequestValidator();
      Assert.IsTrue(validator.Validate(req)
                             .IsValid);

      var listTasks = await TaskTable!.ListTasksAsync(req,
                                                      CancellationToken.None)
                                      .ConfigureAwait(false);

      var listTasksResponseTaskData = listTasks.ToList();
      foreach (var task in listTasksResponseTaskData)
      {
        Console.WriteLine(task);
      }

      Assert.AreEqual(1,
                      listTasksResponseTaskData.Count());
    }
  }

  [Test]
  public async Task ListTaskWithRequestCreatedAfterShouldSucceed()
  {
    if (RunTests)
    {
      var req = new ListTasksRequest
                {
                  Page     = 0,
                  PageSize = 2,
                  Filter = new ListTasksRequest.Types.Filter
                           {
                             SessionId    = "SessionId",
                             CreatedAfter = FromDateTime(DateTime.UtcNow - TimeSpan.FromMinutes(1)),
                           },
                  Sort = new ListTasksRequest.Types.Sort
                         {
                           Direction = ListTasksRequest.Types.OrderDirection.Desc,
                           Field     = ListTasksRequest.Types.OrderByField.SessionId,
                         },
                };

      var validator = new ListTasksRequestValidator();
      Assert.IsTrue(validator.Validate(req)
                             .IsValid);

      var listTasks = await TaskTable!.ListTasksAsync(req,
                                                      CancellationToken.None)
                                      .ConfigureAwait(false);

      var listTasksResponseTaskData = listTasks.ToList();
      foreach (var task in listTasksResponseTaskData)
      {
        Console.WriteLine(task);
      }

      Assert.AreEqual(2,
                      listTasksResponseTaskData.Count());
    }
  }


  [Test]
  public async Task ListTaskWithRequestCreatedBeforeShouldSucceed()
  {
    if (RunTests)
    {
      var req = new ListTasksRequest
                {
                  Page     = 0,
                  PageSize = 2,
                  Filter = new ListTasksRequest.Types.Filter
                           {
                             SessionId     = "SessionId",
                             CreatedBefore = FromDateTime(DateTime.UtcNow),
                           },
                  Sort = new ListTasksRequest.Types.Sort
                         {
                           Direction = ListTasksRequest.Types.OrderDirection.Desc,
                           Field     = ListTasksRequest.Types.OrderByField.SessionId,
                         },
                };

      var validator = new ListTasksRequestValidator();
      Assert.IsTrue(validator.Validate(req)
                             .IsValid);

      var listTasks = await TaskTable!.ListTasksAsync(req,
                                                      CancellationToken.None)
                                      .ConfigureAwait(false);

      var listTasksResponseTaskData = listTasks.ToList();
      foreach (var task in listTasksResponseTaskData)
      {
        Console.WriteLine(task);
      }

      Assert.AreEqual(2,
                      listTasksResponseTaskData.Count());
    }
  }

  [Test]
  public async Task ListTaskWithRequestCreatedBeforeSortAscTaskIdShouldSucceed()
  {
    if (RunTests)
    {
      var req = new ListTasksRequest
                {
                  Page     = 0,
                  PageSize = 2,
                  Filter = new ListTasksRequest.Types.Filter
                           {
                             SessionId     = "SessionId",
                             CreatedBefore = FromDateTime(DateTime.UtcNow),
                           },
                  Sort = new ListTasksRequest.Types.Sort
                         {
                           Direction = ListTasksRequest.Types.OrderDirection.Asc,
                           Field     = ListTasksRequest.Types.OrderByField.TaskId,
                         },
                };

      var validator = new ListTasksRequestValidator();
      Assert.IsTrue(validator.Validate(req)
                             .IsValid);

      var listTasks = await TaskTable!.ListTasksAsync(req,
                                                      CancellationToken.None)
                                      .ConfigureAwait(false);

      var listTasksResponseTaskData = listTasks.ToList();
      foreach (var task in listTasksResponseTaskData)
      {
        Console.WriteLine(task);
      }

      Assert.AreEqual(2,
                      listTasksResponseTaskData.Count);
      Assert.AreEqual("TaskAnotherProcessingId",
                      listTasksResponseTaskData[0]
                        .TaskId);
      Assert.AreEqual("TaskCompletedId",
                      listTasksResponseTaskData[1]
                        .TaskId);
    }
  }

  [Test]
  public async Task ListTaskWithRequestCreatedBeforeSortAscStatusShouldSucceed()
  {
    if (RunTests)
    {
      var req = new ListTasksRequest
                {
                  Page     = 0,
                  PageSize = 2,
                  Filter = new ListTasksRequest.Types.Filter
                           {
                             SessionId     = "SessionId",
                             CreatedBefore = FromDateTime(DateTime.UtcNow),
                           },
                  Sort = new ListTasksRequest.Types.Sort
                         {
                           Direction = ListTasksRequest.Types.OrderDirection.Asc,
                           Field     = ListTasksRequest.Types.OrderByField.Status,
                         },
                };

      var validator = new ListTasksRequestValidator();
      Assert.IsTrue(validator.Validate(req)
                             .IsValid);

      var listTasks = await TaskTable!.ListTasksAsync(req,
                                                      CancellationToken.None)
                                      .ConfigureAwait(false);

      var listTasksResponseTaskData = listTasks.ToList();
      foreach (var task in listTasksResponseTaskData)
      {
        Console.WriteLine(task);
      }

      Assert.AreEqual(2,
                      listTasksResponseTaskData.Count);
      Assert.AreEqual(TaskStatus.Creating,
                      listTasksResponseTaskData[0]
                        .Status);
      Assert.AreEqual(TaskStatus.Submitted,
                      listTasksResponseTaskData[1]
                        .Status);
    }
  }

  [Test]
  public async Task ListTaskWithRequestCreatedBeforeSortDescStatusShouldSucceed()
  {
    if (RunTests)
    {
      var req = new ListTasksRequest
                {
                  Page     = 0,
                  PageSize = 2,
                  Filter = new ListTasksRequest.Types.Filter
                           {
                             SessionId     = "SessionId",
                             CreatedBefore = FromDateTime(DateTime.UtcNow),
                           },
                  Sort = new ListTasksRequest.Types.Sort
                         {
                           Direction = ListTasksRequest.Types.OrderDirection.Desc,
                           Field     = ListTasksRequest.Types.OrderByField.Status,
                         },
                };

      var validator = new ListTasksRequestValidator();
      Assert.IsTrue(validator.Validate(req)
                             .IsValid);

      var listTasks = await TaskTable!.ListTasksAsync(req,
                                                      CancellationToken.None)
                                      .ConfigureAwait(false);

      var listTasksResponseTaskData = listTasks.ToList();
      foreach (var task in listTasksResponseTaskData)
      {
        Console.WriteLine(task);
      }

      Assert.AreEqual(2,
                      listTasksResponseTaskData.Count);
      Assert.AreEqual(TaskStatus.Processing,
                      listTasksResponseTaskData[0]
                        .Status);
      Assert.AreEqual(TaskStatus.Processing,
                      listTasksResponseTaskData[1]
                        .Status);
    }
  }

  [Test]
  public async Task CancelTasksAsyncShouldSucceed()
  {
    if (RunTests)
    {
      var req = new ListTasksRequest
                {
                  Page     = 0,
                  PageSize = 200,
                  Filter = new ListTasksRequest.Types.Filter
                           {
                             SessionId = "SessionId",
                           },
                  Sort = new ListTasksRequest.Types.Sort
                         {
                           Direction = ListTasksRequest.Types.OrderDirection.Asc,
                           Field     = ListTasksRequest.Types.OrderByField.Status,
                         },
                };

      var validator = new ListTasksRequestValidator();
      Assert.IsTrue(validator.Validate(req)
                             .IsValid);

      var listTasks = await TaskTable!.ListTasksAsync(req,
                                                      CancellationToken.None)
                                      .ConfigureAwait(false);

      var cancelledTasks = await TaskTable.CancelTaskAsync(listTasks.Select(data => data.TaskId)
                                                                    .ToList(),
                                                           CancellationToken.None)
                                          .ConfigureAwait(false);

      var listTasksResponseTaskData = listTasks.ToList();
      Assert.AreEqual(cancelledTasks.Count,
                      listTasksResponseTaskData.Count);

      foreach (var task in cancelledTasks)
      {
        Console.WriteLine(task);
        Assert.IsTrue(task.Status is TaskStatus.Completed or TaskStatus.Cancelled or TaskStatus.Cancelling or TaskStatus.Error);
      }
    }
  }

  [TestCase(TaskStatus.Error)]
  [TestCase(TaskStatus.Completed)]
  [TestCase(TaskStatus.Cancelled)]
  [TestCase(TaskStatus.Cancelling)]
  public async Task CancelTasksAsyncShouldNotChangeTheGivenStatus(TaskStatus status)
  {
    if (RunTests)
    {
      var taskId = "TaskToBeCancelled";

      await TaskTable!.CreateTasks(new[]
                                   {
                                     new TaskData("SessionId",
                                                  taskId,
                                                  "OwnerPodId",
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
                                                  status,
                                                  options_,
                                                  new Output(true,
                                                             "")),
                                   })
                      .ConfigureAwait(false);

      var cancelledTasks = await TaskTable.CancelTaskAsync(new List<string>
                                                           {
                                                             taskId,
                                                           },
                                                           CancellationToken.None)
                                          .ConfigureAwait(false);

      Console.WriteLine(cancelledTasks.Single());
      Assert.AreEqual(status,
                      cancelledTasks.Single()
                                    .Status);
      Assert.AreEqual(taskId,
                      cancelledTasks.Single()
                                    .TaskId);
    }
  }

  [Test]
  public async Task CancelTasksAsyncEmptyListShouldSucceed()
  {
    if (RunTests)
    {
      var cancelledTasks = await TaskTable!.CancelTaskAsync(new List<string>(),
                                                            CancellationToken.None)
                                           .ConfigureAwait(false);

      Assert.AreEqual(0,
                      cancelledTasks.Count);
    }
  }
}
