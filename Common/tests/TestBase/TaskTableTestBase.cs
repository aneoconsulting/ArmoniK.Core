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
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using Armonik.Api.gRPC.V1;

using ArmoniK.Api.gRPC.V1.Applications;

using Armonik.Api.Grpc.V1.SortDirection;

using ArmoniK.Api.gRPC.V1.Submitter;

using Armonik.Api.gRPC.V1.Tasks;

using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.gRPC.Validators;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.Helpers;
using ArmoniK.Core.Common.Tests.ListTasksRequestExt;
using ArmoniK.Core.Common.Utils;
using ArmoniK.Utils;

using Microsoft.Extensions.Diagnostics.HealthChecks;

using NUnit.Framework;

using FilterDate = Armonik.Api.gRPC.V1.Tasks.FilterDate;
using FilterField = ArmoniK.Api.gRPC.V1.Applications.FilterField;
using Filters = ArmoniK.Api.gRPC.V1.Applications.Filters;
using FiltersAnd = ArmoniK.Api.gRPC.V1.Applications.FiltersAnd;
using FilterString = ArmoniK.Api.gRPC.V1.Applications.FilterString;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Tests.TestBase;

[TestFixture]
public class TaskTableTestBase
{
  [SetUp]
  public async Task SetUp()
  {
    GetTaskTableInstance();

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
                                   taskProcessingData_,
                                   taskProcessingData2_,
                                   taskSubmittedData_,
                                   taskFailedData_,
                                   taskSession2_,
                                 })
                    .ConfigureAwait(false);
  }

  [TearDown]
  public virtual void TearDown()
  {
    TaskTable = null;
    RunTests  = false;
  }

  private readonly TaskData taskSubmittedData_ = new("SessionId",
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

  private readonly TaskData taskProcessingData_ = new("SessionId",
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

  private readonly TaskData taskSession2_ = new("SessionId2",
                                                "TaskFailedId_session2",
                                                "OwnerPodId",
                                                "OwnerPodName",
                                                "PayloadId",
                                                new[]
                                                {
                                                  "parent1",
                                                },
                                                new[]
                                                {
                                                  "dependency2",
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

  private static bool CheckForSkipSetup()
  {
    var category = TestContext.CurrentContext.Test.Properties.Get("Category") as string;
    return category is "SkipSetUp";
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
  [Category("SkipSetUp")]
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

      Assert.AreEqual(Options.Options,
                      result.Options.Options);

      var optDic = new Dictionary<string, string>();
      Assert.AreEqual(Options with
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
  public async Task CountPartitionTasksAsyncShouldSucceed()
  {
    if (RunTests)
    {
      var result = (await TaskTable!.CountPartitionTasksAsync(CancellationToken.None)
                                    .ConfigureAwait(false)).OrderBy(r => r.Status)
                                                           .ThenBy(r => r.PartitionId)
                                                           .AsIList();

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
                                                   2),
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

      result = await TaskTable.CountTasksAsync(data => data.SessionId == "SessionId" && (data.Status == TaskStatus.Completed || data.Status == TaskStatus.Creating),
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
      await TaskTable!.SetTaskSuccessAsync(taskProcessingData_ with
                                           {
                                             EndDate = DateTime.UtcNow,
                                             CreationToEndDuration = DateTime.UtcNow   - taskProcessingData_.EndDate,
                                             ProcessingToEndDuration = DateTime.UtcNow - taskProcessingData_.StartDate,
                                           },
                                           CancellationToken.None)
                      .ConfigureAwait(false);

      var resStatus = await TaskTable.GetTaskStatus(new[]
                                                    {
                                                      taskProcessingData_.TaskId,
                                                    },
                                                    CancellationToken.None)
                                     .ConfigureAwait(false);

      Assert.AreEqual(TaskStatus.Completed,
                      resStatus.Single()
                               .Status);

      var taskData = await TaskTable.ReadTaskAsync(taskProcessingData_.TaskId,
                                                   CancellationToken.None)
                                    .ConfigureAwait(false);

      Assert.AreEqual(TaskStatus.Completed,
                      taskData.Status);
      Assert.IsTrue(taskData.Output.Success);
      Assert.AreEqual("",
                      taskData.Output.Error);
    }
  }

  [Test]
  public async Task SetTaskErrorAsyncShouldSucceed()
  {
    if (RunTests)
    {
      await TaskTable!.SetTaskErrorAsync(taskProcessingData_ with
                                         {
                                           EndDate = DateTime.UtcNow,
                                           CreationToEndDuration = DateTime.UtcNow   - taskProcessingData_.EndDate,
                                           ProcessingToEndDuration = DateTime.UtcNow - taskProcessingData_.StartDate,
                                         },
                                         "Testing SetTaskError",
                                         CancellationToken.None)
                      .ConfigureAwait(false);

      var resStatus = await TaskTable!.GetTaskStatus(new[]
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
      Assert.IsFalse(output.Success);
    }
  }

  [Test]
  public async Task SetTaskCanceledAsyncShouldSucceed()
  {
    if (RunTests)
    {
      await TaskTable!.SetTaskCanceledAsync(taskProcessingData_ with
                                            {
                                              EndDate = DateTime.UtcNow,
                                              CreationToEndDuration = DateTime.UtcNow   - taskProcessingData_.EndDate,
                                              ProcessingToEndDuration = DateTime.UtcNow - taskProcessingData_.StartDate,
                                            },
                                            CancellationToken.None)
                      .ConfigureAwait(false);

      var resStatus = await TaskTable!.GetTaskStatus(new[]
                                                     {
                                                       "TaskProcessingId",
                                                     },
                                                     CancellationToken.None)
                                      .ConfigureAwait(false);

      Assert.AreEqual(TaskStatus.Cancelled,
                      resStatus.Single()
                               .Status);

      var output = await TaskTable.GetTaskOutput("TaskProcessingId",
                                                 CancellationToken.None)
                                  .ConfigureAwait(false);

      Assert.IsEmpty(output.Error);
      Assert.IsFalse(output.Success);
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
      var ownerpodid    = LocalIPv4.GetLocalIPv4Ethernet();
      var ownerpodname  = Dns.GetHostName();
      var receptionDate = DateTime.UtcNow.Date;

      var result = await TaskTable!.AcquireTask(taskSubmittedData_ with
                                                {
                                                  OwnerPodId = ownerpodid,
                                                  OwnerPodName = ownerpodname,
                                                  ReceptionDate = receptionDate,
                                                  AcquisitionDate = DateTime.UtcNow,
                                                },
                                                CancellationToken.None)
                                   .ConfigureAwait(false);

      Assert.AreEqual("TaskSubmittedId",
                      result.TaskId);
      Assert.AreEqual(ownerpodid,
                      result.OwnerPodId);
      Assert.AreEqual(ownerpodname,
                      result.OwnerPodName);
      Assert.AreEqual(receptionDate,
                      result.ReceptionDate);
      Assert.Greater(DateTime.UtcNow,
                     result.AcquisitionDate);
      Assert.Greater(result.AcquisitionDate,
                     result.ReceptionDate);
    }
  }

  [Test]
  public async Task ReleaseTaskShouldSucceed()
  {
    if (RunTests)
    {
      var ownerpodid    = LocalIPv4.GetLocalIPv4Ethernet();
      var ownerpodname  = Dns.GetHostName();
      var receptionDate = DateTime.UtcNow.Date;
      var taskSubmitted = taskSubmittedData_ with
                          {
                            OwnerPodId = ownerpodid,
                            OwnerPodName = ownerpodname,
                            ReceptionDate = receptionDate,
                            AcquisitionDate = DateTime.UtcNow,
                          };

      var result = await TaskTable!.AcquireTask(taskSubmitted,
                                                CancellationToken.None)
                                   .ConfigureAwait(false);

      Assert.AreEqual("TaskSubmittedId",
                      result.TaskId);
      Assert.AreEqual(ownerpodid,
                      result.OwnerPodId);
      Assert.AreEqual(ownerpodname,
                      result.OwnerPodName);
      Assert.AreEqual(receptionDate,
                      result.ReceptionDate);
      Assert.Greater(DateTime.UtcNow,
                     result.AcquisitionDate);
      Assert.Greater(result.AcquisitionDate,
                     result.ReceptionDate);

      var resultRelease = await TaskTable!.ReleaseTask(taskSubmitted,
                                                       CancellationToken.None)
                                          .ConfigureAwait(false);
      Assert.AreEqual("TaskSubmittedId",
                      resultRelease.TaskId);
      Assert.AreEqual("",
                      resultRelease.OwnerPodId);
      Assert.AreEqual("",
                      resultRelease.OwnerPodName);
      Assert.AreEqual(null,
                      resultRelease.ReceptionDate);
      Assert.AreEqual(null,
                      resultRelease.AcquisitionDate);
    }
  }

  [Test]
  public async Task AcquireAcquiredTaskShouldReturnSame()
  {
    if (RunTests)
    {
      var hostname = LocalIPv4.GetLocalIPv4Ethernet();

      var taskSubmitted = taskSubmittedData_ with
                          {
                            OwnerPodId = hostname,
                            OwnerPodName = hostname,
                            ReceptionDate = DateTime.UtcNow,
                            AcquisitionDate = DateTime.UtcNow,
                          };

      var result1 = await TaskTable!.AcquireTask(taskSubmitted,
                                                 CancellationToken.None)
                                    .ConfigureAwait(false);

      Assert.AreEqual("TaskSubmittedId",
                      result1.TaskId);
      Assert.AreEqual(hostname,
                      result1.OwnerPodId);

      var result2 = await TaskTable.AcquireTask(taskSubmitted,
                                                CancellationToken.None)
                                   .ConfigureAwait(false);
      Assert.AreEqual(result1.Status,
                      result2.Status);

      Assert.AreEqual(result1.OwnerPodId,
                      result2.OwnerPodId);

      Assert.AreEqual(result1.OwnerPodName,
                      result2.OwnerPodName);

      Assert.AreEqual(result1.ReceptionDate,
                      result2.ReceptionDate);

      Assert.AreEqual(result1.AcquisitionDate,
                      result2.AcquisitionDate);
    }
  }

  [Test]
  public async Task AcquireTaskShouldFail()
  {
    if (RunTests)
    {
      var task = taskFailedData_ with
                 {
                   OwnerPodId = LocalIPv4.GetLocalIPv4Ethernet(),
                   OwnerPodName = Dns.GetHostName(),
                   ReceptionDate = DateTime.UtcNow,
                   AcquisitionDate = DateTime.UtcNow,
                 };

      var result = await TaskTable!.AcquireTask(task,
                                                CancellationToken.None)
                                   .ConfigureAwait(false);

      Assert.AreNotEqual(LocalIPv4.GetLocalIPv4Ethernet(),
                         result.OwnerPodId);

      Assert.AreEqual(TaskStatus.Error,
                      result.Status);
    }
  }

  [Test]
  public async Task AcquireCreatingTaskShouldFail()
  {
    if (RunTests)
    {
      var task = taskCreatingData_ with
                 {
                   OwnerPodId = LocalIPv4.GetLocalIPv4Ethernet(),
                   OwnerPodName = Dns.GetHostName(),
                   ReceptionDate = DateTime.UtcNow,
                   AcquisitionDate = DateTime.UtcNow,
                 };

      var result = await TaskTable!.AcquireTask(task,
                                                CancellationToken.None)
                                   .ConfigureAwait(false);

      Assert.AreNotEqual(LocalIPv4.GetLocalIPv4Ethernet(),
                         result.OwnerPodId);

      Assert.AreEqual(TaskStatus.Creating,
                      result.Status);

      Assert.AreEqual(null,
                      result.AcquisitionDate);

      Assert.AreEqual(null,
                      result.ReceptionDate);

      Assert.AreEqual(null,
                      result.SubmittedDate);
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
      await TaskTable!.StartTask(taskSubmittedData_,
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
                                                  await TaskTable!.StartTask(taskSubmittedData_ with
                                                                             {
                                                                               TaskId = "NotExistingTask",
                                                                             },
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
                  PageSize = 4,
                  Filters = new Filters
                            {
                              Or =
                              {
                                new FiltersAnd
                                {
                                  And =
                                  {
                                    new FilterField
                                    {
                                      String = new FilterString
                                               {
                                                 Field = new ApplicationField
                                                         {
                                                           ApplicationField_ = new ApplicationRawField
                                                                               {
                                                                                 Field = ApplicationRawEnumField.Name,
                                                                               },
                                                         },
                                                 Operator = FilterStringOperator.Equal,
                                                 Value    = Options.ApplicationName,
                                               },
                                    },
                                  },
                                },
                              },
                            },
                  Sort = new ListApplicationsRequest.Types.Sort
                         {
                           Direction = SortDirection.Desc,
                           Fields =
                           {
                             new ApplicationField
                             {
                               ApplicationField_ = new ApplicationRawField
                                                   {
                                                     Field = ApplicationRawEnumField.Name,
                                                   },
                             },
                           },
                         },
                };

      var validator = new ListApplicationsRequestValidator();
      Assert.IsTrue(validator.Validate(req)
                             .IsValid);

      var listTasks = await TaskTable!.ListApplicationsAsync(req.Filters.ToApplicationFilter(),
                                                             req.Sort.Fields.Select(sort => sort.ToField())
                                                                .ToList(),
                                                             false,
                                                             req.Page,
                                                             req.PageSize,
                                                             CancellationToken.None)
                                      .ConfigureAwait(false);

      var listTasksResponseTaskData = listTasks.applications.ToList();
      foreach (var task in listTasksResponseTaskData)
      {
        Console.WriteLine(task);
      }

      Assert.AreEqual(new Application(Options.ApplicationName,
                                      Options.ApplicationNamespace,
                                      Options.ApplicationVersion,
                                      Options.ApplicationService),
                      listTasksResponseTaskData.Single());
    }
  }

  [Test]
  public async Task ListApplicationFromTasksSortShouldSucceed()
  {
    if (RunTests)
    {
      var version1 = "version1";
      var version2 = "version2";
      var applicationName = Guid.NewGuid()
                                .ToString();
      var applicationNamespace = "ApplicationNamespace";
      var applicationService1  = "ApplicationService1";
      var applicationService2  = "ApplicationService2";

      var app1 = new Application(applicationName,
                                 applicationNamespace,
                                 version1,
                                 applicationService1);

      var app2 = app1 with
                 {
                   Version = version2,
                 };

      var app3 = app1 with
                 {
                   Service = applicationService2,
                 };

      var taskOptions1 = new TaskOptions(new Dictionary<string, string>(),
                                         TimeSpan.FromHours(1),
                                         2,
                                         1,
                                         "Partition",
                                         applicationName,
                                         version1,
                                         applicationNamespace,
                                         applicationService1,
                                         "EngineType");

      var taskOptions2 = taskOptions1 with
                         {
                           ApplicationVersion = version2,
                         };

      var taskOptions3 = taskOptions1 with
                         {
                           ApplicationService = applicationService2,
                         };

      var taskData1 = new TaskData("SessionId",
                                   Guid.NewGuid()
                                       .ToString(),
                                   "owner",
                                   "ownerpodname",
                                   "payload",
                                   new List<string>(),
                                   new List<string>(),
                                   new List<string>(),
                                   new List<string>(),
                                   TaskStatus.Completed,
                                   taskOptions1,
                                   new Output(false,
                                              ""));

      await TaskTable!.CreateTasks(new List<TaskData>
                                   {
                                     taskData1,
                                     taskData1 with
                                     {
                                       TaskId = Guid.NewGuid()
                                                    .ToString(),
                                       Options = taskOptions2,
                                     },
                                     taskData1 with
                                     {
                                       TaskId = Guid.NewGuid()
                                                    .ToString(),
                                       Options = taskOptions2,
                                     },
                                     taskData1 with
                                     {
                                       TaskId = Guid.NewGuid()
                                                    .ToString(),
                                       Options = taskOptions3,
                                     },
                                     taskData1 with
                                     {
                                       TaskId = Guid.NewGuid()
                                                    .ToString(),
                                       Options = taskOptions3,
                                     },
                                   },
                                   CancellationToken.None)
                      .ConfigureAwait(false);

      var req = new ListApplicationsRequest
                {
                  Page     = 0,
                  PageSize = 4,
                  Filters = new Filters
                            {
                              Or =
                              {
                                new FiltersAnd
                                {
                                  And =
                                  {
                                    new FilterField
                                    {
                                      String = new FilterString
                                               {
                                                 Field = new ApplicationField
                                                         {
                                                           ApplicationField_ = new ApplicationRawField
                                                                               {
                                                                                 Field = ApplicationRawEnumField.Name,
                                                                               },
                                                         },
                                                 Operator = FilterStringOperator.Equal,
                                                 Value    = applicationName,
                                               },
                                    },
                                  },
                                },
                              },
                            },
                  Sort = new ListApplicationsRequest.Types.Sort
                         {
                           Direction = SortDirection.Desc,
                           Fields =
                           {
                             new ApplicationField
                             {
                               ApplicationField_ = new ApplicationRawField
                                                   {
                                                     Field = ApplicationRawEnumField.Version,
                                                   },
                             },
                             new ApplicationField
                             {
                               ApplicationField_ = new ApplicationRawField
                                                   {
                                                     Field = ApplicationRawEnumField.Service,
                                                   },
                             },
                           },
                         },
                };

      var validator = new ListApplicationsRequestValidator();
      Assert.IsTrue(validator.Validate(req)
                             .IsValid);

      var listTasks = await TaskTable.ListApplicationsAsync(req.Filters.ToApplicationFilter(),
                                                            req.Sort.Fields.Select(sort => sort.ToField())
                                                               .ToList(),
                                                            req.Sort.Direction == SortDirection.Asc,
                                                            req.Page,
                                                            req.PageSize,
                                                            CancellationToken.None)
                                     .ConfigureAwait(false);

      var listTasksResponseTaskData = listTasks.applications.ToList();
      foreach (var task in listTasksResponseTaskData)
      {
        Console.WriteLine(task);
      }


      // First order by version => app2 then app3 or app1 because version2 is before version1 in desc order
      // app3 before app1 because desc ordering by service (service2 before service1 in desc)
      Assert.AreEqual(new List<Application>
                      {
                        app2,
                        app3,
                        app1,
                      },
                      listTasksResponseTaskData);
    }
  }

  [Test]
  public async Task ListTaskWithRequestShouldSucceed()
  {
    if (RunTests)
    {
      var listTasks = await TaskTable!.ListTasksAsync(data => data.SessionId == "SessionId",
                                                      data => data.SessionId,
                                                      data => data,
                                                      false,
                                                      0,
                                                      20,
                                                      CancellationToken.None)
                                      .ConfigureAwait(false);

      Assert.AreEqual(6,
                      listTasks.totalCount);
    }
  }


  [Test]
  public async Task ListTaskWithRequestOrderByTaskOptionsOptionsShouldSucceed()
  {
    if (RunTests)
    {
      var req = new ListTasksRequest
                {
                  Filters = new Armonik.Api.gRPC.V1.Tasks.Filters
                            {
                              Or =
                              {
                                new Armonik.Api.gRPC.V1.Tasks.FiltersAnd
                                {
                                  And =
                                  {
                                    new Armonik.Api.gRPC.V1.Tasks.FilterField
                                    {
                                      String = new Armonik.Api.gRPC.V1.Tasks.FilterString
                                               {
                                                 Value = "SessionId",
                                                 Field = new TaskField
                                                         {
                                                           TaskSummaryField = new TaskSummaryField
                                                                              {
                                                                                Field = TaskSummaryEnumField.SessionId,
                                                                              },
                                                         },
                                                 Operator = FilterStringOperator.Equal,
                                               },
                                    },
                                  },
                                },
                              },
                            },
                  Sort = new ListTasksRequest.Types.Sort
                         {
                           Direction = SortDirection.Asc,
                           Field = new TaskField
                                   {
                                     TaskOptionGenericField = new TaskOptionGenericField
                                                              {
                                                                Field = "test",
                                                              },
                                   },
                         },
                };

      var listTasks = await TaskTable!.ListTasksAsync(req.Filters.ToTaskDataFilter(),
                                                      req.Sort.ToField(),
                                                      data => data,
                                                      false,
                                                      0,
                                                      20,
                                                      CancellationToken.None)
                                      .ConfigureAwait(false);

      Assert.AreEqual(6,
                      listTasks.totalCount);
    }
  }

  [Test]
  public async Task ListTaskWithListInRequestShouldSucceed()
  {
    if (RunTests)
    {
      var statusList = new List<TaskStatus>
                       {
                         TaskStatus.Error,
                         TaskStatus.Completed,
                       };

      var listTasks = await TaskTable!.ListTasksAsync(data => statusList.Contains(data.Status),
                                                      data => data.SessionId,
                                                      data => data,
                                                      false,
                                                      0,
                                                      20,
                                                      CancellationToken.None)
                                      .ConfigureAwait(false);

      Assert.AreEqual(3,
                      listTasks.totalCount);
    }
  }

  [Test]
  public async Task ListTaskEmptyResultShouldSucceed()
  {
    if (RunTests)
    {
      var listTasks = await TaskTable!.ListTasksAsync(data => data.TaskId == "NotExisting",
                                                      data => data.SessionId,
                                                      data => data,
                                                      false,
                                                      0,
                                                      20,
                                                      CancellationToken.None)
                                      .ConfigureAwait(false);

      Assert.AreEqual(0,
                      listTasks.totalCount);
    }
  }

  [Test]
  [TestCaseSource(nameof(TestCasesFilter))]
  public async Task ListTaskFilter(ListTasksRequest request,
                                   int              count)
  {
    if (RunTests)
    {
      var listTasks = await TaskTable!.ListTasksAsync(request.Filters.ToTaskDataFilter(),
                                                      data => data.SessionId,
                                                      data => data,
                                                      false,
                                                      0,
                                                      20,
                                                      CancellationToken.None)
                                      .ConfigureAwait(false);

      Assert.AreEqual(count,
                      listTasks.totalCount);
    }
  }

  public static IEnumerable<TestCaseData> TestCasesFilter()
  {
    TestCaseData CaseTrue(Armonik.Api.gRPC.V1.Tasks.FilterField filterField)
      => new TestCaseData(ListTasksHelper.CreateListSessionsRequest(new ListTasksRequest.Types.Sort(),
                                                                    new[]
                                                                    {
                                                                      filterField,
                                                                      ListTasksHelper.CreateListTasksFilterString(TaskSummaryEnumField.TaskId,
                                                                                                                  FilterStringOperator.Equal,
                                                                                                                  "TaskCompletedId"),
                                                                    }),
                          1).SetArgDisplayNames(filterField.ToDisplay() + " true");

    TestCaseData CaseFalse(Armonik.Api.gRPC.V1.Tasks.FilterField filterField)
      => new TestCaseData(ListTasksHelper.CreateListSessionsRequest(new ListTasksRequest.Types.Sort(),
                                                                    new[]
                                                                    {
                                                                      filterField,
                                                                      ListTasksHelper.CreateListTasksFilterString(TaskSummaryEnumField.TaskId,
                                                                                                                  FilterStringOperator.Equal,
                                                                                                                  "TaskCompletedId"),
                                                                    }),
                          0).SetArgDisplayNames(filterField.ToDisplay() + " false");

    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterDate(TaskSummaryEnumField.CreatedAt,
                                                                    FilterDateOperator.After,
                                                                    DateTime.UtcNow));
    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterDate(TaskSummaryEnumField.CreatedAt,
                                                                    FilterDateOperator.AfterOrEqual,
                                                                    DateTime.UtcNow));
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterDate(TaskSummaryEnumField.CreatedAt,
                                                                     FilterDateOperator.Before,
                                                                     DateTime.UtcNow));
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterDate(TaskSummaryEnumField.CreatedAt,
                                                                     FilterDateOperator.BeforeOrEqual,
                                                                     DateTime.UtcNow));

    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterDate(TaskSummaryEnumField.StartedAt,
                                                                    FilterDateOperator.Equal,
                                                                    null));
    yield return CaseTrue(new Armonik.Api.gRPC.V1.Tasks.FilterField
                          {
                            Date = new FilterDate
                                   {
                                     Field = new TaskField
                                             {
                                               TaskSummaryField = new TaskSummaryField
                                                                  {
                                                                    Field = TaskSummaryEnumField.StartedAt,
                                                                  },
                                             },
                                     Operator = FilterDateOperator.Equal,
                                   },
                          });

    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterString(TaskSummaryEnumField.SessionId,
                                                                      FilterStringOperator.Equal,
                                                                      "SessionId"));
    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterString(TaskSummaryEnumField.SessionId,
                                                                      FilterStringOperator.StartsWith,
                                                                      "SessionId"));
    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterString(TaskSummaryEnumField.SessionId,
                                                                      FilterStringOperator.EndsWith,
                                                                      "SessionId"));
    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterString(TaskSummaryEnumField.SessionId,
                                                                      FilterStringOperator.Contains,
                                                                      "SessionId"));
    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterString(TaskSummaryEnumField.SessionId,
                                                                      FilterStringOperator.NotContains,
                                                                      "BadSessionId"));
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterString(TaskSummaryEnumField.SessionId,
                                                                       FilterStringOperator.Equal,
                                                                       "BadSessionId"));

    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterString("key1",
                                                                      FilterStringOperator.Equal,
                                                                      "val1"));
    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterString("key1",
                                                                      FilterStringOperator.Contains,
                                                                      "val1"));
    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterString("key1",
                                                                      FilterStringOperator.StartsWith,
                                                                      "val1"));
    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterString("key1",
                                                                      FilterStringOperator.EndsWith,
                                                                      "val1"));

    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterString(TaskOptionEnumField.PartitionId,
                                                                      FilterStringOperator.Equal,
                                                                      "part1"));
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterString(TaskOptionEnumField.PartitionId,
                                                                       FilterStringOperator.Equal,
                                                                       "BadPartitionId"));

    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterStatus(TaskSummaryEnumField.Status,
                                                                      FilterStatusOperator.Equal,
                                                                      TaskStatus.Completed));
    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterStatus(TaskSummaryEnumField.Status,
                                                                      FilterStatusOperator.NotEqual,
                                                                      TaskStatus.Cancelling));
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterStatus(TaskSummaryEnumField.Status,
                                                                       FilterStatusOperator.Equal,
                                                                       TaskStatus.Cancelling));

    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterNumber(TaskOptionEnumField.MaxRetries,
                                                                      FilterNumberOperator.LessThanOrEqual,
                                                                      5));
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterNumber(TaskOptionEnumField.MaxRetries,
                                                                       FilterNumberOperator.LessThan,
                                                                       5));
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
                                                  status,
                                                  Options,
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

      Assert.AreEqual(0,
                      cancelledTasks);

      var taskData = await TaskTable.ReadTaskAsync(taskId)
                                    .ConfigureAwait(false);
      Assert.AreEqual(status,
                      taskData.Status);
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
                      cancelledTasks);
    }
  }

  [Test]
  [TestCase(TaskStatus.Completed,
            1)]
  [TestCase(TaskStatus.Processing,
            2)]
  public async Task FindTasksAsyncStatusShouldSucceed(TaskStatus status,
                                                      int        expectedCount)
  {
    if (RunTests)
    {
      var cancelledTasks = await TaskTable!.FindTasksAsync(data => data.Status == status,
                                                           data => data.TaskId,
                                                           CancellationToken.None)
                                           .ConfigureAwait(false);

      Assert.AreEqual(expectedCount,
                      cancelledTasks.Count());
    }
  }

  [Test]
  public async Task FindTasksAsyncContainsShouldSucceed()
  {
    if (RunTests)
    {
      var cancelledTasks = await TaskTable!.FindTasksAsync(data => data.DataDependencies.Contains("dependency1"),
                                                           data => data.DataDependencies,
                                                           CancellationToken.None)
                                           .ToListAsync()
                                           .ConfigureAwait(false);

      Assert.AreEqual(6,
                      cancelledTasks.Count());
      Assert.AreEqual(6,
                      cancelledTasks.SelectMany(list => list)
                                    .Count(s => s == "dependency1"));
    }
  }

  [Test]
  public async Task RemoveRemainingDataDependenciesShouldSucceed()
  {
    if (RunTests)
    {
      var taskId = Guid.NewGuid()
                       .ToString();
      var dd1 = "dependency.1";
      var dd2 = "dependency.2";

      await TaskTable!.CreateTasks(new[]
                                   {
                                     new TaskData("SessionId",
                                                  taskId,
                                                  "OwnerPodId",
                                                  "OwnerPodName",
                                                  "PayloadId",
                                                  new[]
                                                  {
                                                    "parent1",
                                                  },
                                                  new[]
                                                  {
                                                    dd1,
                                                    dd2,
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
                                                             "")),
                                   })
                      .ConfigureAwait(false);

      await TaskTable.RemoveRemainingDataDependenciesAsync(new[]
                                                           {
                                                             taskId,
                                                           },
                                                           new[]
                                                           {
                                                             dd1,
                                                             dd2,
                                                           },
                                                           CancellationToken.None)
                     .ConfigureAwait(false);

      var taskData = await TaskTable.ReadTaskAsync(taskId,
                                                   CancellationToken.None)
                                    .ConfigureAwait(false);

      Assert.IsEmpty(taskData.RemainingDataDependencies);
    }
  }

  [Test]
  public async Task RemoveRemainingDataDependenciesDepDoesNotExistShouldSucceed()
  {
    if (RunTests)
    {
      var taskId = Guid.NewGuid()
                       .ToString();
      var dd1 = "dependency1";

      await TaskTable!.CreateTasks(new[]
                                   {
                                     new TaskData("SessionId",
                                                  taskId,
                                                  "OwnerPodId",
                                                  "OwnerPodName",
                                                  "PayloadId",
                                                  new[]
                                                  {
                                                    "parent1",
                                                  },
                                                  new List<string>(),
                                                  new[]
                                                  {
                                                    "output1",
                                                    "output2",
                                                  },
                                                  Array.Empty<string>(),
                                                  TaskStatus.Creating,
                                                  Options,
                                                  new Output(true,
                                                             "")),
                                   })
                      .ConfigureAwait(false);

      await TaskTable.RemoveRemainingDataDependenciesAsync(new[]
                                                           {
                                                             taskId,
                                                           },
                                                           new[]
                                                           {
                                                             dd1,
                                                           },
                                                           CancellationToken.None)
                     .ConfigureAwait(false);

      var taskData = await TaskTable.ReadTaskAsync(taskId,
                                                   CancellationToken.None)
                                    .ConfigureAwait(false);

      Assert.IsEmpty(taskData.RemainingDataDependencies);
      Assert.IsEmpty(taskData.DataDependencies);
    }
  }
}
