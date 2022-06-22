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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

using Output = ArmoniK.Core.Common.Storage.Output;
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
      TaskTable.CreateTasks(new[]
                            {
                              new TaskData("SessionId",
                                           "TaskCompletedId",
                                           "OwnerPodId",
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
                                           TaskStatus.Completed,
                                           "",
                                           default,
                                           DateTime.Now,
                                           DateTime.Now + TimeSpan.FromSeconds(1),
                                           DateTime.Now + TimeSpan.FromSeconds(10),
                                           DateTime.Now + TimeSpan.FromSeconds(20),
                                           DateTime.Now,
                                           new Output(true,
                                                      "")),
                              new TaskData("SessionId",
                                           "TaskCreatingId",
                                           "OwnerPodId",
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
                                           "",
                                           default,
                                           DateTime.Now,
                                           null,
                                           null,
                                           null,
                                           DateTime.Now,
                                           new Output(false,
                                                      "")),
                              new TaskData("SessionId",
                                           "TaskProcessingId",
                                           "OwnerPodId",
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
                                           "",
                                           default,
                                           DateTime.Now,
                                           DateTime.Now + TimeSpan.FromSeconds(1),
                                           DateTime.Now + TimeSpan.FromSeconds(10),
                                           null,
                                           DateTime.Now,
                                           new Output(false,
                                                      "")),
                              new TaskData("SessionId",
                                           "TaskAnotherProcessingId",
                                           "OwnerPodId",
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
                                           "",
                                           default,
                                           DateTime.Now,
                                           DateTime.Now + TimeSpan.FromSeconds(1),
                                           DateTime.Now + TimeSpan.FromSeconds(10),
                                           null,
                                           DateTime.Now,
                                           new Output(false,
                                                      "")),
                              new TaskData("SessionId",
                                           "TaskSubmittedId",
                                           "",
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
                                           "",
                                           default,
                                           DateTime.Now,
                                           DateTime.Now + TimeSpan.FromSeconds(1),
                                           null,
                                           null,
                                           DateTime.Now,
                                           new Output(false,
                                                      "")),
                              new TaskData("SessionId",
                                           "TaskFailedId",
                                           "OwnerPodId",
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
                                           TaskStatus.Failed,
                                           "",
                                           default,
                                           DateTime.Now,
                                           DateTime.Now + TimeSpan.FromSeconds(1),
                                           DateTime.Now + TimeSpan.FromSeconds(10),
                                           DateTime.Now + TimeSpan.FromSeconds(100),
                                           DateTime.Now,
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

  /* Interface to test */
  protected ITaskTable TaskTable;

  /* Boolean to control that tests are executed in
   * an instance of this class */
  protected bool RunTests;

  /* Function be override so it returns the suitable instance
   * of TaskTable to the corresponding interface implementation */
  public virtual void GetTaskTableInstance()
  {
  }

  [Test]
  public async Task ReadTaskAsyncShouldSucceed()
  {
    if (RunTests)
    {
      var result = await TaskTable.ReadTaskAsync("TaskCompletedId",
                                                 CancellationToken.None)
                                  .ConfigureAwait(false);

      Assert.AreEqual("TaskCompletedId",
                      result.TaskId);
    }
  }

  [Test]
  public void ReadTaskAsyncShouldFail()
  {
    if (RunTests)
    {

      Assert.ThrowsAsync<TaskNotFoundException>(async () => await TaskTable.ReadTaskAsync("TaskDoNotExists",
                                                                                          CancellationToken.None)
                                                                           .ConfigureAwait(false));
    }
  }


  [Test]
  public async Task UpdateTaskStatusAsyncShouldSucceed()
  {
    if (RunTests)
    {
      await TaskTable.UpdateTaskStatusAsync("TaskProcessingId",
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
                                             await TaskTable.UpdateTaskStatusAsync("TaskFailedId",
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
      await TaskTable.UpdateAllTaskStatusAsync(testFilter,
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
      await TaskTable.UpdateAllTaskStatusAsync(testFilter,
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
      await TaskTable.CancelTasks(testFilter,
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

      Assert.AreEqual(TaskStatus.Canceling,
                      resCreating.Single()
                                 .Status);
      Assert.AreEqual(TaskStatus.Canceling,
                      resProcessing.Single()
                                   .Status);

      var resAnotherProcessing = await TaskTable.GetTaskStatus(new[]
                                                        {
                                                          "TaskAnotherProcessingId",
                                                        },
                                                        CancellationToken.None)
                                         .ConfigureAwait(false);

      Assert.AreNotEqual(TaskStatus.Canceling,
                         resAnotherProcessing.Single()
                                             .Status);
    }
  }

  [Test(Description = "Forbidden update: A given Task its on a final status")]
  public void UpdateAllTaskStatusAsyncShouldFail()
  {
    if (RunTests)
    {
      var testFilter = new TaskFilter
                       {
                         Included = new TaskFilter.Types.StatusesRequest
                                    {
                                      Statuses =
                                      {
                                        TaskStatus.Failed, // Presence of this status should generate an exception
                                        TaskStatus.Creating,
                                        TaskStatus.Processing,
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
                                             await TaskTable.UpdateAllTaskStatusAsync(testFilter,
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
      var result = await TaskTable.IsTaskCancelledAsync("TaskCreatingId",
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
                                                  await TaskTable.IsTaskCancelledAsync("TaskDoesNotExist",
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
      var result = TaskTable.CancelSessionAsync("SessionId",
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
                                                     await TaskTable.CancelSessionAsync("NonExistingSessionId",
                                                                                        CancellationToken.None)
                                                                    .ConfigureAwait(false);
                                                   });
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

      var result = await TaskTable.CountTasksAsync(testFilter,
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
      var result = await TaskTable.CountAllTasksAsync(TaskStatus.Processing,
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
      var result = TaskTable.SetTaskSuccessAsync("TaskProcessingId",
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
      var result = TaskTable.SetTaskErrorAsync("TaskProcessingId",
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
  public async Task GetTaskOutputShouldSucceed()
  {
    if (RunTests)
    {
      var expectedOutput = new Output(true,
                                      "");
      var result = await TaskTable.GetTaskOutput("TaskCompletedId",
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
                                                  await TaskTable.GetTaskOutput("NonExistingTaskId",
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
                           };
      var result = await TaskTable.GetTaskExpectedOutputKeys("TaskCompletedId",
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
                                                  await TaskTable.GetTaskExpectedOutputKeys("NonExistingTaskId",
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
      var result = await TaskTable.GetParentTaskIds("TaskCompletedId",
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
                                                  await TaskTable.GetParentTaskIds("NonExistingTaskId",
                                                                                   CancellationToken.None)
                                                                 .ConfigureAwait(false);
                                                });
    }
  }

  [Test]
  public async Task AcquireTaskShouldSucceed()
  {
    if (RunTests)
    {

      var result = await TaskTable.AcquireTask("TaskSubmittedId",
                                               CancellationToken.None)
                                  .ConfigureAwait(false);

      Assert.IsTrue(result);
    }
  }

  [Test]
  public async Task AcquireTaskShouldFail()
  {
    if (RunTests)
    {

      var result = await TaskTable.AcquireTask("TaskFailedId",
                                               CancellationToken.None)
                                  .ConfigureAwait(false);

      Assert.IsFalse(result);
    }
  }

  [Test]
  public async Task FinalizeTaskCreationShouldSucceed()
  {
    if (RunTests)
    {

      var result = await TaskTable.FinalizeTaskCreation(new List<string>
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
      var result = await TaskTable.FinalizeTaskCreation(new List<string>
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
      await TaskTable.StartTask("TaskSubmittedId",
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
                                                  await TaskTable.StartTask("NonExistingTaskId",
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
                                                  await TaskTable.DeleteTaskAsync("NonExistingTaskId",
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
      await TaskTable.DeleteTaskAsync("TaskSubmittedId",
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
      var taskList = await TaskTable.ListTasksAsync(new TaskFilter
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
}
