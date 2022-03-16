// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Adapters.MongoDB.Table;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Tests;

[TestFixture]
public class TaskTableTestBase
{
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

  [SetUp]
  public void SetUp()
  {
    GetTaskTableInstance();

    if (RunTests)
    {
      TaskTable.CreateTasks(new[]
               {
                 new TaskData("SessionId",
                              "PTaskId",
                              "DispatchId",
                              "TaskCompletedId",
                              default,
                              default,
                              true,
                              new[] { (byte) (1), (byte) (2) },
                              TaskStatus.Completed,
                              default,
                              new[] { "Ancestor1DispatchId", "Ancestor2DispatchId" },
                              DateTime.Now,
                              default),
                 new TaskData("SessionId",
                              "PTaskId",
                              "DispatchId",
                              "TaskCreatingId",
                              default,
                              default,
                              false,
                              new List<byte>().ToArray(),
                              TaskStatus.Creating,
                              default,
                              new[] { "Ancestor3DispatchId" },
                              DateTime.Now,
                              default),
                 new TaskData("SessionId",
                              "PTaskId",
                              "DispatchId",
                              "TaskProcessingId",
                              default,
                              default,
                              false,
                              new List<byte>().ToArray(),
                              TaskStatus.Processing,
                              default,
                              new[] { "Ancestor4DispatchId" },
                              DateTime.Now,
                              default),
                 new TaskData("SessionId",
                              "PTaskId",
                              "DispatchId",
                              "TaskFailedId",
                              default,
                              default,
                              false,
                              new List<byte>().ToArray(),
                              TaskStatus.Failed,
                              default,
                              new List<string>(),
                              DateTime.Now,
                              default),
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

  [Test]
  public async Task ReadTaskAsyncShouldSucceed()
  {
    if (RunTests)
    {
      var payload = new[] { (byte) (1), (byte) (2) };
      var result = await TaskTable.ReadTaskAsync("TaskCompletedId",
                                                 CancellationToken.None);

      Assert.AreEqual(payload,
                      result.Payload);
    }
  }

  [Test]
  public async Task ReadTaskAsyncShouldFail()
  {
    if (RunTests)
    {
      var payload = new[] { (byte) (1), (byte) (2) };
      var result = await TaskTable.ReadTaskAsync("TaskFailedId",
                                                 CancellationToken.None);

      Assert.AreNotEqual(payload,
                         result.Payload);
    }
  }

  [Test]
  public async Task GetTaskDispatchIdShouldSucceed()
  {
    if (RunTests)
    {
      var result = await TaskTable.GetTaskDispatchId("TaskCompletedId",
                                                     CancellationToken.None);

      Assert.IsTrue(result == "DispatchId");
    }
  }

  [Test]
  public void GetTaskDispatchIdShouldFail()
  {
    if (RunTests)
    {
      Assert.ThrowsAsync<ArmoniKException>(async () =>
      {
        await TaskTable.GetTaskDispatchId("NonExistingTaskId",
                                          CancellationToken.None);
      });
    }
  }

  [Test]
  public async Task GetTaskAncestorDispatchIdsShouldSucceed()
  {
    if (RunTests)
    {
      var ancestors = new[] { "Ancestor1DispatchId", "Ancestor2DispatchId" };
      var result = await TaskTable.GetTaskAncestorDispatchIds("TaskCompletedId",
                                                              CancellationToken.None);

      Assert.AreEqual(result,
                      ancestors);
    }
  }

  [Test]
  public void GetTaskAncestorDispatchIdsShouldFail()
  {
    if (RunTests)
    {
      Assert.ThrowsAsync<ArmoniKException>(async () =>
      {
        await TaskTable.GetTaskAncestorDispatchIds("NonExistingTaskId",
                                                   CancellationToken.None);
      });
    }
  }

  [Test]
  public async Task ChangeTaskDispatchShouldSucceed()
  {
    if (RunTests)
    {
      await TaskTable.ChangeTaskDispatch("DispatchId",
                                         "NewDispatchId",
                                         CancellationToken.None);
      var result = await TaskTable.GetTaskDispatchId("TaskCompletedId",
                                                     CancellationToken.None);

      Assert.IsTrue(result == "NewDispatchId");
    }
  }

  [Test]
  public void ChangeTaskDispatchShouldFail()
  {
    if (RunTests)
    {
      Assert.ThrowsAsync<ArmoniKException>(async () =>
      {
        await TaskTable.ChangeTaskDispatch("NonExistingDispatchId",
                                           "NewDispatchId",
                                           CancellationToken.None);
      });
    }
  }

  [Test]
  public async Task UpdateTaskStatusAsyncShouldSucceed()
  {
    if (RunTests)
    {
      await TaskTable.UpdateTaskStatusAsync("TaskProcessingId",
                                            TaskStatus.Processed,
                                            CancellationToken.None);
      var result = await TaskTable.GetTaskStatus("TaskProcessingId",
                                                 CancellationToken.None);

      Assert.IsTrue(result == TaskStatus.Processed);
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
                                              CancellationToken.None);
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
        Dispatch = new TaskFilter.Types.IdsRequest
        {
          Ids =
          {
            "DispatchId",
          },
        },
      };
      await TaskTable.UpdateAllTaskStatusAsync(testFilter,
                                               TaskStatus.Timeout,
                                               CancellationToken.None);
      var resCreating = await TaskTable.GetTaskStatus("TaskCreatingId",
                                                 CancellationToken.None);
      var resProcessing = await TaskTable.GetTaskStatus("TaskProcessingId",
                                                 CancellationToken.None);

      Assert.IsTrue(resCreating == TaskStatus.Timeout && resProcessing == TaskStatus.Timeout);
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
        Dispatch = new TaskFilter.Types.IdsRequest
        {
          Ids =
          {
            "DispatchId",
          },
        },
      };
      Assert.ThrowsAsync<ArmoniKException>(async () =>
      {
        await TaskTable.UpdateAllTaskStatusAsync(testFilter,
                                                 TaskStatus.Timeout,
                                                 CancellationToken.None);
      });
    }
  }
}