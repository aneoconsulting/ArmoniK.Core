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

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Tests;

[TestFixture]
public class DispatchTableTestBase
{
  [SetUp]
  public void SetUp()
  {
    GetDispatchTableInstance();

    if (RunTests)
    {
      DispatchTable.TryAcquireDispatchAsync("SessionId",
                                            "TaskId",
                                            "DispatchId",
                                            new Dictionary<string, string>(),
                                            CancellationToken.None)
                   .Wait();
      DispatchTable.TryAcquireDispatchAsync("SessionId",
                                            "Task2Id",
                                            "Dispatch2Id",
                                            new Dictionary<string, string>(),
                                            CancellationToken.None)
                   .Wait();
    }
  }

  [TearDown]
  public virtual void TearDown()
  {
    DispatchTable = null;
    RunTests      = false;
  }

  protected IDispatchTable DispatchTable;

  protected bool RunTests;

  public virtual void GetDispatchTableInstance()
  {
  }

  [Test]
  public async Task TryAcquireDispatchAsyncSucceeds()
  {
    if (RunTests)
    {
      var result = await DispatchTable.TryAcquireDispatchAsync("SessionId",
                                                               "Task2Id",
                                                               "Dispatch2Id",
                                                               new Dictionary<string, string>(),
                                                               CancellationToken.None)
                                      .ConfigureAwait(false);
      Assert.IsTrue(result);
    }
  }

  [Test]
  public async Task TryAcquireDispatchAsyncFails()
  {
    if (RunTests)
    {
      var result = await DispatchTable.TryAcquireDispatchAsync("SessionId",
                                                               "TaskId",
                                                               "NonExistingDispatchId",
                                                               new Dictionary<string, string>(),
                                                               CancellationToken.None)
                                      .ConfigureAwait(false);
      Assert.IsFalse(result);
    }
  }

  [Test]
  public async Task GetDispatchAsyncSucceeds()
  {
    if (RunTests)
    {
      var dispatch = await DispatchTable.GetDispatchAsync("DispatchId",
                                                          CancellationToken.None)
                                        .ConfigureAwait(false);
      Assert.IsTrue(dispatch.Id == "DispatchId");
    }
  }

  [Test]
  public void GetDispatchAsyncFails()
  {
    if (RunTests)
    {
      Assert.ThrowsAsync<ArmoniKException>(async () =>
                                           {
                                             await DispatchTable.GetDispatchAsync("NonExistingDispatchId",
                                                                                  CancellationToken.None)
                                                                .ConfigureAwait(false);
                                           });
    }
  }

  [Test]
  public async Task AddStatusToDispatchSucceeds()
  {
    if (RunTests)
    {
      var result = DispatchTable.AddStatusToDispatch("DispatchId",
                                                     TaskStatus.Dispatched,
                                                     CancellationToken.None);
      var dispatch = await DispatchTable.GetDispatchAsync("DispatchId",
                                                          CancellationToken.None)
                                        .ConfigureAwait(false);

      var statusWasInserted = dispatch.Statuses.AsQueryable()
                                      .Select(a => a.Status == TaskStatus.Dispatched)
                                      .FirstOrDefault();

      Assert.IsTrue(result.IsCompletedSuccessfully && statusWasInserted);
    }
  }

  [Test]
  public void AddStatusToDispatchFails()
  {
    if (RunTests)
    {
      Assert.ThrowsAsync<ArmoniKException>(async () =>
                                           {
                                             await DispatchTable.AddStatusToDispatch("BadDispatchId",
                                                                                     TaskStatus.Creating,
                                                                                     CancellationToken.None)
                                                                .ConfigureAwait(false);
                                           });
    }
  }

  [Test]
  public async Task ExtendDispatchTtlSucceeds()
  {
    if (RunTests)
    {
      var result = DispatchTable.ExtendDispatchTtl("DispatchId",
                                                   CancellationToken.None);
      await result.ConfigureAwait(false);
      Assert.IsTrue(result.IsCompletedSuccessfully);
    }
  }

  [Test]
  public void ExtendDispatchTtlFails()
  {
    if (RunTests)
    {
      Assert.ThrowsAsync<ArmoniKException>(async () =>
                                           {
                                             await DispatchTable.ExtendDispatchTtl("BadDispatchId",
                                                                                   CancellationToken.None)
                                                                .ConfigureAwait(false);
                                           });
    }
  }

  [Test]
  public async Task DeleteDispatchFromTaskIdAsyncSucceeds()
  {
    if (RunTests)
    {
      var result = DispatchTable.DeleteDispatchFromTaskIdAsync("TaskId",
                                                               CancellationToken.None);
      await result.ConfigureAwait(false);
      Assert.IsTrue(result.IsCompletedSuccessfully);
    }
  }

  [Test]
  public void DeleteDispatchFromTaskIdAsyncFails()
  {
    if (RunTests)
    {
      Assert.ThrowsAsync<ArmoniKException>(async () =>
                                           {
                                             await DispatchTable.DeleteDispatchFromTaskIdAsync("BadTaskId",
                                                                                               CancellationToken.None)
                                                                .ConfigureAwait(false);
                                           });
    }
  }

  [Test]
  public async Task DeleteDispatchSucceeds()
  {
    if (RunTests)
    {
      var result = DispatchTable.DeleteDispatch("DispatchId",
                                                CancellationToken.None);
      await result.ConfigureAwait(false);
      Assert.IsTrue(result.IsCompletedSuccessfully);
    }
  }

  [Test]
  public void DeleteDispatchFails()
  {
    if (RunTests)
    {
      Assert.ThrowsAsync<ArmoniKException>(async () =>
                                           {
                                             await DispatchTable.DeleteDispatch("BadDispatchId",
                                                                                CancellationToken.None)
                                                                .ConfigureAwait(false);
                                           });
    }
  }
}
