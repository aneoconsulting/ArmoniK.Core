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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Storage;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using Assert = Microsoft.VisualStudio.TestTools.UnitTesting.Assert;

namespace ArmoniK.Core.Common.Tests;

[TestClass]
public abstract class ResultTableTestBase
{
  public abstract IResultTable GetResultTableInstance();

  [TestMethod]
  public async Task ResultsAreAvailableShouldSucceed()
  {
    var resultTable = GetResultTableInstance();

    var checkTable = await resultTable.AreResultsAvailableAsync("SessionId",
                                                                new[] { "ResultIsAvailable" },
                                                                CancellationToken.None);
    Assert.IsTrue(checkTable);
  }

  [TestMethod]
  public async Task ResultsAreAvailableShouldFail()
  {
    var resultTable = GetResultTableInstance();

    var checkTable = await resultTable.AreResultsAvailableAsync("SessionId",
                                                                new[] { "ResultIsNotAvailable" },
                                                                CancellationToken.None);
    Assert.IsFalse(checkTable);
  }

  [TestMethod]
  public async Task ChangeResultDispatchShouldSucceed()
  {
    var resultTable = GetResultTableInstance();

    await resultTable.ChangeResultDispatch("SessionId",
                                           "DispatchId",
                                           "NewDispatchId",
                                           CancellationToken.None);
    var result = await resultTable.GetResult("SessionId",
                                             "ResultIsAvailable",
                                             CancellationToken.None);

    Assert.IsTrue(result.OriginDispatchId == "NewDispatchId");
  }

  [TestMethod]
  public void ChangeResultDispatchShouldFail()
  {
    var resultTable = GetResultTableInstance();

    Assert.ThrowsExceptionAsync<KeyNotFoundException>(async () =>
    {
      await resultTable.ChangeResultDispatch("NonExistingSessionId",
                                             "",
                                             "",
                                             CancellationToken.None);
    });
  }

  [TestMethod]
  public async Task ChangeResultOwnershipShouldSucceed()
  {
    var resultTable = GetResultTableInstance();

    await resultTable.ChangeResultOwnership("SessionId",
                                            new[] { "ResultIsAvailable" },
                                            "OwnerId",
                                            "NewOwnerId",
                                            CancellationToken.None);
    var result = await resultTable.GetResult("SessionId",
                                             "ResultIsAvailable",
                                             CancellationToken.None);

    Assert.IsTrue(result.OwnerTaskId == "NewOwnerId");
  }

  [TestMethod]
  public void CreateShouldSucceed()
  {
    var resultTable = GetResultTableInstance();

    var success = resultTable.Create(new[]
    {
      new Result("AnotherSessionId",
                 "ResultIsAvailable",
                 "OwnerId",
                 "DispatchId",
                 true,
                 DateTime.Today,
                 new[] { (byte) 1 }),
    });

    Assert.IsTrue(success.IsCompletedSuccessfully);
  }

  [TestMethod]
  public void CreateShouldFail()
  {
    var resultTable = GetResultTableInstance();

    /* Check if an exception is thrown when attempting to
       create an already existing result entry */
    Assert.ThrowsExceptionAsync<InvalidOperationException>(async () =>
    {
      await resultTable.Create(new[]
      {
        new Result("SessionId",
                   "ResultIsAvailable",
                   "",
                   "",
                   true,
                   DateTime.Today,
                   new[] { (byte) 1 })
      });
    });
  }

  [TestMethod]
  public async Task DeleteResultsShouldRemoveAll()
  {
    var resultTable = GetResultTableInstance();

    await resultTable.DeleteResults("SessionId",
                                     CancellationToken.None);

    var resList = resultTable.ListResultsAsync("SessionId",
                                                CancellationToken.None);

    // Query first element, function returns default if the list is empty
    var firstElement = await resList.FirstOrDefaultAsync();

    Assert.IsTrue(firstElement == default);
  }

  [TestMethod]
  public async Task DeleteResultShouldRemove()
  {
    var resultTable = GetResultTableInstance();

    await resultTable.DeleteResult("SessionId",
                                   "ResultIsAvailable",
                                   CancellationToken.None);

    await Assert.ThrowsExceptionAsync<KeyNotFoundException>(async () =>
    {
      await resultTable.GetResult("SessionId",
                                   "ResultIsAvailable",
                                   CancellationToken.None);
    });
  }

  [TestMethod]
  public async Task SetResultShouldSucceed()
  {
    var resultTable = GetResultTableInstance();

    await resultTable.SetResult("SessionId",
                                "OwnerId",
                                "ResultIsNotAvailable",
                                CancellationToken.None);

    var result = await resultTable.GetResult("SessionId",
                                             "ResultIsNotAvailable",
                                             CancellationToken.None);

    Assert.IsTrue(result.IsResultAvailable);
  }

  [TestMethod]
  public async Task SetResultSmallPayloadShouldSucceed()
  {
    var resultTable  = GetResultTableInstance();
    var smallPayload = new[] { (byte) (1), (byte) (2) };

    await resultTable.SetResult("SessionId",
                                "OwnerId",
                                "ResultIsNotAvailable",
                                smallPayload,
                                CancellationToken.None);
    var result = await resultTable.GetResult("SessionId",
                                             "ResultIsNotAvailable",
                                             CancellationToken.None);

    Assert.AreEqual(result.Data,
                    smallPayload);
  }
}