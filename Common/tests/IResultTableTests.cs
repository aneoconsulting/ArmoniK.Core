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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

[TestFixture]
public class ResultTableTestBase
{
  /* Interface to test */
  protected IResultTable ResultTable;

  /* Boolean to control that tests are executed in
   * an instance of this class */
  protected bool RunTests;

  /* Function be override so it returns the suitable instance
   * of ResultTable to the corresponding interface implementation */
  public virtual void GetResultTableInstance()
  {
  }

  [SetUp]
  public void SetUp()
  {
    GetResultTableInstance();

    if (RunTests)
    {
      ResultTable.Create(new[]
      {
        new Result("SessionId",
                   "ResultIsAvailable",
                   "OwnerId",
                   "DispatchId",
                   true,
                   DateTime.Today,
                   new[] { (byte) 1 }),
        new Result("SessionId",
                   "ResultIsNotAvailable",
                   "OwnerId",
                   "DispatchId",
                   false,
                   DateTime.Today,
                   new[] { (byte) 1 }),
      }).Wait();
    }
  }

  [TearDown]
  public virtual void TearDown()
  {
    ResultTable = null;
    RunTests    = false;
  }

  [Test]
  public async Task ResultsAreAvailableShouldSucceed()
  {
    if (RunTests)
    {
      var checkTable = await ResultTable.AreResultsAvailableAsync("SessionId",
                                                                  new[] { "ResultIsAvailable" },
                                                                  CancellationToken.None);
      Assert.IsTrue(checkTable);
    }
  }

  [Test]
  public async Task ResultsAreAvailableShouldFail()
  {
    if (RunTests)
    {
      var checkTable = await ResultTable.AreResultsAvailableAsync("SessionId",
                                                                  new[] { "ResultIsNotAvailable" },
                                                                  CancellationToken.None);
      Assert.IsFalse(checkTable);
    }
  }

  [Test]
  public async Task ChangeResultDispatchShouldSucceed()
  {
    if (RunTests)
    {
      await ResultTable.ChangeResultDispatch("SessionId",
                                             "DispatchId",
                                             "NewDispatchId",
                                             CancellationToken.None);
      var result = await ResultTable.GetResult("SessionId",
                                               "ResultIsAvailable",
                                               CancellationToken.None);

      Assert.IsTrue(result.OriginDispatchId == "NewDispatchId");
    }
  }

  [Test]
  public async Task ChangeResultDispatchShouldFail()
  {
    if (RunTests)
    {
      await ResultTable.ChangeResultDispatch("SessionId",
                                             "NonExistingDispatchId",
                                             "NewDispatchId",
                                             CancellationToken.None);
      var result = await ResultTable.GetResult("SessionId",
                                               "ResultIsAvailable",
                                               CancellationToken.None);

      Assert.IsFalse(result.OriginDispatchId == "NewDispatchId");
    }
    //TODO: Write a test for the case in which a non existing Id is given.
  }

  [Test]
  public void ChangeResultDispatchShouldFailOnBadId()
  {
    if (RunTests)
    {
      Assert.ThrowsAsync<ArmoniKException>(async () =>
      {
        await ResultTable.ChangeResultDispatch("NonExistingSessionId",
                                               "DispatchId",
                                               "NewDispatchId",
                                               CancellationToken.None);
      });
    }
  }

  [Test]
  public async Task ChangeResultOwnershipShouldSucceed()
  {
    if (RunTests)
    {
      await ResultTable.ChangeResultOwnership("SessionId",
                                              new[] { "ResultIsAvailable" },
                                              "OwnerId",
                                              "NewOwnerId",
                                              CancellationToken.None);
      var result = await ResultTable.GetResult("SessionId",
                                               "ResultIsAvailable",
                                               CancellationToken.None);
      Assert.IsTrue(result.OwnerTaskId == "NewOwnerId");
    }
  }

  [Test]
  public async Task CreateShouldSucceed()
  {
    if (!RunTests)
      return;
    await ResultTable.Create(new[]
    {
      new Result("AnotherSessionId",
                 "Key",
                 "OwnerId",
                 "DispatchId",
                 true,
                 DateTime.Today,
                 new[] { (byte) 1 }),
    });

    var success = await ResultTable.GetResult("AnotherSessionId",
                                              "Key",
                                              CancellationToken.None);

    Assert.IsTrue(success.IsResultAvailable);
  }

  [Test]
  public void CreateShouldFail()
  {
    if (RunTests)
    {
      /* Check if an exception is thrown when attempting to
         create an already existing result entry */
      Assert.ThrowsAsync<ArmoniKException>(async () =>
      {
        await ResultTable.Create(new[]
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
  }

  [Test]
  public async Task DeleteResultsShouldRemoveAll()
  {
    if (RunTests)
    {
      await ResultTable.DeleteResults("SessionId",
                                      CancellationToken.None);

      var resList = ResultTable.ListResultsAsync("SessionId",
                                                 CancellationToken.None);

      // Query first element, function returns default if the list is empty
      var firstElement = await resList.FirstOrDefaultAsync();

      Assert.IsTrue(firstElement == default);
    }
  }

  [Test]
  public async Task DeleteResultShouldRemoveOne()
  {
    if (RunTests)
    {
      await ResultTable.DeleteResult("SessionId",
                                     "ResultIsAvailable",
                                     CancellationToken.None);

      Assert.ThrowsAsync<ArmoniKException>(async () =>
      {
        await ResultTable.GetResult("SessionId",
                                    "ResultIsAvailable",
                                    CancellationToken.None);
      });
    }
  }

  [Test]
  public async Task SetResultShouldSucceed()
  {
    if (RunTests)
    {
      await ResultTable.SetResult("SessionId",
                                  "OwnerId",
                                  "ResultIsNotAvailable",
                                  CancellationToken.None);

      var result = await ResultTable.GetResult("SessionId",
                                               "ResultIsNotAvailable",
                                               CancellationToken.None);

      Assert.IsTrue(result.IsResultAvailable);
    }
  }

  [Test]
  public async Task SetResultSmallPayloadShouldSucceed()
  {
    if (RunTests)
    {
      var smallPayload = new[] { (byte) (1), (byte) (2) };

      await ResultTable.SetResult("SessionId",
                                  "OwnerId",
                                  "ResultIsNotAvailable",
                                  smallPayload,
                                  CancellationToken.None);
      var result = await ResultTable.GetResult("SessionId",
                                               "ResultIsNotAvailable",
                                               CancellationToken.None);

      Assert.AreEqual(result.Data,
                      smallPayload);
    }
  }
}