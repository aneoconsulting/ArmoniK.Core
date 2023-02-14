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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.TestBase;

[TestFixture]
public class ResultTableTestBase
{
  [SetUp]
  public void SetUp()
  {
    GetResultTableInstance();

    if (RunTests)
    {
      ResultTable!.Create(new[]
                          {
                            new Result("SessionId",
                                       "ResultIsAvailable",
                                       "OwnerId",
                                       ResultStatus.Completed,
                                       DateTime.Today,
                                       new[]
                                       {
                                         (byte)1,
                                       }),
                            new Result("SessionId",
                                       "ResultIsNotAvailable",
                                       "OwnerId",
                                       ResultStatus.Aborted,
                                       DateTime.Today,
                                       new[]
                                       {
                                         (byte)1,
                                       }),
                            new Result("SessionId",
                                       "ResultIsCreated",
                                       "OwnerId",
                                       ResultStatus.Created,
                                       DateTime.Today,
                                       new[]
                                       {
                                         (byte)1,
                                       }),
                            new Result("SessionId",
                                       "ResultIsCreated2",
                                       "OwnerId",
                                       ResultStatus.Created,
                                       DateTime.Today,
                                       new[]
                                       {
                                         (byte)1,
                                       }),
                          })
                  .Wait();
    }
  }

  [TearDown]
  public virtual void TearDown()
  {
    ResultTable = null;
    RunTests    = false;
  }

  /* Interface to test */
  protected IResultTable? ResultTable;

  /* Boolean to control that tests are executed in
   * an instance of this class */
  protected bool RunTests;

  /* Function be override so it returns the suitable instance
   * of ResultTable to the corresponding interface implementation */
  public virtual void GetResultTableInstance()
  {
  }

  [Test]
  public async Task InitShouldSucceed()
  {
    if (RunTests)
    {
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await ResultTable!.Check(HealthCheckTag.Liveness)
                                            .ConfigureAwait(false)).Status);
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await ResultTable.Check(HealthCheckTag.Readiness)
                                           .ConfigureAwait(false)).Status);
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await ResultTable.Check(HealthCheckTag.Startup)
                                           .ConfigureAwait(false)).Status);

      await ResultTable.Init(CancellationToken.None)
                       .ConfigureAwait(false);

      Assert.AreEqual(HealthStatus.Healthy,
                      (await ResultTable.Check(HealthCheckTag.Liveness)
                                        .ConfigureAwait(false)).Status);
      Assert.AreEqual(HealthStatus.Healthy,
                      (await ResultTable.Check(HealthCheckTag.Readiness)
                                        .ConfigureAwait(false)).Status);
      Assert.AreEqual(HealthStatus.Healthy,
                      (await ResultTable.Check(HealthCheckTag.Startup)
                                        .ConfigureAwait(false)).Status);
    }
  }


  [Test]
  public async Task ResultsAreAvailableShouldSucceed()
  {
    if (RunTests)
    {
      var checkTable = await ResultTable!.AreResultsAvailableAsync("SessionId",
                                                                   new[]
                                                                   {
                                                                     "ResultIsAvailable",
                                                                   },
                                                                   CancellationToken.None)
                                         .ConfigureAwait(false);
      Assert.AreEqual(1,
                      checkTable.Count(count => count.Status == ResultStatus.Completed));
    }
  }

  [Test]
  public async Task ResultsAreAvailableShouldReturnEmpty()
  {
    if (RunTests)
    {
      var checkTable = await ResultTable!.AreResultsAvailableAsync("SessionId",
                                                                   new[]
                                                                   {
                                                                     "ResultDoesNotExist",
                                                                   },
                                                                   CancellationToken.None)
                                         .ConfigureAwait(false);
      Assert.AreEqual(0,
                      checkTable.Count(count => count.Status == ResultStatus.Aborted));
    }
  }

  [Test]
  public async Task ResultsAreAvailableShouldReturnAborted()
  {
    if (RunTests)
    {
      var checkTable = await ResultTable!.AreResultsAvailableAsync("SessionId",
                                                                   new[]
                                                                   {
                                                                     "ResultIsNotAvailable",
                                                                   },
                                                                   CancellationToken.None)
                                         .ConfigureAwait(false);
      Assert.AreEqual(1,
                      checkTable.Count(count => count.Status == ResultStatus.Aborted));
    }
  }

  [Test]
  public async Task ChangeResultOwnershipShouldSucceed()
  {
    if (RunTests)
    {
      await ResultTable!.ChangeResultOwnership("SessionId",
                                               "OwnerId",
                                               new IResultTable.ChangeResultOwnershipRequest[]
                                               {
                                                 new(new[]
                                                     {
                                                       "ResultIsAvailable",
                                                     },
                                                     "NewOwnerId"),
                                               },
                                               CancellationToken.None)
                        .ConfigureAwait(false);
      var result = await ResultTable.GetResult("SessionId",
                                               "ResultIsAvailable",
                                               CancellationToken.None)
                                    .ConfigureAwait(false);
      Assert.IsTrue(result.OwnerTaskId == "NewOwnerId");
    }
  }

  [Test]
  public async Task CreateShouldSucceed()
  {
    if (!RunTests)
    {
      return;
    }

    await ResultTable!.Create(new[]
                              {
                                new Result("AnotherSessionId",
                                           "Key",
                                           "OwnerId",
                                           ResultStatus.Completed,
                                           DateTime.Today,
                                           new[]
                                           {
                                             (byte)1,
                                           }),
                              })
                      .ConfigureAwait(false);

    var result = await ResultTable.GetResult("AnotherSessionId",
                                             "Key",
                                             CancellationToken.None)
                                  .ConfigureAwait(false);

    Assert.IsTrue(result.Status == ResultStatus.Completed);
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
                                             await ResultTable!.Create(new[]
                                                                       {
                                                                         new Result("SessionId",
                                                                                    "ResultIsAvailable",
                                                                                    "",
                                                                                    ResultStatus.Unspecified,
                                                                                    DateTime.Today,
                                                                                    new[]
                                                                                    {
                                                                                      (byte)1,
                                                                                    }),
                                                                       })
                                                               .ConfigureAwait(false);
                                           });
    }
  }

  [Test]
  public async Task DeleteResultsShouldRemoveAll()
  {
    if (RunTests)
    {
      await ResultTable!.DeleteResults("SessionId",
                                       CancellationToken.None)
                        .ConfigureAwait(false);

      var resList = ResultTable.ListResultsAsync("SessionId",
                                                 CancellationToken.None);

      // Query first element, function returns default if the list is empty
      var firstElement = await resList.FirstOrDefaultAsync()
                                      .ConfigureAwait(false);

      Assert.IsTrue(firstElement == default);
    }
  }

  [Test]
  public async Task DeleteResultShouldRemoveOne()
  {
    if (RunTests)
    {
      await ResultTable!.DeleteResult("SessionId",
                                      "ResultIsAvailable",
                                      CancellationToken.None)
                        .ConfigureAwait(false);

      Assert.ThrowsAsync<ResultNotFoundException>(async () =>
                                                  {
                                                    await ResultTable.GetResult("SessionId",
                                                                                "ResultIsAvailable",
                                                                                CancellationToken.None)
                                                                     .ConfigureAwait(false);
                                                  });
    }
  }

  [Test]
  public void DeleteUnknownResultShouldFail()
  {
    if (RunTests)
    {
      Assert.ThrowsAsync<ResultNotFoundException>(async () =>
                                                  {
                                                    await ResultTable!.DeleteResult("SessionId",
                                                                                    "unknown",
                                                                                    CancellationToken.None)
                                                                      .ConfigureAwait(false);
                                                  });
    }
  }

  [Test]
  public async Task SetResultShouldSucceed()
  {
    if (RunTests)
    {
      await ResultTable!.SetResult("SessionId",
                                   "OwnerId",
                                   "ResultIsNotAvailable",
                                   CancellationToken.None)
                        .ConfigureAwait(false);

      var result = await ResultTable.GetResult("SessionId",
                                               "ResultIsNotAvailable",
                                               CancellationToken.None)
                                    .ConfigureAwait(false);

      Assert.IsTrue(result.Name == "ResultIsNotAvailable");
    }
  }

  [Test]
  public async Task SetResultSmallPayloadShouldSucceed()
  {
    if (RunTests)
    {
      var smallPayload = new[]
                         {
                           (byte)1,
                           (byte)2,
                         };

      await ResultTable!.SetResult("SessionId",
                                   "OwnerId",
                                   "ResultIsNotAvailable",
                                   smallPayload,
                                   CancellationToken.None)
                        .ConfigureAwait(false);
      var result = await ResultTable.GetResult("SessionId",
                                               "ResultIsNotAvailable",
                                               CancellationToken.None)
                                    .ConfigureAwait(false);

      Assert.AreEqual(result.Data,
                      smallPayload);
    }
  }

  [Test]
  public async Task GetResultsBench()
  {
    if (RunTests)
    {
      var n = 2000;
      var ids = Enumerable.Range(0,
                                 n)
                          .Select(i => $"ResultIsAvailable#{i}")
                          .ToArray();
      await ResultTable!.Create(ids.Select(id => new Result("SessionId",
                                                            id,
                                                            "OwnerId",
                                                            ResultStatus.Completed,
                                                            DateTime.Today,
                                                            Encoding.ASCII.GetBytes(id))))
                        .ConfigureAwait(false);
      var results = await ResultTable!.GetResults("SessionId",
                                                  ids,
                                                  CancellationToken.None)
                                      .ToListAsync()
                                      .ConfigureAwait(false);

      Assert.AreEqual(n,
                      results.Count);
      foreach (var result in results)
      {
        Assert.AreEqual(ResultStatus.Completed,
                        result.Status);
      }
    }
  }

  [Test]
  public async Task GetResultBench()
  {
    if (RunTests)
    {
      var n = 2000;
      var ids = Enumerable.Range(0,
                                 n)
                          .Select(i => $"ResultIsAvailable#{i}")
                          .ToArray();
      await ResultTable!.Create(ids.Select(id => new Result("SessionId",
                                                            id,
                                                            "OwnerId",
                                                            ResultStatus.Completed,
                                                            DateTime.Today,
                                                            Encoding.ASCII.GetBytes(id))))
                        .ConfigureAwait(false);

      List<Result> results = new(n);
      foreach (var id in ids)
      {
        results.Add(await ResultTable.GetResult("SessionId",
                                                id)
                                     .ConfigureAwait(false));
      }

      Assert.AreEqual(n,
                      results.Count);
      foreach (var result in results)
      {
        Assert.AreEqual(ResultStatus.Completed,
                        result.Status);
      }
    }
  }

  [Test]
  public async Task GetResultStatusShouldSucceed()
  {
    if (RunTests)
    {
      var result = (await ResultTable!.GetResultStatus(new[]
                                                       {
                                                         "ResultIsAvailable",
                                                         "ResultIsNotAvailable",
                                                         "ResultIsCreated",
                                                         "ResultDoesNotExist",
                                                       },
                                                       "SessionId",
                                                       CancellationToken.None)
                                      .ConfigureAwait(false)).ToList();

      Assert.Contains(new GetResultStatusReply.Types.IdStatus
                      {
                        Status   = ResultStatus.Completed,
                        ResultId = "ResultIsAvailable",
                      },
                      result);

      Assert.Contains(new GetResultStatusReply.Types.IdStatus
                      {
                        Status   = ResultStatus.Aborted,
                        ResultId = "ResultIsNotAvailable",
                      },
                      result);

      Assert.Contains(new GetResultStatusReply.Types.IdStatus
                      {
                        Status   = ResultStatus.Created,
                        ResultId = "ResultIsCreated",
                      },
                      result);
    }
  }

  [Test]
  public async Task GetNotExistingResultStatusShouldSucceed()
  {
    if (RunTests)
    {
      var result = await ResultTable!.GetResultStatus(new[]
                                                      {
                                                        "ResultDoesNotExist",
                                                      },
                                                      "SessionId",
                                                      CancellationToken.None)
                                     .ConfigureAwait(false);

      Assert.AreEqual(0,
                      result.Count());
    }
  }

  [Test]
  public async Task AbortResultsShouldSucceed()
  {
    if (RunTests)
    {
      await ResultTable!.AbortTaskResults("SessionId",
                                          "OwnerId",
                                          CancellationToken.None)
                        .ConfigureAwait(false);


      var resultStatus = (await ResultTable.GetResultStatus(new[]
                                                            {
                                                              "ResultIsAvailable",
                                                              "ResultIsNotAvailable",
                                                              "ResultIsCreated",
                                                            },
                                                            "SessionId",
                                                            CancellationToken.None)
                                           .ConfigureAwait(false)).ToList();

      Assert.AreEqual(3,
                      resultStatus.Count(status => status.Status == ResultStatus.Aborted));
      Assert.AreEqual(0,
                      resultStatus.Count(status => status.Status != ResultStatus.Aborted));
    }
  }

  [Test]
  public async Task AbortResultsShouldFail()
  {
    if (RunTests)
    {
      await ResultTable!.AbortTaskResults("SessionId",
                                          "TaskDoesNotExist",
                                          CancellationToken.None)
                        .ConfigureAwait(false);


      var resultStatus = (await ResultTable.GetResultStatus(new[]
                                                            {
                                                              "ResultIsAvailable",
                                                              "ResultIsNotAvailable",
                                                              "ResultIsCreated",
                                                            },
                                                            "SessionId",
                                                            CancellationToken.None)
                                           .ConfigureAwait(false)).ToList();

      Assert.AreEqual(1,
                      resultStatus.Count(status => status.Status == ResultStatus.Aborted));
      Assert.AreEqual(2,
                      resultStatus.Count(status => status.Status != ResultStatus.Aborted));
    }
  }

  [Test]
  public async Task ListResultsAsyncFilterResultStatusAndSessionIdShouldSucceed()
  {
    if (RunTests)
    {
      var res = (await ResultTable!.ListResultsAsync(result => result.Status == ResultStatus.Created && result.SessionId == "SessionId",
                                                     result => result.Status,
                                                     true,
                                                     0,
                                                     3,
                                                     CancellationToken.None)
                                   .ConfigureAwait(false)).results.ToList();

      Assert.AreEqual(2,
                      res.Count);
    }
  }

  [Test]
  public async Task ListResultsAsyncFilterResultStatusAndSessionIdLimit1ShouldSucceed()
  {
    if (RunTests)
    {
      var res = (await ResultTable!.ListResultsAsync(result => result.Status == ResultStatus.Created && result.SessionId == "SessionId",
                                                     result => result.Status,
                                                     true,
                                                     0,
                                                     1,
                                                     CancellationToken.None)
                                   .ConfigureAwait(false)).results.ToList();

      Assert.AreEqual(1,
                      res.Count);
    }
  }

  [Test]
  public async Task ListSessionAsyncNoFilterShouldSucceed()
  {
    if (RunTests)
    {
      var res = (await ResultTable!.ListResultsAsync(result => true,
                                                     result => result.Status,
                                                     true,
                                                     0,
                                                     4,
                                                     CancellationToken.None)
                                   .ConfigureAwait(false)).results.ToList();

      Assert.AreEqual(4,
                      res.Count);
    }
  }
}
