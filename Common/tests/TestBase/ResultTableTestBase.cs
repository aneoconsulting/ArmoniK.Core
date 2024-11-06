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
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Base.Exceptions;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Utils;

using Microsoft.Extensions.Diagnostics.HealthChecks;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.TestBase;

[TestFixture]
public class ResultTableTestBase
{
  [SetUp]
  public async Task SetUp()
  {
    GetResultTableInstance();

    if (!RunTests || CheckForSkipSetup())
    {
      return;
    }

    await ResultTable!.Init(CancellationToken.None)
                      .ConfigureAwait(false);

    await ResultTable!.Create(new[]
                              {
                                new Result("SessionId",
                                           "ResultIsAvailable",
                                           "",
                                           "CreatedBy",
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
                                           "CreatedBy",
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
                                           "CreatedBy",
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
                                           "CreatedBy",
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
                                           "CreatedBy",
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
  }

  [TearDown]
  public virtual void TearDown()
  {
    ResultTable = null;
    RunTests    = false;
  }

  private static bool CheckForSkipSetup()
  {
    var category = TestContext.CurrentContext.Test.Properties.Get("Category") as string;
    return category is "SkipSetUp";
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
  [Category("SkipSetUp")]
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
  public async Task ChangeResultOwnershipShouldSucceed()
  {
    if (RunTests)
    {
      await ResultTable!.ChangeResultOwnership("OwnerId",
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
      var result = await ResultTable.GetResult("ResultIsAvailable",
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
                                           "",
                                           "CreatedBy",
                                           "OwnerId",
                                           ResultStatus.Completed,
                                           new List<string>(),
                                           DateTime.Today,
                                           1,
                                           new[]
                                           {
                                             (byte)1,
                                           }),
                              })
                      .ConfigureAwait(false);

    var result = await ResultTable.GetResult("Key",
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
                                                                                    "",
                                                                                    "",
                                                                                    ResultStatus.Unspecified,
                                                                                    new List<string>(),
                                                                                    DateTime.Today,
                                                                                    1,
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

      var resList = await ResultTable.GetResults(result => result.SessionId == "SessionId",
                                                 result => result.ResultId,
                                                 CancellationToken.None)
                                     .ToListAsync()
                                     .ConfigureAwait(false);

      Assert.IsEmpty(resList);
    }
  }

  [Test]
  public async Task DeleteResultShouldRemoveOne()
  {
    if (RunTests)
    {
      await ResultTable!.DeleteResult("ResultIsAvailable",
                                      CancellationToken.None)
                        .ConfigureAwait(false);

      Assert.ThrowsAsync<ResultNotFoundException>(async () =>
                                                  {
                                                    await ResultTable.GetResult("ResultIsAvailable",
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
                                                    await ResultTable!.DeleteResult("unknown",
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
      var id = Encoding.UTF8.GetBytes("OpaqueId");
      await ResultTable!.SetResult("SessionId",
                                   "OwnerId",
                                   "ResultIsNotAvailable",
                                   5,
                                   id,
                                   CancellationToken.None)
                        .ConfigureAwait(false);

      var result = await ResultTable!.GetResult("ResultIsNotAvailable",
                                                CancellationToken.None)
                                     .ConfigureAwait(false);

      Assert.AreEqual("ResultIsNotAvailable",
                      result.ResultId);
      Assert.AreEqual(5,
                      result.Size);
      Assert.AreEqual(id,
                      result.OpaqueId);
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
                                                            "",
                                                            "CreatedBy",
                                                            "OwnerId",
                                                            ResultStatus.Completed,
                                                            new List<string>(),
                                                            DateTime.Today,
                                                            id.Length,
                                                            Encoding.ASCII.GetBytes(id)))
                                   .AsICollection())
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
                                                            "",
                                                            "CreatedBy",
                                                            "OwnerId",
                                                            ResultStatus.Completed,
                                                            new List<string>(),
                                                            DateTime.Today,
                                                            id.Length,
                                                            Encoding.ASCII.GetBytes(id)))
                                   .AsICollection())
                        .ConfigureAwait(false);

      List<Result> results = new(n);
      foreach (var id in ids)
      {
        results.Add(await ResultTable.GetResult(id)
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

      Assert.Contains(new ResultIdStatus("ResultIsAvailable",
                                         ResultStatus.Completed),
                      result);

      Assert.Contains(new ResultIdStatus("ResultIsNotAvailable",
                                         ResultStatus.Aborted),
                      result);

      Assert.Contains(new ResultIdStatus("ResultIsCreated",
                                         ResultStatus.Created),
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


      var resultStatus = (await ResultTable!.GetResultStatus(new[]
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


      var resultStatus = (await ResultTable!.GetResultStatus(new[]
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

  [Test]
  public async Task GetDependentsShouldSucceed()
  {
    if (RunTests)
    {
      var dependents = await ResultTable!.GetDependents("SessionId",
                                                        "ResultIsCompletedWithDependents")
                                         .ToListAsync()
                                         .ConfigureAwait(false);
      Assert.AreEqual(2,
                      dependents.Count);
      Assert.Contains("Dependent1",
                      dependents);
      Assert.Contains("Dependent2",
                      dependents);
    }
  }

  [Test]
  public async Task GetEmptyDependentsShouldSucceed()
  {
    if (RunTests)
    {
      var dependents = await ResultTable!.GetDependents("SessionId",
                                                        "ResultIsCreated")
                                         .ToListAsync()
                                         .ConfigureAwait(false);
      Assert.AreEqual(0,
                      dependents.Count);
    }
  }

  [Test]
  public void GetDependentsShouldThrow()
  {
    if (RunTests)
    {
      Assert.ThrowsAsync<ResultNotFoundException>(async () => await ResultTable!.GetDependents("SessionId",
                                                                                               "ResultDoesNotExists")
                                                                                .ConfigureAwait(false));
    }
  }

  [Test]
  public async Task AddDependentShouldSucceed()
  {
    if (RunTests)
    {
      var resultId  = "ResultToAddDependents";
      var sessionId = "SessionId";

      await ResultTable!.Create(new[]
                                {
                                  new Result(sessionId,
                                             resultId,
                                             "",
                                             "CreatedBy",
                                             "OwnerTask",
                                             ResultStatus.Created,
                                             new List<string>(),
                                             DateTime.UtcNow,
                                             0,
                                             Array.Empty<byte>()),
                                })
                        .ConfigureAwait(false);


      await ResultTable.AddTaskDependencies(new Dictionary<string, ICollection<string>>
                                            {
                                              {
                                                resultId, new[]
                                                          {
                                                            "Task1",
                                                            "Task2",
                                                          }
                                              },
                                            })
                       .ConfigureAwait(false);

      var dependents = await ResultTable.GetDependents(sessionId,
                                                       resultId)
                                        .ToListAsync()
                                        .ConfigureAwait(false);
      Assert.AreEqual(2,
                      dependents.Count);
      Assert.Contains("Task1",
                      dependents);
      Assert.Contains("Task2",
                      dependents);
    }
  }

  [Test]
  public void AddDependentNotExistingResultShouldThrow()
  {
    if (RunTests)
    {
      Assert.ThrowsAsync<ResultNotFoundException>(async () => await ResultTable!.AddTaskDependencies(new Dictionary<string, ICollection<string>>
                                                                                                     {
                                                                                                       {
                                                                                                         "resultDoesNotExist", new[]
                                                                                                                               {
                                                                                                                                 "Task1",
                                                                                                                                 "Task2",
                                                                                                                               }
                                                                                                       },
                                                                                                     })
                                                                                .ConfigureAwait(false));
    }
  }

  [Test]
  public async Task SetTaskOwnershipShouldSucceed()
  {
    if (RunTests)
    {
      await ResultTable!.SetTaskOwnership(new[]
                                          {
                                            ("ResultIsCreated2", "NewTaskId"),
                                          })
                        .ConfigureAwait(false);

      var res = await ResultTable.GetResult("ResultIsCreated2")
                                 .ConfigureAwait(false);

      Assert.AreEqual("NewTaskId",
                      res.OwnerTaskId);

      // Overriding an existing result should succeed
      await ResultTable!.SetTaskOwnership(new[]
                                          {
                                            ("ResultIsCreated2", "NewTaskId2"),
                                          })
                        .ConfigureAwait(false);

      res = await ResultTable.GetResult("ResultIsCreated2")
                             .ConfigureAwait(false);

      Assert.AreEqual("NewTaskId2",
                      res.OwnerTaskId);
    }
  }

  [Test]
  public void SetTaskOwnershipShouldFail()
  {
    if (RunTests)
    {
      Assert.ThrowsAsync<ResultNotFoundException>(async () => await ResultTable!.SetTaskOwnership(new[]
                                                                                                  {
                                                                                                    ("ResultDoesNotExist", "NewTaskId"),
                                                                                                  })
                                                                                .ConfigureAwait(false));
    }
  }

  [Test]
  public async Task CompleteResultShouldSucceed()
  {
    if (RunTests)
    {
      var resultId = Guid.NewGuid()
                         .ToString();
      var sessionId = Guid.NewGuid()
                          .ToString();
      var id = Encoding.UTF8.GetBytes("OpaqueId");
      await ResultTable!.Create(new List<Result>
                                {
                                  new(sessionId,
                                      resultId,
                                      "Name",
                                      "",
                                      "",
                                      ResultStatus.Created,
                                      new List<string>(),
                                      DateTime.UtcNow,
                                      0,
                                      Array.Empty<byte>()),
                                },
                                CancellationToken.None)
                        .ConfigureAwait(false);

      var result = await ResultTable.CompleteResult(sessionId,
                                                    resultId,
                                                    5,
                                                    id,
                                                    CancellationToken.None)
                                    .ConfigureAwait(false);

      Assert.AreEqual(ResultStatus.Completed,
                      result.Status);
      Assert.AreEqual(id,
                      result.OpaqueId);
      Assert.AreEqual(5,
                      result.Size);

      result = await ResultTable.GetResult(resultId,
                                           CancellationToken.None)
                                .ConfigureAwait(false);

      Assert.AreEqual(ResultStatus.Completed,
                      result.Status);
      Assert.AreEqual(5,
                      result.Size);
      Assert.AreEqual(id,
                      result.OpaqueId);
    }
  }

  [Test]
  public void CompleteResultShouldThrow()
  {
    if (RunTests)
    {
      var id = Encoding.UTF8.GetBytes("OpaqueId");
      Assert.ThrowsAsync<ResultNotFoundException>(async () => await ResultTable!.CompleteResult("SessionId",
                                                                                                "NotExistingResult111",
                                                                                                5,
                                                                                                id,
                                                                                                CancellationToken.None)
                                                                                .ConfigureAwait(false));
    }
  }
}
