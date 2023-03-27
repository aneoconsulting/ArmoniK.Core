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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Storage.Events;

using Microsoft.Extensions.Diagnostics.HealthChecks;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.TestBase;

[TestFixture]
public class ResultWatcherTestBase
{
  [SetUp]
  public async Task SetUp()
  {
    GetInstance();

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
                                           "OwnerId",
                                           ResultStatus.Completed,
                                           new List<string>(),
                                           DateTime.Today,
                                           new[]
                                           {
                                             (byte)1,
                                           }),
                                new Result("SessionId",
                                           "ResultIsNotAvailable",
                                           "OwnerId",
                                           ResultStatus.Aborted,
                                           new List<string>(),
                                           DateTime.Today,
                                           new[]
                                           {
                                             (byte)1,
                                           }),
                                new Result("SessionId",
                                           "ResultIsCreated",
                                           "OwnerId2",
                                           ResultStatus.Created,
                                           new List<string>(),
                                           DateTime.Today,
                                           new[]
                                           {
                                             (byte)1,
                                           }),
                                new Result("SessionId",
                                           "ResultIsCreated2",
                                           "OwnerId2",
                                           ResultStatus.Created,
                                           new List<string>(),
                                           DateTime.Today,
                                           new[]
                                           {
                                             (byte)1,
                                           }),
                                new Result("SessionId",
                                           "ResultIsCreated3",
                                           "OwnerId2",
                                           ResultStatus.Created,
                                           new List<string>(),
                                           DateTime.Today,
                                           new[]
                                           {
                                             (byte)1,
                                           }),
                                new Result("SessionId2",
                                           "ResultIsCreated3",
                                           "OwnerId3",
                                           ResultStatus.Created,
                                           new List<string>(),
                                           DateTime.Today,
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
    ResultTable   = null;
    ResultWatcher = null;
    RunTests      = false;
  }

  private static bool CheckForSkipSetup()
  {
    var category = TestContext.CurrentContext.Test.Properties.Get("Category") as string;
    return category is "SkipSetUp";
  }

  /* Interface to test */
  protected IResultTable?   ResultTable;
  protected IResultWatcher? ResultWatcher;

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
                         (await ResultWatcher!.Check(HealthCheckTag.Liveness)
                                              .ConfigureAwait(false)).Status);
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await ResultWatcher.Check(HealthCheckTag.Readiness)
                                             .ConfigureAwait(false)).Status);
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await ResultWatcher.Check(HealthCheckTag.Startup)
                                             .ConfigureAwait(false)).Status);

      await ResultWatcher.Init(CancellationToken.None)
                         .ConfigureAwait(false);

      Assert.AreEqual(HealthStatus.Healthy,
                      (await ResultWatcher.Check(HealthCheckTag.Liveness)
                                          .ConfigureAwait(false)).Status);
      Assert.AreEqual(HealthStatus.Healthy,
                      (await ResultWatcher.Check(HealthCheckTag.Readiness)
                                          .ConfigureAwait(false)).Status);
      Assert.AreEqual(HealthStatus.Healthy,
                      (await ResultWatcher.Check(HealthCheckTag.Startup)
                                          .ConfigureAwait(false)).Status);
    }
  }

  /// <summary>
  ///   This method produces the events (new results, owner id updates and status update)
  ///   that will be used to test the IResultWatcher interface.
  /// </summary>
  /// <param name="resultTable">Result table interface</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  private static async Task ProduceEvents(IResultTable      resultTable,
                                          CancellationToken cancellationToken)
  {
    await resultTable.Create(new[]
                             {
                               new Result("SessionId",
                                          "NewResult",
                                          "OwnerId",
                                          ResultStatus.Created,
                                          new List<string>(),
                                          DateTime.Today,
                                          new[]
                                          {
                                            (byte)1,
                                          }),
                             },
                             cancellationToken)
                     .ConfigureAwait(false);

    await resultTable.Create(new[]
                             {
                               new Result("SessionId",
                                          "NewResult2",
                                          "OwnerId",
                                          ResultStatus.Created,
                                          new List<string>(),
                                          DateTime.Today,
                                          new[]
                                          {
                                            (byte)1,
                                          }),
                               // we also create results in another session to check if the filter works
                               new Result("SessionId2",
                                          "NewResult2",
                                          "OwnerId",
                                          ResultStatus.Created,
                                          new List<string>(),
                                          DateTime.Today,
                                          new[]
                                          {
                                            (byte)1,
                                          }),
                             },
                             cancellationToken)
                     .ConfigureAwait(false);

    await resultTable.AbortTaskResults("SessionId",
                                       "OwnerId2",
                                       CancellationToken.None)
                     .ConfigureAwait(false);

    await resultTable.AbortTaskResults("SessionId2",
                                       "OwnerId3",
                                       CancellationToken.None)
                     .ConfigureAwait(false);

    await resultTable.ChangeResultOwnership("SessionId",
                                            "OwnerId2",
                                            new[]
                                            {
                                              new IResultTable.ChangeResultOwnershipRequest(new List<string>
                                                                                            {
                                                                                              "ResultIsCreated3",
                                                                                            },
                                                                                            "NewOwnerId"),
                                            },
                                            CancellationToken.None)
                     .ConfigureAwait(false);
  }

  [Test]
  public async Task WatchNewResultShouldSucceed()
  {
    if (RunTests)
    {
      var cts = new CancellationTokenSource();

      var watchEnumerator = await ResultWatcher!.GetNewResults("SessionId",
                                                               cts.Token)
                                                .ConfigureAwait(false);

      await ProduceEvents(ResultTable!,
                          cts.Token)
        .ConfigureAwait(false);

      cts.CancelAfter(TimeSpan.FromSeconds(1));

      var newResults = new List<NewResult>();
      Assert.ThrowsAsync<OperationCanceledException>(async () =>
                                                     {
                                                       await foreach (var cur in watchEnumerator.WithCancellation(cts.Token)
                                                                                                .ConfigureAwait(false))
                                                       {
                                                         Console.WriteLine(cur);
                                                         newResults.Add(cur);
                                                       }
                                                     });

      Assert.AreEqual(2,
                      newResults.Count);
    }
  }

  [Test]
  public async Task WatchResultStatusUpdateShouldSucceed()
  {
    if (RunTests)
    {
      var cts = new CancellationTokenSource();

      var watchEnumerator = await ResultWatcher!.GetResultStatusUpdates("SessionId",
                                                                        cts.Token)
                                                .ConfigureAwait(false);

      await ProduceEvents(ResultTable!,
                          cts.Token)
        .ConfigureAwait(false);

      cts.CancelAfter(TimeSpan.FromSeconds(2));

      var newResults = new List<ResultStatusUpdate>();
      Assert.ThrowsAsync<OperationCanceledException>(async () =>
                                                     {
                                                       await foreach (var cur in watchEnumerator.WithCancellation(cts.Token)
                                                                                                .ConfigureAwait(false))
                                                       {
                                                         Console.WriteLine(cur);
                                                         newResults.Add(cur);
                                                       }
                                                     });

      Assert.AreEqual(3,
                      newResults.Count);
    }
  }


  [Test]
  public async Task WatchResultOwnerUpdateShouldSucceed()
  {
    if (RunTests)
    {
      var cts = new CancellationTokenSource();

      var watchEnumerator = await ResultWatcher!.GetResultOwnerUpdates("SessionId",
                                                                       cts.Token)
                                                .ConfigureAwait(false);

      await ProduceEvents(ResultTable!,
                          cts.Token)
        .ConfigureAwait(false);

      cts.CancelAfter(TimeSpan.FromSeconds(2));

      var newResults = new List<ResultOwnerUpdate>();
      Assert.ThrowsAsync<OperationCanceledException>(async () =>
                                                     {
                                                       await foreach (var cur in watchEnumerator.WithCancellation(cts.Token)
                                                                                                .ConfigureAwait(false))
                                                       {
                                                         Console.WriteLine(cur);
                                                         newResults.Add(cur);
                                                       }
                                                     });

      Assert.AreEqual(1,
                      newResults.Count);

      Assert.AreEqual("NewOwnerId",
                      newResults.Single()
                                .NewOwner);
      Assert.AreEqual("ResultIsCreated3",
                      newResults.Single()
                                .ResultId);
    }
  }
}
