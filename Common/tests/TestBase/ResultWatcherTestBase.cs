// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base.DataStructures;
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
                                           "",
                                           "CreatedBy",
                                           "CompletedBy",
                                           "OwnerId",
                                           ResultStatus.Completed,
                                           new List<string>(),
                                           DateTime.Today,
                                           DateTime.Today,
                                           1,
                                           new[]
                                           {
                                             (byte)1,
                                           },
                                           false),
                                new Result("SessionId",
                                           "ResultIsNotAvailable",
                                           "",
                                           "CreatedBy",
                                           "CompletedBy",
                                           "OwnerId",
                                           ResultStatus.Aborted,
                                           new List<string>(),
                                           DateTime.Today,
                                           DateTime.Today,
                                           1,
                                           new[]
                                           {
                                             (byte)1,
                                           },
                                           false),
                                new Result("SessionId",
                                           "ResultIsCreated",
                                           "",
                                           "CreatedBy2",
                                           "CompletedBy2",
                                           "OwnerId2",
                                           ResultStatus.Created,
                                           new List<string>(),
                                           DateTime.Today,
                                           null,
                                           1,
                                           new[]
                                           {
                                             (byte)1,
                                           },
                                           false),
                                new Result("SessionId",
                                           "ResultIsCreated2",
                                           "",
                                           "CreatedBy2",
                                           "CompletedBy2",
                                           "OwnerId2",
                                           ResultStatus.Created,
                                           new List<string>(),
                                           DateTime.Today,
                                           null,
                                           1,
                                           new[]
                                           {
                                             (byte)1,
                                           },
                                           false),
                                new Result("SessionId",
                                           "ResultIsCreated3",
                                           "",
                                           "CreatedBy2",
                                           "CompletedBy3",
                                           "OwnerId2",
                                           ResultStatus.Created,
                                           new List<string>(),
                                           DateTime.Today,
                                           null,
                                           1,
                                           new[]
                                           {
                                             (byte)1,
                                           },
                                           false),
                                new Result("SessionId2",
                                           "ResultIsCreated4",
                                           "",
                                           "CreatedBy3",
                                           "CompletedBy3",
                                           "OwnerId3",
                                           ResultStatus.Created,
                                           new List<string>(),
                                           DateTime.Today,
                                           null,
                                           1,
                                           new[]
                                           {
                                             (byte)1,
                                           },
                                           false),
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
      Assert.That((await ResultWatcher!.Check(HealthCheckTag.Liveness)
                                       .ConfigureAwait(false)).Status,
                  Is.Not.EqualTo(HealthStatus.Healthy));
      Assert.That((await ResultWatcher.Check(HealthCheckTag.Readiness)
                                      .ConfigureAwait(false)).Status,
                  Is.Not.EqualTo(HealthStatus.Healthy));
      Assert.That((await ResultWatcher.Check(HealthCheckTag.Startup)
                                      .ConfigureAwait(false)).Status,
                  Is.Not.EqualTo(HealthStatus.Healthy));

      await ResultWatcher.Init(CancellationToken.None)
                         .ConfigureAwait(false);

      Assert.That((await ResultWatcher.Check(HealthCheckTag.Liveness)
                                      .ConfigureAwait(false)).Status,
                  Is.EqualTo(HealthStatus.Healthy));
      Assert.That((await ResultWatcher.Check(HealthCheckTag.Readiness)
                                      .ConfigureAwait(false)).Status,
                  Is.EqualTo(HealthStatus.Healthy));
      Assert.That((await ResultWatcher.Check(HealthCheckTag.Startup)
                                      .ConfigureAwait(false)).Status,
                  Is.EqualTo(HealthStatus.Healthy));
    }
  }

  private static readonly Result NewResult1 = new("SessionId",
                                                  "NewResult",
                                                  "",
                                                  "CreatedBy",
                                                  "CompletedBy",
                                                  "OwnerId",
                                                  ResultStatus.Created,
                                                  new List<string>(),
                                                  DateTime.Today,
                                                  null,
                                                  1,
                                                  new[]
                                                  {
                                                    (byte)1,
                                                  },
                                                  false);

  private static readonly Result NewResult2 = new("SessionId",
                                                  "NewResult2",
                                                  "",
                                                  "CreatedBy",
                                                  "CompletedBy",
                                                  "OwnerId",
                                                  ResultStatus.Created,
                                                  new List<string>(),
                                                  DateTime.Today,
                                                  null,
                                                  1,
                                                  new[]
                                                  {
                                                    (byte)1,
                                                  },
                                                  false);

  private static readonly Result NewResult3 = new("SessionId2",
                                                  "NewResult3",
                                                  "",
                                                  "CreatedBy",
                                                  "CompletedBy",
                                                  "OwnerId",
                                                  ResultStatus.Created,
                                                  new List<string>(),
                                                  DateTime.Today,
                                                  null,
                                                  1,
                                                  new[]
                                                  {
                                                    (byte)1,
                                                  },
                                                  false);

  private NewResult ResultToNewResult(Result result)
    => new(result.SessionId,
           result.ResultId,
           result.OwnerTaskId,
           result.Status);

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
                               NewResult1,
                             },
                             cancellationToken)
                     .ConfigureAwait(false);

    await resultTable.Create(new[]
                             {
                               NewResult2,
                               // we also create results in another session to check if the filter works
                               NewResult3,
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

    await resultTable.ChangeResultOwnership("OwnerId2",
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

      var watchEnumerator = await ResultWatcher!.GetNewResults(result => result.SessionId == "SessionId",
                                                               cts.Token)
                                                .ConfigureAwait(false);

      var newResults = new List<NewResult>();
      var watch = Task.Run(async () =>
                           {
                             await foreach (var cur in watchEnumerator.WithCancellation(cts.Token)
                                                                      .ConfigureAwait(false))
                             {
                               Console.WriteLine(cur);
                               newResults.Add(cur);
                             }
                           },
                           CancellationToken.None);

      await Task.Delay(TimeSpan.FromMilliseconds(20),
                       CancellationToken.None)
                .ConfigureAwait(false);

      await ProduceEvents(ResultTable!,
                          cts.Token)
        .ConfigureAwait(false);

      cts.CancelAfter(TimeSpan.FromMilliseconds(100));

      Assert.That(() => watch,
                  Throws.InstanceOf<OperationCanceledException>());

      Assert.That(newResults,
                  Is.EquivalentTo(new List<NewResult>
                                  {
                                    ResultToNewResult(NewResult1),
                                    ResultToNewResult(NewResult2),
                                  }));
    }
  }

  [Test]
  public async Task WatchResultStatusUpdateShouldSucceed()
  {
    if (RunTests)
    {
      var cts = new CancellationTokenSource();

      var watchEnumerator = await ResultWatcher!.GetResultStatusUpdates(result => result.SessionId == "SessionId",
                                                                        cts.Token)
                                                .ConfigureAwait(false);

      var newResults = new List<ResultStatusUpdate>();
      var watch = Task.Run(async () =>
                           {
                             await foreach (var cur in watchEnumerator.WithCancellation(cts.Token)
                                                                      .ConfigureAwait(false))
                             {
                               Console.WriteLine(cur);
                               newResults.Add(cur);
                             }
                           },
                           CancellationToken.None);

      await Task.Delay(TimeSpan.FromMilliseconds(20),
                       CancellationToken.None)
                .ConfigureAwait(false);

      await ProduceEvents(ResultTable!,
                          cts.Token)
        .ConfigureAwait(false);

      cts.CancelAfter(TimeSpan.FromMilliseconds(100));

      Assert.That(() => watch,
                  Throws.InstanceOf<OperationCanceledException>());

      Assert.That(newResults,
                  Is.EqualTo(new List<ResultStatusUpdate>
                             {
                               new("SessionId",
                                   "ResultIsCreated",
                                   ResultStatus.Aborted),
                               new("SessionId",
                                   "ResultIsCreated2",
                                   ResultStatus.Aborted),
                               new("SessionId",
                                   "ResultIsCreated3",
                                   ResultStatus.Aborted),
                             }));
    }
  }


  [Test]
  public async Task WatchResultOwnerUpdateShouldSucceed()
  {
    if (RunTests)
    {
      var cts = new CancellationTokenSource();

      var watchEnumerator = await ResultWatcher!.GetResultOwnerUpdates(result => result.SessionId == "SessionId",
                                                                       cts.Token)
                                                .ConfigureAwait(false);

      var newResults = new List<ResultOwnerUpdate>();
      var watch = Task.Run(async () =>
                           {
                             await foreach (var cur in watchEnumerator.WithCancellation(cts.Token)
                                                                      .ConfigureAwait(false))
                             {
                               Console.WriteLine(cur);
                               newResults.Add(cur);
                             }
                           },
                           CancellationToken.None);

      await Task.Delay(TimeSpan.FromMilliseconds(20),
                       CancellationToken.None)
                .ConfigureAwait(false);

      await ProduceEvents(ResultTable!,
                          cts.Token)
        .ConfigureAwait(false);

      cts.CancelAfter(TimeSpan.FromMilliseconds(100));

      Assert.That(() => watch,
                  Throws.InstanceOf<OperationCanceledException>());

      Assert.That(newResults,
                  Is.EqualTo(new List<ResultOwnerUpdate>
                             {
                               new("SessionId",
                                   "ResultIsCreated3",
                                   "",
                                   "NewOwnerId"),
                             }));
    }
  }
}
