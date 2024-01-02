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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Sessions;
using ArmoniK.Api.gRPC.V1.SortDirection;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.gRPC.Convertors;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.TestBase;

[TestFixture]
public class SessionTableTestBase
{
  [SetUp]
  public async Task SetUp()
  {
    GetSessionTableInstance();

    if (!RunTests || CheckForSkipSetup())
    {
      return;
    }

    await SessionTable!.Init(CancellationToken.None)
                       .ConfigureAwait(false);

    rootSessionId1_ = await SessionTable!.SetSessionDataAsync(new[]
                                                              {
                                                                "part1",
                                                                "part2",
                                                              },
                                                              Options,
                                                              CancellationToken.None)
                                         .ConfigureAwait(false);

    await SessionTable!.SetSessionDataAsync(new[]
                                            {
                                              "part1",
                                              "part2",
                                            },
                                            Options,
                                            CancellationToken.None)
                       .ConfigureAwait(false);

    rootSessionId2_ = await SessionTable!.SetSessionDataAsync(new[]
                                                              {
                                                                "part1",
                                                                "part2",
                                                              },
                                                              Options,
                                                              CancellationToken.None)
                                         .ConfigureAwait(false);

    await SessionTable!.SetSessionDataAsync(new[]
                                            {
                                              "part1",
                                              "part2",
                                            },
                                            Options with
                                            {
                                              ApplicationName = "ApplicationName2",
                                              ApplicationVersion = "ApplicationVersion2",
                                            },
                                            CancellationToken.None)
                       .ConfigureAwait(false);

    await SessionTable.CancelSessionAsync(rootSessionId2_,
                                          CancellationToken.None)
                      .ConfigureAwait(false);
  }

  [TearDown]
  public virtual void TearDown()
  {
    SessionTable = null;
    RunTests     = false;
  }

  private static readonly TaskOptions Options = new(new Dictionary<string, string>
                                                    {
                                                      {
                                                        "key1", "val1"
                                                      },
                                                    },
                                                    TimeSpan.FromMinutes(1),
                                                    2,
                                                    1,
                                                    "part1",
                                                    "ApplicationName",
                                                    "ApplicationVersion",
                                                    "",
                                                    "",
                                                    "");

  private static bool CheckForSkipSetup()
  {
    var category = TestContext.CurrentContext.Test.Properties.Get("Category") as string;
    return category is "SkipSetUp";
  }

  protected ISessionTable? SessionTable;

  protected bool RunTests;

  private string? rootSessionId1_;
  private string? rootSessionId2_;

  public virtual void GetSessionTableInstance()
  {
  }

  [Test]
  [Category("SkipSetUp")]
  public async Task InitShouldSucceed()
  {
    if (RunTests)
    {
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await SessionTable!.Check(HealthCheckTag.Liveness)
                                             .ConfigureAwait(false)).Status);
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await SessionTable.Check(HealthCheckTag.Readiness)
                                            .ConfigureAwait(false)).Status);
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await SessionTable.Check(HealthCheckTag.Startup)
                                            .ConfigureAwait(false)).Status);

      await SessionTable.Init(CancellationToken.None)
                        .ConfigureAwait(false);

      Assert.AreEqual(HealthStatus.Healthy,
                      (await SessionTable.Check(HealthCheckTag.Liveness)
                                         .ConfigureAwait(false)).Status);
      Assert.AreEqual(HealthStatus.Healthy,
                      (await SessionTable.Check(HealthCheckTag.Readiness)
                                         .ConfigureAwait(false)).Status);
      Assert.AreEqual(HealthStatus.Healthy,
                      (await SessionTable.Check(HealthCheckTag.Startup)
                                         .ConfigureAwait(false)).Status);
    }
  }

  [Test]
  public async Task IsSessionCancelledAsyncShouldSucceed()
  {
    if (RunTests)
    {
      var res = await SessionTable!.IsSessionCancelledAsync(rootSessionId1_!,
                                                            CancellationToken.None)
                                   .ConfigureAwait(false);
      Assert.IsFalse(res);
    }
  }

  [Test]
  public void IsSessionCancelledAsyncShouldFail()
  {
    if (RunTests)
    {
      Assert.ThrowsAsync<SessionNotFoundException>(async () =>
                                                   {
                                                     await SessionTable!.IsSessionCancelledAsync("BadSessionId",
                                                                                                 CancellationToken.None)
                                                                        .ConfigureAwait(false);
                                                   });
    }
  }

  [Test]
  public async Task GetDefaultTaskOptionAsyncShouldSucceed()
  {
    if (RunTests)
    {
      var res = await SessionTable!.GetDefaultTaskOptionAsync(rootSessionId1_!,
                                                              CancellationToken.None)
                                   .ConfigureAwait(false);
      Assert.NotNull(res);
    }
  }

  [Test]
  public void GetDefaultTaskOptionAsyncShouldFail()
  {
    if (RunTests)
    {
      Assert.ThrowsAsync<SessionNotFoundException>(async () =>
                                                   {
                                                     await SessionTable!.GetDefaultTaskOptionAsync("BadSessionId",
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
      var sessionData = await SessionTable!.CancelSessionAsync(rootSessionId1_!,
                                                               CancellationToken.None)
                                           .ConfigureAwait(false);

      Assert.AreEqual(SessionStatus.Cancelled,
                      sessionData.Status);

      var wasSessionCanceled = await SessionTable.IsSessionCancelledAsync(rootSessionId1_!,
                                                                          CancellationToken.None)
                                                 .ConfigureAwait(false);
      Assert.IsTrue(wasSessionCanceled);
    }
  }

  [Test]
  public void CancelSessionAsyncShouldFail()
  {
    if (RunTests)
    {
      Assert.ThrowsAsync<SessionNotFoundException>(async () =>
                                                   {
                                                     await SessionTable!.CancelSessionAsync("BadSessionId",
                                                                                            CancellationToken.None)
                                                                        .ConfigureAwait(false);
                                                   });
    }
  }

  [Test]
  public async Task CancelCancelledSessionAsyncShouldFail()
  {
    if (RunTests)
    {
      await SessionTable!.CancelSessionAsync(rootSessionId1_!,
                                             CancellationToken.None)
                         .ConfigureAwait(false);

      Assert.ThrowsAsync<SessionNotFoundException>(async () =>
                                                   {
                                                     await SessionTable.CancelSessionAsync(rootSessionId1_!,
                                                                                           CancellationToken.None)
                                                                       .ConfigureAwait(false);
                                                   });
    }
  }


  [Test]
  public async Task DeleteSessionAsyncShouldSucceed()
  {
    if (RunTests)
    {
      var res = SessionTable!.DeleteSessionAsync(rootSessionId1_!,
                                                 CancellationToken.None);
      await res.ConfigureAwait(false);

      Assert.IsTrue(res.IsCompletedSuccessfully);
    }
  }

  [Test]
  public void DeleteSessionAsyncShouldFail()
  {
    if (RunTests)
    {
      Assert.ThrowsAsync<SessionNotFoundException>(async () =>
                                                   {
                                                     await SessionTable!.DeleteSessionAsync("BadSessionId",
                                                                                            CancellationToken.None)
                                                                        .ConfigureAwait(false);
                                                   });
    }
  }

  [Test]
  public async Task ListSessionAsyncShouldSucceed()
  {
    if (RunTests)
    {
      var res = await SessionTable!.ListSessionsAsync(new SessionFilter
                                                      {
                                                        Included = new SessionFilter.Types.StatusesRequest
                                                                   {
                                                                     Statuses =
                                                                     {
                                                                       Api.gRPC.V1.SessionStatus.Running,
                                                                     },
                                                                   },
                                                      },
                                                      CancellationToken.None)
                                   .ToListAsync()
                                   .ConfigureAwait(false);

      Assert.AreEqual(3,
                      res.Count);
    }
  }

  [Test]
  public async Task ListSessionAsyncShouldSucceed2()
  {
    if (RunTests)
    {
      var res = await SessionTable!.ListSessionsAsync(new SessionFilter
                                                      {
                                                        Sessions =
                                                        {
                                                          rootSessionId1_!,
                                                        },
                                                      },
                                                      CancellationToken.None)
                                   .ToListAsync()
                                   .ConfigureAwait(false);

      Assert.AreEqual(1,
                      res.Count);
    }
  }

  [Test]
  public async Task ListSessionAsyncShouldFail()
  {
    if (RunTests)
    {
      var res = await SessionTable!.ListSessionsAsync(new SessionFilter
                                                      {
                                                        Sessions =
                                                        {
                                                          "SessionIdDoesNotExist",
                                                        },
                                                      },
                                                      CancellationToken.None)
                                   .ToListAsync()
                                   .ConfigureAwait(false);

      Assert.AreEqual(0,
                      res.Count);
    }
  }

  [Test]
  public async Task ListSessionAsyncFilterApplicationNameShouldSucceed()
  {
    if (RunTests)
    {
      var res = (await SessionTable!.ListSessionsAsync(data => data.Options.ApplicationName == "ApplicationName",
                                                       data => data.Status,
                                                       true,
                                                       0,
                                                       3,
                                                       CancellationToken.None)
                                    .ConfigureAwait(false)).sessions.ToList();

      Assert.AreEqual(3,
                      res.Count);
    }
  }

  [Test]
  public async Task ListSessionAsyncFilterApplicationNameAndSessionIdShouldSucceed()
  {
    if (RunTests)
    {
      var res = (await SessionTable!.ListSessionsAsync(data => data.Options.ApplicationName == "ApplicationName" && data.SessionId == rootSessionId1_!,
                                                       data => data.Status,
                                                       true,
                                                       0,
                                                       3,
                                                       CancellationToken.None)
                                    .ConfigureAwait(false)).sessions.ToList();


      Assert.AreEqual(1,
                      res.Count);
    }
  }

  [Test]
  public async Task ListSessionAsyncFilterApplicationNameAndStatusShouldSucceed()
  {
    if (RunTests)
    {
      var res = (await SessionTable!.ListSessionsAsync(data => data.Options.ApplicationName == "ApplicationName" && data.Status == SessionStatus.Running,
                                                       data => data.Status,
                                                       true,
                                                       0,
                                                       3,
                                                       CancellationToken.None)
                                    .ConfigureAwait(false)).sessions.ToList();

      Assert.AreEqual(2,
                      res.Count);
    }
  }

  [Test]
  public async Task ListSessionAsyncNoFilterShouldSucceed()
  {
    if (RunTests)
    {
      var res = (await SessionTable!.ListSessionsAsync(data => true,
                                                       data => data.Status,
                                                       true,
                                                       0,
                                                       3,
                                                       CancellationToken.None)
                                    .ConfigureAwait(false)).sessions.ToList();

      Assert.AreEqual(3,
                      res.Count);
    }
  }


  [Test]
  public async Task ListSessionAsyncTaskOptionsOptions()
  {
    if (RunTests)
    {
      var req = new ListSessionsRequest
                {
                  Sort = new ListSessionsRequest.Types.Sort
                         {
                           Direction = SortDirection.Asc,
                           Field = new SessionField
                                   {
                                     TaskOptionGenericField = new TaskOptionGenericField
                                                              {
                                                                Field = "key1",
                                                              },
                                   },
                         },
                };

      var res = (await SessionTable!.ListSessionsAsync(data => true,
                                                       req.Sort.ToField(),
                                                       false,
                                                       0,
                                                       3,
                                                       CancellationToken.None)
                                    .ConfigureAwait(false)).sessions.ToList();

      Assert.AreEqual(3,
                      res.Count);
    }
  }
}
