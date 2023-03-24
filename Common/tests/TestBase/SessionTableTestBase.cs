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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Base;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using Google.Protobuf.WellKnownTypes;

using Microsoft.Extensions.Diagnostics.HealthChecks;

using NUnit.Framework;

using TaskOptions = ArmoniK.Api.gRPC.V1.TaskOptions;

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

    rootSessionId_ = await SessionTable!.SetSessionDataAsync(new[]
                                                             {
                                                               "part1",
                                                               "part2",
                                                             },
                                                             new TaskOptions
                                                             {
                                                               MaxDuration        = Duration.FromTimeSpan(TimeSpan.FromMinutes(1)),
                                                               MaxRetries         = 2,
                                                               Priority           = 1,
                                                               PartitionId        = "part1",
                                                               ApplicationName    = "ApplicationName",
                                                               ApplicationVersion = "ApplicationVersion",
                                                             },
                                                             CancellationToken.None)
                                        .ConfigureAwait(false);

    rootSessionId2_ = await SessionTable!.SetSessionDataAsync(new[]
                                                              {
                                                                "part1",
                                                                "part2",
                                                              },
                                                              new TaskOptions
                                                              {
                                                                MaxDuration        = Duration.FromTimeSpan(TimeSpan.FromMinutes(1)),
                                                                MaxRetries         = 2,
                                                                Priority           = 1,
                                                                PartitionId        = "part1",
                                                                ApplicationName    = "ApplicationName",
                                                                ApplicationVersion = "ApplicationVersion",
                                                              },
                                                              CancellationToken.None)
                                         .ConfigureAwait(false);

    rootSessionId3_ = await SessionTable!.SetSessionDataAsync(new[]
                                                              {
                                                                "part1",
                                                                "part2",
                                                              },
                                                              new TaskOptions
                                                              {
                                                                MaxDuration        = Duration.FromTimeSpan(TimeSpan.FromMinutes(1)),
                                                                MaxRetries         = 2,
                                                                Priority           = 1,
                                                                PartitionId        = "part1",
                                                                ApplicationName    = "ApplicationName",
                                                                ApplicationVersion = "ApplicationVersion",
                                                              },
                                                              CancellationToken.None)
                                         .ConfigureAwait(false);

    rootSessionId4_ = await SessionTable!.SetSessionDataAsync(new[]
                                                              {
                                                                "part1",
                                                                "part2",
                                                              },
                                                              new TaskOptions
                                                              {
                                                                MaxDuration        = Duration.FromTimeSpan(TimeSpan.FromMinutes(1)),
                                                                MaxRetries         = 2,
                                                                Priority           = 1,
                                                                PartitionId        = "part1",
                                                                ApplicationName    = "ApplicationName2",
                                                                ApplicationVersion = "ApplicationVersion2",
                                                              },
                                                              CancellationToken.None)
                                         .ConfigureAwait(false);

    await SessionTable.CancelSessionAsync(rootSessionId3_,
                                          CancellationToken.None)
                      .ConfigureAwait(false);
  }

  [TearDown]
  public virtual void TearDown()
  {
    SessionTable = null;
    RunTests     = false;
  }

  private static bool CheckForSkipSetup()
  {
    var category = TestContext.CurrentContext.Test.Properties.Get("Category") as string;
    return category is "SkipSetUp";
  }

  protected ISessionTable? SessionTable;

  protected bool RunTests;

  private string? rootSessionId_;
  private string? rootSessionId2_;
  private string? rootSessionId3_;
  private string? rootSessionId4_;

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
      var res = await SessionTable!.IsSessionCancelledAsync(rootSessionId_!,
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
      var res = await SessionTable!.GetDefaultTaskOptionAsync(rootSessionId_!,
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
      var sessionData = await SessionTable!.CancelSessionAsync(rootSessionId_!,
                                                               CancellationToken.None)
                                           .ConfigureAwait(false);

      Assert.AreEqual(SessionStatus.Cancelled,
                      sessionData.Status);

      var wasSessionCanceled = await SessionTable.IsSessionCancelledAsync(rootSessionId_!,
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
      await SessionTable!.CancelSessionAsync(rootSessionId_!,
                                             CancellationToken.None)
                         .ConfigureAwait(false);

      Assert.ThrowsAsync<SessionNotFoundException>(async () =>
                                                   {
                                                     await SessionTable.CancelSessionAsync(rootSessionId_!,
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
      var res = SessionTable!.DeleteSessionAsync(rootSessionId_!,
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
                                                                       SessionStatus.Running,
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
                                                          rootSessionId_!,
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
      var res = (await SessionTable!.ListSessionsAsync(data => data.Options.ApplicationName == "ApplicationName" && data.SessionId == rootSessionId_!,
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
}
