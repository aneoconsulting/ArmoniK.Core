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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Sessions;

using Armonik.Api.Grpc.V1.SortDirection;

using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

using static Google.Protobuf.WellKnownTypes.Timestamp;

using TaskOptions = ArmoniK.Core.Base.DataStructures.TaskOptions;

namespace ArmoniK.Core.Common.Tests.ListSessionsRequestExt;

[TestFixture(TestOf = typeof(ToSessionDataFilterTest))]
public class ToSessionDataFilterTest
{
  private static readonly TaskOptions Options = new(new Dictionary<string, string>(),
                                                    TimeSpan.MaxValue,
                                                    5,
                                                    1,
                                                    "part1",
                                                    "applicationName",
                                                    "applicationVersion",
                                                    "applicationNamespace",
                                                    "applicationService",
                                                    "engineType");

  private readonly SessionData sessionData_ = new("SessionId",
                                                  SessionStatus.Running,
                                                  DateTime.UtcNow,
                                                  DateTime.UtcNow,
                                                  new List<string>(),
                                                  Options);

  private static readonly ListSessionsRequest.Types.Sort Sort = new()
                                                                {
                                                                  Direction = SortDirection.Asc,
                                                                  Field = new SessionField
                                                                          {
                                                                            SessionRawField = new SessionRawField
                                                                                              {
                                                                                                Field = SessionRawEnumField.CreatedAt,
                                                                                              },
                                                                          },
                                                                };

  [Test]
  public void FilterStatusShouldSucceed()
  {
    var func = new ListSessionsRequest
               {
                 Filter = new ListSessionsRequest.Types.Filter
                          {
                            Status = SessionStatus.Running,
                          },
                 Sort = Sort,
               }.Filter.ToSessionDataFilter()
                .Compile();

    Assert.IsTrue(func.Invoke(sessionData_));
  }

  [Test]
  public void FilterStatusShouldFail()
  {
    var func = new ListSessionsRequest
               {
                 Filter = new ListSessionsRequest.Types.Filter
                          {
                            Status = SessionStatus.Cancelled,
                          },
                 Sort = Sort,
               }.Filter.ToSessionDataFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(sessionData_));
  }

  [Test]
  public void FilterSessionIdShouldSucceed()
  {
    var func = new ListSessionsRequest
               {
                 Filter = new ListSessionsRequest.Types.Filter
                          {
                            SessionId = "SessionId",
                          },
                 Sort = Sort,
               }.Filter.ToSessionDataFilter()
                .Compile();

    Assert.IsTrue(func.Invoke(sessionData_));
  }

  [Test]
  public void FilterSessionIdShouldFail()
  {
    var func = new ListSessionsRequest
               {
                 Filter = new ListSessionsRequest.Types.Filter
                          {
                            SessionId = "BadSessionId",
                          },
                 Sort = Sort,
               }.Filter.ToSessionDataFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(sessionData_));
  }

  [Test]
  public void FilterAppNameShouldSucceed()
  {
    var func = new ListSessionsRequest
               {
                 Filter = new ListSessionsRequest.Types.Filter
                          {
                            ApplicationName = "applicationName",
                          },
                 Sort = Sort,
               }.Filter.ToSessionDataFilter()
                .Compile();

    Assert.IsTrue(func.Invoke(sessionData_));
  }

  [Test]
  public void FilterAppNameShouldFail()
  {
    var func = new ListSessionsRequest
               {
                 Filter = new ListSessionsRequest.Types.Filter
                          {
                            ApplicationName = "badname",
                          },
                 Sort = Sort,
               }.Filter.ToSessionDataFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(sessionData_));
  }

  [Test]
  public void FilterAppVersionShouldSucceed()
  {
    var func = new ListSessionsRequest
               {
                 Filter = new ListSessionsRequest.Types.Filter
                          {
                            ApplicationVersion = "applicationVersion",
                          },
                 Sort = Sort,
               }.Filter.ToSessionDataFilter()
                .Compile();

    Assert.IsTrue(func.Invoke(sessionData_));
  }

  [Test]
  public void FilterAppVersionShouldFail()
  {
    var func = new ListSessionsRequest
               {
                 Filter = new ListSessionsRequest.Types.Filter
                          {
                            ApplicationVersion = "badname",
                          },
                 Sort = Sort,
               }.Filter.ToSessionDataFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(sessionData_));
  }

  [Test]
  public void FilterCreatedBeforeShouldSucceed()
  {
    var func = new ListSessionsRequest
               {
                 Filter = new ListSessionsRequest.Types.Filter
                          {
                            CreatedBefore = FromDateTime(DateTime.UtcNow),
                          },
                 Sort = Sort,
               }.Filter.ToSessionDataFilter()
                .Compile();

    Assert.IsTrue(func.Invoke(sessionData_));
  }

  [Test]
  public void FilterCreatedBeforeShouldFail()
  {
    var func = new ListSessionsRequest
               {
                 Filter = new ListSessionsRequest.Types.Filter
                          {
                            CreatedBefore = FromDateTime(DateTime.UtcNow),
                          },
                 Sort = Sort,
               }.Filter.ToSessionDataFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(sessionData_ with
                               {
                                 CreationDate = DateTime.UtcNow + TimeSpan.FromHours(3),
                               }));
  }

  [Test]
  public void FilterCreatedAfterShouldSucceed()
  {
    var func = new ListSessionsRequest
               {
                 Filter = new ListSessionsRequest.Types.Filter
                          {
                            CreatedAfter = FromDateTime(DateTime.UtcNow),
                          },
                 Sort = Sort,
               }.Filter.ToSessionDataFilter()
                .Compile();

    Assert.IsTrue(func.Invoke(sessionData_ with
                              {
                                CreationDate = DateTime.UtcNow + TimeSpan.FromHours(3),
                              }));
  }

  [Test]
  public void FilterCreatedAfterShouldFail()
  {
    var func = new ListSessionsRequest
               {
                 Filter = new ListSessionsRequest.Types.Filter
                          {
                            CreatedAfter = FromDateTime(DateTime.UtcNow),
                          },
                 Sort = Sort,
               }.Filter.ToSessionDataFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(sessionData_));
  }

  [Test]
  public void FilterCancelledBeforeShouldSucceed()
  {
    var func = new ListSessionsRequest
               {
                 Filter = new ListSessionsRequest.Types.Filter
                          {
                            CancelledBefore = FromDateTime(DateTime.UtcNow),
                          },
                 Sort = Sort,
               }.Filter.ToSessionDataFilter()
                .Compile();

    Assert.IsTrue(func.Invoke(sessionData_));
  }

  [Test]
  public void FilterCancelledBeforeShouldFail()
  {
    var func = new ListSessionsRequest
               {
                 Filter = new ListSessionsRequest.Types.Filter
                          {
                            CancelledBefore = FromDateTime(DateTime.UtcNow),
                          },
                 Sort = Sort,
               }.Filter.ToSessionDataFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(sessionData_ with
                               {
                                 CancellationDate = DateTime.UtcNow + TimeSpan.FromHours(3),
                               }));
  }

  [Test]
  public void FilterCancelledAfterShouldSucceed()
  {
    var func = new ListSessionsRequest
               {
                 Filter = new ListSessionsRequest.Types.Filter
                          {
                            CancelledAfter = FromDateTime(DateTime.UtcNow),
                          },
                 Sort = Sort,
               }.Filter.ToSessionDataFilter()
                .Compile();

    Assert.IsTrue(func.Invoke(sessionData_ with
                              {
                                CancellationDate = DateTime.UtcNow + TimeSpan.FromHours(3),
                              }));
  }

  [Test]
  public void FilterCancelledAfterShouldFail()
  {
    var func = new ListSessionsRequest
               {
                 Filter = new ListSessionsRequest.Types.Filter
                          {
                            CancelledAfter = FromDateTime(DateTime.UtcNow),
                          },
                 Sort = Sort,
               }.Filter.ToSessionDataFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(sessionData_));
  }
}
