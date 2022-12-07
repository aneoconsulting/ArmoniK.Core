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

using System;
using System.Collections.Generic;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Sessions;
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

using static Google.Protobuf.WellKnownTypes.Timestamp;

using TaskOptions = ArmoniK.Core.Common.Storage.TaskOptions;

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

  [Test]
  public void FilterStatusShouldSucceed()
  {
    var func = new ListSessionsRequest
               {
                 Filter = new ListSessionsRequest.Types.Filter
                          {
                            Status = SessionStatus.Running,
                          },
                 Sort = new ListSessionsRequest.Types.Sort
                        {
                          Direction = ListSessionsRequest.Types.OrderDirection.Asc,
                          Field     = ListSessionsRequest.Types.OrderByField.CreatedAt,
                        },
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
                 Sort = new ListSessionsRequest.Types.Sort
                        {
                          Direction = ListSessionsRequest.Types.OrderDirection.Asc,
                          Field     = ListSessionsRequest.Types.OrderByField.CreatedAt,
                        },
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
                 Sort = new ListSessionsRequest.Types.Sort
                        {
                          Direction = ListSessionsRequest.Types.OrderDirection.Asc,
                          Field     = ListSessionsRequest.Types.OrderByField.CreatedAt,
                        },
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
                 Sort = new ListSessionsRequest.Types.Sort
                        {
                          Direction = ListSessionsRequest.Types.OrderDirection.Asc,
                          Field     = ListSessionsRequest.Types.OrderByField.CreatedAt,
                        },
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
                 Sort = new ListSessionsRequest.Types.Sort
                        {
                          Direction = ListSessionsRequest.Types.OrderDirection.Asc,
                          Field     = ListSessionsRequest.Types.OrderByField.CreatedAt,
                        },
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
                 Sort = new ListSessionsRequest.Types.Sort
                        {
                          Direction = ListSessionsRequest.Types.OrderDirection.Asc,
                          Field     = ListSessionsRequest.Types.OrderByField.CreatedAt,
                        },
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
                 Sort = new ListSessionsRequest.Types.Sort
                        {
                          Direction = ListSessionsRequest.Types.OrderDirection.Asc,
                          Field     = ListSessionsRequest.Types.OrderByField.CreatedAt,
                        },
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
                 Sort = new ListSessionsRequest.Types.Sort
                        {
                          Direction = ListSessionsRequest.Types.OrderDirection.Asc,
                          Field     = ListSessionsRequest.Types.OrderByField.CreatedAt,
                        },
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
                 Sort = new ListSessionsRequest.Types.Sort
                        {
                          Direction = ListSessionsRequest.Types.OrderDirection.Asc,
                          Field     = ListSessionsRequest.Types.OrderByField.CreatedAt,
                        },
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
                 Sort = new ListSessionsRequest.Types.Sort
                        {
                          Direction = ListSessionsRequest.Types.OrderDirection.Asc,
                          Field     = ListSessionsRequest.Types.OrderByField.CreatedAt,
                        },
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
                 Sort = new ListSessionsRequest.Types.Sort
                        {
                          Direction = ListSessionsRequest.Types.OrderDirection.Asc,
                          Field     = ListSessionsRequest.Types.OrderByField.CreatedAt,
                        },
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
                 Sort = new ListSessionsRequest.Types.Sort
                        {
                          Direction = ListSessionsRequest.Types.OrderDirection.Asc,
                          Field     = ListSessionsRequest.Types.OrderByField.CreatedAt,
                        },
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
                 Sort = new ListSessionsRequest.Types.Sort
                        {
                          Direction = ListSessionsRequest.Types.OrderDirection.Asc,
                          Field     = ListSessionsRequest.Types.OrderByField.CreatedAt,
                        },
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
                 Sort = new ListSessionsRequest.Types.Sort
                        {
                          Direction = ListSessionsRequest.Types.OrderDirection.Asc,
                          Field     = ListSessionsRequest.Types.OrderByField.CreatedAt,
                        },
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
                 Sort = new ListSessionsRequest.Types.Sort
                        {
                          Direction = ListSessionsRequest.Types.OrderDirection.Asc,
                          Field     = ListSessionsRequest.Types.OrderByField.CreatedAt,
                        },
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
                 Sort = new ListSessionsRequest.Types.Sort
                        {
                          Direction = ListSessionsRequest.Types.OrderDirection.Asc,
                          Field     = ListSessionsRequest.Types.OrderByField.CreatedAt,
                        },
               }.Filter.ToSessionDataFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(sessionData_));
  }
}
