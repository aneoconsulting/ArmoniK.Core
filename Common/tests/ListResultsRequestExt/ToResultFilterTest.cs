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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

using static Google.Protobuf.WellKnownTypes.Timestamp;

namespace ArmoniK.Core.Common.Tests.ListResultsRequestExt;

[TestFixture(TestOf = typeof(ToResultFilterTest))]
public class ToResultFilterTest
{
  private readonly Result result_ = new("SessionId",
                                        "Name",
                                        "OwnerTaskId",
                                        ResultStatus.Created,
                                        DateTime.UtcNow,
                                        Array.Empty<byte>());

  [Test]
  public void FilterStatusShouldSucceed()
  {
    var func = new ListResultsRequest
               {
                 Filter = new ListResultsRequest.Types.Filter
                          {
                            Status = ResultStatus.Created,
                          },
                 Sort = new ListResultsRequest.Types.Sort
                        {
                          Field     = ListResultsRequest.Types.OrderByField.CreatedAt,
                          Direction = ListResultsRequest.Types.OrderDirection.Asc,
                        },
               }.Filter.ToResultFilter()
                .Compile();

    Assert.IsTrue(func.Invoke(result_));
  }

  [Test]
  public void FilterStatusShouldFail()
  {
    var func = new ListResultsRequest
               {
                 Filter = new ListResultsRequest.Types.Filter
                          {
                            Status = ResultStatus.Aborted,
                          },
                 Sort = new ListResultsRequest.Types.Sort
                        {
                          Field     = ListResultsRequest.Types.OrderByField.CreatedAt,
                          Direction = ListResultsRequest.Types.OrderDirection.Asc,
                        },
               }.Filter.ToResultFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(result_));
  }

  [Test]
  public void FilterSessionIdShouldSucceed()
  {
    var func = new ListResultsRequest
               {
                 Filter = new ListResultsRequest.Types.Filter
                          {
                            SessionId = "SessionId",
                          },
                 Sort = new ListResultsRequest.Types.Sort
                        {
                          Field     = ListResultsRequest.Types.OrderByField.CreatedAt,
                          Direction = ListResultsRequest.Types.OrderDirection.Asc,
                        },
               }.Filter.ToResultFilter()
                .Compile();

    Assert.IsTrue(func.Invoke(result_));
  }

  [Test]
  public void FilterSessionIdShouldFail()
  {
    var func = new ListResultsRequest
               {
                 Filter = new ListResultsRequest.Types.Filter
                          {
                            SessionId = "BadSessionId",
                          },
                 Sort = new ListResultsRequest.Types.Sort
                        {
                          Field     = ListResultsRequest.Types.OrderByField.CreatedAt,
                          Direction = ListResultsRequest.Types.OrderDirection.Asc,
                        },
               }.Filter.ToResultFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(result_));
  }

  [Test]
  public void FilterNameShouldSucceed()
  {
    var func = new ListResultsRequest
               {
                 Filter = new ListResultsRequest.Types.Filter
                          {
                            Name = "Name",
                          },
                 Sort = new ListResultsRequest.Types.Sort
                        {
                          Field     = ListResultsRequest.Types.OrderByField.CreatedAt,
                          Direction = ListResultsRequest.Types.OrderDirection.Asc,
                        },
               }.Filter.ToResultFilter()
                .Compile();

    Assert.IsTrue(func.Invoke(result_));
  }

  [Test]
  public void FilterNameShouldFail()
  {
    var func = new ListResultsRequest
               {
                 Filter = new ListResultsRequest.Types.Filter
                          {
                            Name = "BadName",
                          },
                 Sort = new ListResultsRequest.Types.Sort
                        {
                          Field     = ListResultsRequest.Types.OrderByField.CreatedAt,
                          Direction = ListResultsRequest.Types.OrderDirection.Asc,
                        },
               }.Filter.ToResultFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(result_));
  }

  [Test]
  public void FilterOwnerTaskIdShouldSucceed()
  {
    var func = new ListResultsRequest
               {
                 Filter = new ListResultsRequest.Types.Filter
                          {
                            OwnerTaskId = "OwnerTaskId",
                          },
                 Sort = new ListResultsRequest.Types.Sort
                        {
                          Field     = ListResultsRequest.Types.OrderByField.CreatedAt,
                          Direction = ListResultsRequest.Types.OrderDirection.Asc,
                        },
               }.Filter.ToResultFilter()
                .Compile();

    Assert.IsTrue(func.Invoke(result_));
  }

  [Test]
  public void FilterOwnerTaskIdShouldFail()
  {
    var func = new ListResultsRequest
               {
                 Filter = new ListResultsRequest.Types.Filter
                          {
                            OwnerTaskId = "BadOwnerTaskId",
                          },
                 Sort = new ListResultsRequest.Types.Sort
                        {
                          Field     = ListResultsRequest.Types.OrderByField.CreatedAt,
                          Direction = ListResultsRequest.Types.OrderDirection.Asc,
                        },
               }.Filter.ToResultFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(result_));
  }

  [Test]
  public void FilterCreatedBeforeShouldSucceed()
  {
    var func = new ListResultsRequest
               {
                 Filter = new ListResultsRequest.Types.Filter
                          {
                            CreatedBefore = FromDateTime(DateTime.UtcNow),
                          },
                 Sort = new ListResultsRequest.Types.Sort
                        {
                          Field     = ListResultsRequest.Types.OrderByField.CreatedAt,
                          Direction = ListResultsRequest.Types.OrderDirection.Asc,
                        },
               }.Filter.ToResultFilter()
                .Compile();

    Assert.IsTrue(func.Invoke(result_));
  }

  [Test]
  public void FilterCreatedBeforeShouldFail()
  {
    var func = new ListResultsRequest
               {
                 Filter = new ListResultsRequest.Types.Filter
                          {
                            CreatedBefore = FromDateTime(DateTime.UtcNow),
                          },
                 Sort = new ListResultsRequest.Types.Sort
                        {
                          Field     = ListResultsRequest.Types.OrderByField.CreatedAt,
                          Direction = ListResultsRequest.Types.OrderDirection.Asc,
                        },
               }.Filter.ToResultFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(result_ with
                               {
                                 CreationDate = DateTime.UtcNow + TimeSpan.FromHours(3),
                               }));
  }

  [Test]
  public void FilterCreatedAfterShouldSucceed()
  {
    var func = new ListResultsRequest
               {
                 Filter = new ListResultsRequest.Types.Filter
                          {
                            CreatedAfter = FromDateTime(DateTime.UtcNow),
                          },
                 Sort = new ListResultsRequest.Types.Sort
                        {
                          Field     = ListResultsRequest.Types.OrderByField.CreatedAt,
                          Direction = ListResultsRequest.Types.OrderDirection.Asc,
                        },
               }.Filter.ToResultFilter()
                .Compile();

    Assert.IsTrue(func.Invoke(result_ with
                              {
                                CreationDate = DateTime.UtcNow + TimeSpan.FromHours(3),
                              }));
  }

  [Test]
  public void FilterCreatedAfterShouldFail()
  {
    var func = new ListResultsRequest
               {
                 Filter = new ListResultsRequest.Types.Filter
                          {
                            CreatedAfter = FromDateTime(DateTime.UtcNow),
                          },
                 Sort = new ListResultsRequest.Types.Sort
                        {
                          Field     = ListResultsRequest.Types.OrderByField.CreatedAt,
                          Direction = ListResultsRequest.Types.OrderDirection.Asc,
                        },
               }.Filter.ToResultFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(result_));
  }
}
