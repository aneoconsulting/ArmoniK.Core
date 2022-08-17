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
using Armonik.Api.gRPC.V1.Tasks;

using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.ListTasksRequestExt;

[TestFixture(TestOf = typeof(ToTaskDataFilterTest))]
public class ToTaskDataFilterTest
{
  private static readonly Storage.TaskOptions Options = new(new Dictionary<string, string>(),
                                                            TimeSpan.MaxValue,
                                                            5,
                                                            1,
                                                            "part1",
                                                            "applicationName",
                                                            "applicationVersion");

  private readonly TaskData taskData_ = new("SessionId",
                                            "TaskCompletedId",
                                            "OwnerPodId",
                                            "PayloadId",
                                            new[]
                                            {
                                              "parent1",
                                            },
                                            new[]
                                            {
                                              "dependency1",
                                            },
                                            new[]
                                            {
                                              "output1",
                                            },
                                            Array.Empty<string>(),
                                            TaskStatus.Completed,
                                            Options,
                                            new Storage.Output(true,
                                                               ""));

  [Test]
  public void FilterSessionIdShouldSucceed()
  {
    var func = new ListTasksRequest
               {
                 Filter = new ListTasksRequest.Types.Filter
                          {
                            SessionId = "SessionId",
                          },
                 Sort = new ListTasksRequest.Types.Sort
                        {
                          Direction = ListTasksRequest.Types.OrderDirection.Asc,
                          Field     = ListTasksRequest.Types.OrderByField.StartedAt,
                        },
               }.Filter.ToTaskDataFilter()
                .Compile();

    Assert.IsTrue(func.Invoke(taskData_));
  }

  [Test]
  public void FilterWrongSessionIdShouldFail()
  {
    var func = new ListTasksRequest
               {
                 Filter = new ListTasksRequest.Types.Filter
                          {
                            SessionId = "SessionId",
                          },
                 Sort = new ListTasksRequest.Types.Sort
                        {
                          Direction = ListTasksRequest.Types.OrderDirection.Asc,
                          Field     = ListTasksRequest.Types.OrderByField.StartedAt,
                        },
               }.Filter.ToTaskDataFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(taskData_ with
                               {
                                 SessionId = "test",
                               }));
  }

  [Test]
  public void FilterCreatedAfterShouldFail()
  {
    var func = new ListTasksRequest
               {
                 Filter = new ListTasksRequest.Types.Filter
                          {
                            CreatedAfter = Google.Protobuf.WellKnownTypes.Timestamp.FromDateTime(DateTime.UtcNow),
                          },
                 Sort = new ListTasksRequest.Types.Sort
                        {
                          Direction = ListTasksRequest.Types.OrderDirection.Asc,
                          Field     = ListTasksRequest.Types.OrderByField.StartedAt,
                        },
               }.Filter.ToTaskDataFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(taskData_ with
                               {
                                 CreationDate = DateTime.UtcNow - TimeSpan.FromHours(3),
                               }));
  }

  [Test]
  public void FilterCreatedAfterShouldSucceed()
  {
    var func = new ListTasksRequest
               {
                 Filter = new ListTasksRequest.Types.Filter
                          {
                            CreatedAfter = Google.Protobuf.WellKnownTypes.Timestamp.FromDateTime(DateTime.UtcNow),
                          },
                 Sort = new ListTasksRequest.Types.Sort
                        {
                          Direction = ListTasksRequest.Types.OrderDirection.Asc,
                          Field     = ListTasksRequest.Types.OrderByField.StartedAt,
                        },
               }.Filter.ToTaskDataFilter()
                .Compile();

    Assert.IsTrue(func.Invoke(taskData_ with
                              {
                                CreationDate = DateTime.UtcNow + TimeSpan.FromHours(3),
                              }));
  }

  [Test]
  public void FilterCreatedBeforeShouldFail()
  {
    var func = new ListTasksRequest
               {
                 Filter = new ListTasksRequest.Types.Filter
                          {
                            CreatedBefore = Google.Protobuf.WellKnownTypes.Timestamp.FromDateTime(DateTime.UtcNow),
                          },
                 Sort = new ListTasksRequest.Types.Sort
                        {
                          Direction = ListTasksRequest.Types.OrderDirection.Asc,
                          Field     = ListTasksRequest.Types.OrderByField.StartedAt,
                        },
               }.Filter.ToTaskDataFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(taskData_ with
                               {
                                 CreationDate = DateTime.UtcNow + TimeSpan.FromHours(3),
                               }));
  }

  [Test]
  public void FilterCreatedBeforeShouldSucceed()
  {
    var func = new ListTasksRequest
               {
                 Filter = new ListTasksRequest.Types.Filter
                          {
                            CreatedBefore = Google.Protobuf.WellKnownTypes.Timestamp.FromDateTime(DateTime.UtcNow),
                          },
                 Sort = new ListTasksRequest.Types.Sort
                        {
                          Direction = ListTasksRequest.Types.OrderDirection.Asc,
                          Field     = ListTasksRequest.Types.OrderByField.StartedAt,
                        },
               }.Filter.ToTaskDataFilter()
                .Compile();

    Assert.IsTrue(func.Invoke(taskData_ with
                              {
                                CreationDate = DateTime.UtcNow - TimeSpan.FromHours(3),
                              }));
  }

  [Test]
  public void FilterEndedAfterShouldFail()
  {
    var func = new ListTasksRequest
    {
      Filter = new ListTasksRequest.Types.Filter
      {
        EndedAfter = Google.Protobuf.WellKnownTypes.Timestamp.FromDateTime(DateTime.UtcNow),
      },
      Sort = new ListTasksRequest.Types.Sort
      {
        Direction = ListTasksRequest.Types.OrderDirection.Asc,
        Field = ListTasksRequest.Types.OrderByField.StartedAt,
      },
    }.Filter.ToTaskDataFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(taskData_ with
    {
      EndDate = DateTime.UtcNow - TimeSpan.FromHours(3),
    }));
  }

  [Test]
  public void FilterEndedAfterShouldSucceed()
  {
    var func = new ListTasksRequest
    {
      Filter = new ListTasksRequest.Types.Filter
      {
        EndedAfter = Google.Protobuf.WellKnownTypes.Timestamp.FromDateTime(DateTime.UtcNow),
      },
      Sort = new ListTasksRequest.Types.Sort
      {
        Direction = ListTasksRequest.Types.OrderDirection.Asc,
        Field = ListTasksRequest.Types.OrderByField.StartedAt,
      },
    }.Filter.ToTaskDataFilter()
                .Compile();

    Assert.IsTrue(func.Invoke(taskData_ with
    {
      EndDate = DateTime.UtcNow + TimeSpan.FromHours(3),
    }));
  }

  [Test]
  public void FilterEndedBeforeShouldFail()
  {
    var func = new ListTasksRequest
    {
      Filter = new ListTasksRequest.Types.Filter
      {
        EndedBefore = Google.Protobuf.WellKnownTypes.Timestamp.FromDateTime(DateTime.UtcNow),
      },
      Sort = new ListTasksRequest.Types.Sort
      {
        Direction = ListTasksRequest.Types.OrderDirection.Asc,
        Field = ListTasksRequest.Types.OrderByField.StartedAt,
      },
    }.Filter.ToTaskDataFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(taskData_ with
    {
      EndDate = DateTime.UtcNow + TimeSpan.FromHours(3),
    }));
  }

  [Test]
  public void FilterEndedBeforeShouldSucceed()
  {
    var func = new ListTasksRequest
    {
      Filter = new ListTasksRequest.Types.Filter
      {
        EndedBefore = Google.Protobuf.WellKnownTypes.Timestamp.FromDateTime(DateTime.UtcNow),
      },
      Sort = new ListTasksRequest.Types.Sort
      {
        Direction = ListTasksRequest.Types.OrderDirection.Asc,
        Field = ListTasksRequest.Types.OrderByField.StartedAt,
      },
    }.Filter.ToTaskDataFilter()
                .Compile();

    Assert.IsTrue(func.Invoke(taskData_ with
    {
      EndDate = DateTime.UtcNow - TimeSpan.FromHours(3),
    }));
  }

  [Test]
  public void FilterStartedAfterShouldFail()
  {
    var func = new ListTasksRequest
    {
      Filter = new ListTasksRequest.Types.Filter
      {
        StartedAfter = Google.Protobuf.WellKnownTypes.Timestamp.FromDateTime(DateTime.UtcNow),
      },
      Sort = new ListTasksRequest.Types.Sort
      {
        Direction = ListTasksRequest.Types.OrderDirection.Asc,
        Field = ListTasksRequest.Types.OrderByField.StartedAt,
      },
    }.Filter.ToTaskDataFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(taskData_ with
    {
      StartDate = DateTime.UtcNow - TimeSpan.FromHours(3),
    }));
  }

  [Test]
  public void FilterStartedAfterShouldSucceed()
  {
    var func = new ListTasksRequest
    {
      Filter = new ListTasksRequest.Types.Filter
      {
        StartedAfter = Google.Protobuf.WellKnownTypes.Timestamp.FromDateTime(DateTime.UtcNow),
      },
      Sort = new ListTasksRequest.Types.Sort
      {
        Direction = ListTasksRequest.Types.OrderDirection.Asc,
        Field = ListTasksRequest.Types.OrderByField.StartedAt,
      },
    }.Filter.ToTaskDataFilter()
                .Compile();

    Assert.IsTrue(func.Invoke(taskData_ with
    {
      StartDate = DateTime.UtcNow + TimeSpan.FromHours(3),
    }));
  }

  [Test]
  public void FilterStartedBeforeShouldFail()
  {
    var func = new ListTasksRequest
    {
      Filter = new ListTasksRequest.Types.Filter
      {
        StartedBefore = Google.Protobuf.WellKnownTypes.Timestamp.FromDateTime(DateTime.UtcNow),
      },
      Sort = new ListTasksRequest.Types.Sort
      {
        Direction = ListTasksRequest.Types.OrderDirection.Asc,
        Field = ListTasksRequest.Types.OrderByField.StartedAt,
      },
    }.Filter.ToTaskDataFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(taskData_ with
    {
      StartDate = DateTime.UtcNow + TimeSpan.FromHours(3),
    }));
  }

  [Test]
  public void FilterStartedBeforeShouldSucceed()
  {
    var func = new ListTasksRequest
    {
      Filter = new ListTasksRequest.Types.Filter
      {
        StartedBefore = Google.Protobuf.WellKnownTypes.Timestamp.FromDateTime(DateTime.UtcNow),
      },
      Sort = new ListTasksRequest.Types.Sort
      {
        Direction = ListTasksRequest.Types.OrderDirection.Asc,
        Field = ListTasksRequest.Types.OrderByField.StartedAt,
      },
    }.Filter.ToTaskDataFilter()
                .Compile();

    Assert.IsTrue(func.Invoke(taskData_ with
    {
      StartDate = DateTime.UtcNow - TimeSpan.FromHours(3),
    }));
  }


  [Test]
  public void FilterStatusShouldSucceed()
  {
    var func = new ListTasksRequest
               {
                 Filter = new ListTasksRequest.Types.Filter
                          {
                            Status = TaskStatus.Completed,
                          },
                 Sort = new ListTasksRequest.Types.Sort
                        {
                          Direction = ListTasksRequest.Types.OrderDirection.Asc,
                          Field     = ListTasksRequest.Types.OrderByField.StartedAt,
                        },
               }.Filter.ToTaskDataFilter()
                .Compile();

    Assert.IsTrue(func.Invoke(taskData_ with
                              {
                                Status = TaskStatus.Completed,
                              }));
  }

  [Test]
  public void FilterStatusShouldFail()
  {
    var func = new ListTasksRequest
               {
                 Filter = new ListTasksRequest.Types.Filter
                          {
                            Status = TaskStatus.Completed,
                          },
                 Sort = new ListTasksRequest.Types.Sort
                        {
                          Direction = ListTasksRequest.Types.OrderDirection.Asc,
                          Field     = ListTasksRequest.Types.OrderByField.StartedAt,
                        },
               }.Filter.ToTaskDataFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(taskData_ with
                               {
                                 Status = TaskStatus.Canceling,
                               }));
  }

}
