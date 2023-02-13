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
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

using Output = ArmoniK.Core.Common.Storage.Output;
using TaskOptions = ArmoniK.Core.Common.Storage.TaskOptions;

namespace ArmoniK.Core.Common.Tests.ListTasksRequestExt;

[TestFixture(TestOf = typeof(ToTaskDataFieldTest))]
public class ToTaskDataFieldTest
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

  private readonly TaskData taskData_ = new("SessionId",
                                            "TaskCompletedId",
                                            "OwnerPodId",
                                            "OwnerPodName",
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
                                            new Output(true,
                                                       ""));

  [Test]
  public void InvokeShouldReturnCreationDate()
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
                          Field     = ListTasksRequest.Types.OrderByField.CreatedAt,
                        },
               }.Sort.ToTaskDataField()
                .Compile();

    Assert.AreEqual(taskData_.CreationDate,
                    func.Invoke(taskData_));
  }

  [Test]
  public void InvokeShouldReturnSessionId()
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
                          Field     = ListTasksRequest.Types.OrderByField.SessionId,
                        },
               }.Sort.ToTaskDataField()
                .Compile();

    Assert.AreEqual(taskData_.SessionId,
                    func.Invoke(taskData_));
  }

  [Test]
  public void InvokeShouldReturnStatus()
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
                          Field     = ListTasksRequest.Types.OrderByField.Status,
                        },
               }.Sort.ToTaskDataField()
                .Compile();

    Assert.AreEqual(taskData_.Status,
                    func.Invoke(taskData_));
  }

  [Test]
  public void InvokeShouldReturnTaskId()
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
                          Field     = ListTasksRequest.Types.OrderByField.TaskId,
                        },
               }.Sort.ToTaskDataField()
                .Compile();

    Assert.AreEqual(taskData_.TaskId,
                    func.Invoke(taskData_));
  }

  [Test]
  public void InvokeShouldReturnEndedAt()
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
                          Field     = ListTasksRequest.Types.OrderByField.EndedAt,
                        },
               }.Sort.ToTaskDataField()
                .Compile();

    Assert.AreEqual(taskData_.EndDate,
                    func.Invoke(taskData_));
  }

  [Test]
  public void InvokeShouldReturnStartedAt()
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
               }.Sort.ToTaskDataField()
                .Compile();

    Assert.AreEqual(taskData_.StartDate,
                    func.Invoke(taskData_));
  }
}
