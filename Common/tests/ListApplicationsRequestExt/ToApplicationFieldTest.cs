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
using ArmoniK.Api.gRPC.V1.Applications;
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

using Output = ArmoniK.Core.Common.Storage.Output;
using TaskOptions = ArmoniK.Core.Common.Storage.TaskOptions;

namespace ArmoniK.Core.Common.Tests.ListApplicationsRequestExt;

[TestFixture(TestOf = typeof(ToApplicationFieldTest))]
public class ToApplicationFieldTest
{
  private const string ApplicationName      = "applicationName";
  private const string ApplicationService   = "applicationService";
  private const string ApplicationNamespace = "applicationNamespace";
  private const string ApplicationVersion   = "applicationVersion";

  private static readonly TaskOptions Options = new(new Dictionary<string, string>(),
                                                    TimeSpan.MaxValue,
                                                    5,
                                                    1,
                                                    "part1",
                                                    ApplicationName,
                                                    ApplicationVersion,
                                                    ApplicationNamespace,
                                                    ApplicationService,
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
  public void InvokeShouldReturnName()
  {
    var func = new ListApplicationsRequest
               {
                 Filter = new ListApplicationsRequest.Types.Filter(),
                 Sort = new ListApplicationsRequest.Types.Sort
                        {
                          Field     = ListApplicationsRequest.Types.OrderByField.Name,
                          Direction = ListApplicationsRequest.Types.OrderDirection.Asc,
                        },
               }.Sort.ToApplicationField()
                .Compile();

    Assert.AreEqual(Options.ApplicationName,
                    func.Invoke(taskData_));
  }

  [Test]
  public void InvokeShouldReturnNamespace()
  {
    var func = new ListApplicationsRequest
               {
                 Filter = new ListApplicationsRequest.Types.Filter(),
                 Sort = new ListApplicationsRequest.Types.Sort
                        {
                          Field     = ListApplicationsRequest.Types.OrderByField.Namespace,
                          Direction = ListApplicationsRequest.Types.OrderDirection.Asc,
                        },
               }.Sort.ToApplicationField()
                .Compile();

    Assert.AreEqual(Options.ApplicationNamespace,
                    func.Invoke(taskData_));
  }

  [Test]
  public void InvokeShouldReturnVersion()
  {
    var func = new ListApplicationsRequest
               {
                 Filter = new ListApplicationsRequest.Types.Filter(),
                 Sort = new ListApplicationsRequest.Types.Sort
                        {
                          Field     = ListApplicationsRequest.Types.OrderByField.Version,
                          Direction = ListApplicationsRequest.Types.OrderDirection.Asc,
                        },
               }.Sort.ToApplicationField()
                .Compile();

    Assert.AreEqual(Options.ApplicationVersion,
                    func.Invoke(taskData_));
  }

  [Test]
  public void InvokeShouldReturnService()
  {
    var func = new ListApplicationsRequest
               {
                 Filter = new ListApplicationsRequest.Types.Filter(),
                 Sort = new ListApplicationsRequest.Types.Sort
                        {
                          Field     = ListApplicationsRequest.Types.OrderByField.Service,
                          Direction = ListApplicationsRequest.Types.OrderDirection.Asc,
                        },
               }.Sort.ToApplicationField()
                .Compile();

    Assert.AreEqual(Options.ApplicationService,
                    func.Invoke(taskData_));
  }
}
