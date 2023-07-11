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

using Armonik.Api.Grpc.V1.SortDirection;

using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

using Output = ArmoniK.Core.Common.Storage.Output;
using TaskOptions = ArmoniK.Core.Base.DataStructures.TaskOptions;

namespace ArmoniK.Core.Common.Tests.ListApplicationsRequestExt;

[TestFixture(TestOf = typeof(ToApplicationFilterTest))]
public class ToApplicationFilterTest
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

  private static readonly ListApplicationsRequest.Types.Sort Sort = new()
                                                                    {
                                                                      Fields =
                                                                      {
                                                                        new ApplicationField
                                                                        {
                                                                          ApplicationField_ = new ApplicationRawField
                                                                                              {
                                                                                                Field = ApplicationRawEnumField.Name,
                                                                                              },
                                                                        },
                                                                      },
                                                                      Direction = SortDirection.Asc,
                                                                    };

  [Test]
  public void FilterNameShouldSucceed()
  {
    var func = new ListApplicationsRequest
               {
                 Filter = new ListApplicationsRequest.Types.Filter
                          {
                            Name = ApplicationName,
                          },
                 Sort = Sort,
               }.Filter.ToApplicationFilter()
                .Compile();

    Assert.IsTrue(func.Invoke(taskData_));
  }

  [Test]
  public void FilterNameShouldFail()
  {
    var func = new ListApplicationsRequest
               {
                 Filter = new ListApplicationsRequest.Types.Filter
                          {
                            Name = ApplicationName + "bad",
                          },
                 Sort = Sort,
               }.Filter.ToApplicationFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(taskData_));
  }

  [Test]
  public void FilterNamespaceShouldSucceed()
  {
    var func = new ListApplicationsRequest
               {
                 Filter = new ListApplicationsRequest.Types.Filter
                          {
                            Namespace = ApplicationNamespace,
                          },
                 Sort = Sort,
               }.Filter.ToApplicationFilter()
                .Compile();

    Assert.IsTrue(func.Invoke(taskData_));
  }

  [Test]
  public void FilterNamespaceShouldFail()
  {
    var func = new ListApplicationsRequest
               {
                 Filter = new ListApplicationsRequest.Types.Filter
                          {
                            Namespace = ApplicationNamespace + "bad",
                          },
                 Sort = Sort,
               }.Filter.ToApplicationFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(taskData_));
  }

  [Test]
  public void FilterVersionShouldSucceed()
  {
    var func = new ListApplicationsRequest
               {
                 Filter = new ListApplicationsRequest.Types.Filter
                          {
                            Version = ApplicationVersion,
                          },
                 Sort = Sort,
               }.Filter.ToApplicationFilter()
                .Compile();

    Assert.IsTrue(func.Invoke(taskData_));
  }

  [Test]
  public void FilterVersionShouldFail()
  {
    var func = new ListApplicationsRequest
               {
                 Filter = new ListApplicationsRequest.Types.Filter
                          {
                            Version = ApplicationVersion + "bad",
                          },
                 Sort = Sort,
               }.Filter.ToApplicationFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(taskData_));
  }

  [Test]
  public void FilterServiceShouldSucceed()
  {
    var func = new ListApplicationsRequest
               {
                 Filter = new ListApplicationsRequest.Types.Filter
                          {
                            Service = ApplicationService,
                          },
                 Sort = Sort,
               }.Filter.ToApplicationFilter()
                .Compile();

    Assert.IsTrue(func.Invoke(taskData_));
  }

  [Test]
  public void FilterServiceShouldFail()
  {
    var func = new ListApplicationsRequest
               {
                 Filter = new ListApplicationsRequest.Types.Filter
                          {
                            Service = ApplicationService + "bad",
                          },
                 Sort = Sort,
               }.Filter.ToApplicationFilter()
                .Compile();

    Assert.IsFalse(func.Invoke(taskData_));
  }
}
