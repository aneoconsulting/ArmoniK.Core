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
using System.Linq;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Applications;

using Armonik.Api.Grpc.V1.SortDirection;

using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

using Output = ArmoniK.Core.Common.Storage.Output;
using TaskOptions = ArmoniK.Core.Base.DataStructures.TaskOptions;

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


  public static IEnumerable<TestCaseData> TestCasesInvoke()
  {
    TestCaseData Case(ApplicationRawEnumField field,
                      object?                 expected)
      => new TestCaseData(new ApplicationRawField
                          {
                            Field = field,
                          },
                          expected).SetArgDisplayNames(field.ToString());

    yield return Case(ApplicationRawEnumField.Service,
                      Options.ApplicationService);
    yield return Case(ApplicationRawEnumField.Name,
                      Options.ApplicationName);
    yield return Case(ApplicationRawEnumField.Namespace,
                      Options.ApplicationNamespace);
    yield return Case(ApplicationRawEnumField.Version,
                      Options.ApplicationVersion);
  }

  [Test]
  [TestCaseSource(nameof(TestCasesInvoke))]
  public void InvokeShouldReturnExpectedValue(ApplicationRawField field,
                                              object?             expected)
  {
    var func = new ListApplicationsRequest
               {
                 Filters = new Filters(),
                 Sort = new ListApplicationsRequest.Types.Sort
                        {
                          Fields =
                          {
                            new ApplicationField
                            {
                              ApplicationField_ = field,
                            },
                          },
                          Direction = SortDirection.Asc,
                        },
               }.Sort.Fields.Single()
                .ToField()
                .Compile();

    Assert.AreEqual(expected,
                    func.Invoke(taskData_));
  }

  [Test]
  public void InvokeMultipleShouldSucceed()
  {
    var field = new ListApplicationsRequest
                {
                  Filters = new Filters(),
                  Sort = new ListApplicationsRequest.Types.Sort
                         {
                           Fields =
                           {
                             new ApplicationField
                             {
                               ApplicationField_ = new ApplicationRawField
                                                   {
                                                     Field = ApplicationRawEnumField.Service,
                                                   },
                             },
                             new ApplicationField
                             {
                               ApplicationField_ = new ApplicationRawField
                                                   {
                                                     Field = ApplicationRawEnumField.Name,
                                                   },
                             },
                           },
                           Direction = SortDirection.Asc,
                         },
                }.Sort.Fields;

    Assert.AreEqual(2,
                    field.Count);

    Assert.AreEqual(Options.ApplicationService,
                    field[0]
                      .ToField()
                      .Compile()
                      .Invoke(taskData_));

    Assert.AreEqual(Options.ApplicationName,
                    field[1]
                      .ToField()
                      .Compile()
                      .Invoke(taskData_));
  }
}
