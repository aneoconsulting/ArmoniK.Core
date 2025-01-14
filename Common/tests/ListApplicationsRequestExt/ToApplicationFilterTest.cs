// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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
using ArmoniK.Api.gRPC.V1.SortDirection;
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

using Output = ArmoniK.Core.Common.Storage.Output;
using TaskOptions = ArmoniK.Core.Base.DataStructures.TaskOptions;
using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

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
                                            "CreatedBy",
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
                                            new Output(OutputStatus.Success,
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

  private static Func<TaskData, bool> RequestToFunc(ListApplicationsRequest.Types.Sort sort,
                                                    IEnumerable<FilterField>           filterFields)
    => CreateListApplicationsRequest(sort,
                                     filterFields)
       .Filters.ToApplicationFilter()
       .Compile();


  public static ListApplicationsRequest CreateListApplicationsRequest(ListApplicationsRequest.Types.Sort sort,
                                                                      IEnumerable<FilterField>           filterFields)
    => new()
       {
         Filters = new Filters
                   {
                     Or =
                     {
                       new FiltersAnd
                       {
                         And =
                         {
                           filterFields,
                         },
                       },
                     },
                   },
         Sort = sort,
       };

  public static FilterField CreateListApplicationsFilterString(ApplicationRawEnumField field,
                                                               FilterStringOperator    op,
                                                               string                  value)
    => new()
       {
         Field = new ApplicationField
                 {
                   ApplicationField_ = new ApplicationRawField
                                       {
                                         Field = field,
                                       },
                 },
         FilterString = new FilterString
                        {
                          Operator = op,
                          Value    = value,
                        },
       };

  [Test]
  [TestCaseSource(nameof(TestCasesFilter))]
  public void Filter2(IEnumerable<FilterField> filterFields,
                      bool                     expected)
  {
    var func = RequestToFunc(Sort,
                             filterFields);

    Assert.AreEqual(expected,
                    func.Invoke(taskData_));
  }

  public static IEnumerable<TestCaseData> TestCasesFilter()
  {
    yield return FieldToTestCaseTrue(CreateListApplicationsFilterString(ApplicationRawEnumField.Name,
                                                                        FilterStringOperator.Equal,
                                                                        ApplicationName));
    yield return FieldToTestCaseFalse(CreateListApplicationsFilterString(ApplicationRawEnumField.Name,
                                                                         FilterStringOperator.Equal,
                                                                         ApplicationName + "bad"));

    yield return FieldToTestCaseTrue(CreateListApplicationsFilterString(ApplicationRawEnumField.Namespace,
                                                                        FilterStringOperator.Equal,
                                                                        ApplicationNamespace));
    yield return FieldToTestCaseFalse(CreateListApplicationsFilterString(ApplicationRawEnumField.Namespace,
                                                                         FilterStringOperator.Equal,
                                                                         ApplicationNamespace + "bad"));

    yield return FieldToTestCaseTrue(CreateListApplicationsFilterString(ApplicationRawEnumField.Version,
                                                                        FilterStringOperator.Equal,
                                                                        ApplicationVersion));
    yield return FieldToTestCaseFalse(CreateListApplicationsFilterString(ApplicationRawEnumField.Version,
                                                                         FilterStringOperator.Equal,
                                                                         ApplicationVersion + "bad"));

    yield return FieldToTestCaseTrue(CreateListApplicationsFilterString(ApplicationRawEnumField.Service,
                                                                        FilterStringOperator.Equal,
                                                                        ApplicationService));
    yield return FieldToTestCaseTrue(CreateListApplicationsFilterString(ApplicationRawEnumField.Service,
                                                                        FilterStringOperator.StartsWith,
                                                                        ApplicationService));
    yield return FieldToTestCaseTrue(CreateListApplicationsFilterString(ApplicationRawEnumField.Service,
                                                                        FilterStringOperator.EndsWith,
                                                                        ApplicationService));
    yield return FieldToTestCaseTrue(CreateListApplicationsFilterString(ApplicationRawEnumField.Service,
                                                                        FilterStringOperator.Contains,
                                                                        ApplicationService));
    yield return FieldToTestCaseTrue(CreateListApplicationsFilterString(ApplicationRawEnumField.Service,
                                                                        FilterStringOperator.NotContains,
                                                                        ApplicationService + "bad"));
    yield return FieldToTestCaseFalse(CreateListApplicationsFilterString(ApplicationRawEnumField.Service,
                                                                         FilterStringOperator.Equal,
                                                                         ApplicationService + "bad"));
  }

  private static TestCaseData FieldToTestCaseFalse(FilterField filterField)
    => new TestCaseData(new[]
                        {
                          filterField,
                        },
                        false).SetArgDisplayNames(filterField.ToString()!);

  private static TestCaseData FieldToTestCaseTrue(FilterField filterField)
    => new TestCaseData(new[]
                        {
                          filterField,
                        },
                        true).SetArgDisplayNames(filterField.ToString()!);
}
