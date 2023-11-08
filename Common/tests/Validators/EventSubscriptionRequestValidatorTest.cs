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
using ArmoniK.Api.gRPC.V1.Events;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Core.Common.gRPC.Validators;
using ArmoniK.Core.Common.Tests.ListResultsRequestExt;
using ArmoniK.Core.Common.Tests.ListTasksRequestExt;

using FluentValidation.TestHelper;

using NUnit.Framework;

using FilterField = ArmoniK.Api.gRPC.V1.Tasks.FilterField;
using Filters = ArmoniK.Api.gRPC.V1.Tasks.Filters;
using FiltersAnd = ArmoniK.Api.gRPC.V1.Tasks.FiltersAnd;

namespace ArmoniK.Core.Common.Tests.Validators;

[TestFixture]
public class EventSubscriptionRequestValidatorTest
{
  private static readonly DateTime DateToCompare = new DateTime(2020,
                                                                3,
                                                                15).ToUniversalTime();


  [Test]
  [TestCaseSource(nameof(TestCasesTasks))]
  [TestCaseSource(nameof(TestCasesResults))]
  public void Validate(EventSubscriptionRequest request,
                       bool                     expected)
  {
    var validator  = new EventSubscriptionRequestValidator();
    var validation = validator.TestValidate(request);

    Console.WriteLine(validation);

    Assert.AreEqual(expected,
                    validation.IsValid);
  }

  public static IEnumerable<TestCaseData> TestCasesTasks()
  {
    TestCaseData CaseTrue(FilterField filterField)
      => new TestCaseData(new EventSubscriptionRequest
                          {
                            TasksFilters = new Filters
                                           {
                                             Or =
                                             {
                                               new FiltersAnd
                                               {
                                                 And =
                                                 {
                                                   filterField,
                                                 },
                                               },
                                             },
                                           },
                          },
                          true).SetArgDisplayNames(filterField.ToString()!);

    TestCaseData CaseFalse(FilterField filterField)
      => new TestCaseData(new EventSubscriptionRequest
                          {
                            TasksFilters = new Filters
                                           {
                                             Or =
                                             {
                                               new FiltersAnd
                                               {
                                                 And =
                                                 {
                                                   filterField,
                                                 },
                                               },
                                             },
                                           },
                          },
                          false).SetArgDisplayNames(filterField.ToString()!);

    foreach (var taskOptionEnumField in Enum.GetValues<TaskOptionEnumField>())
    {
      yield return CaseTrue(ListTasksHelper.CreateListTasksFilterString(taskOptionEnumField,
                                                                        FilterStringOperator.Equal,
                                                                        string.Empty));
    }

    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterString("key",
                                                                      FilterStringOperator.Equal,
                                                                      string.Empty));

    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterString(TaskSummaryEnumField.InitialTaskId,
                                                                      FilterStringOperator.Equal,
                                                                      string.Empty));
    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterString(TaskSummaryEnumField.TaskId,
                                                                      FilterStringOperator.Equal,
                                                                      string.Empty));
    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterString(TaskSummaryEnumField.SessionId,
                                                                      FilterStringOperator.Equal,
                                                                      string.Empty));
    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterString(TaskSummaryEnumField.PodHostname,
                                                                      FilterStringOperator.Equal,
                                                                      string.Empty));
    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterString(TaskSummaryEnumField.OwnerPodId,
                                                                      FilterStringOperator.Equal,
                                                                      string.Empty));

    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterString(TaskSummaryEnumField.Unspecified,
                                                                       FilterStringOperator.Equal,
                                                                       string.Empty));
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterString(TaskSummaryEnumField.Error,
                                                                       FilterStringOperator.Equal,
                                                                       string.Empty));
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterStatus(TaskSummaryEnumField.Status,
                                                                       FilterStatusOperator.Equal,
                                                                       TaskStatus.Cancelled));
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterDate(TaskSummaryEnumField.CreatedAt,
                                                                     FilterDateOperator.After,
                                                                     DateToCompare));
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterDate(TaskSummaryEnumField.ReceivedAt,
                                                                     FilterDateOperator.After,
                                                                     DateToCompare));
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterDate(TaskSummaryEnumField.AcquiredAt,
                                                                     FilterDateOperator.After,
                                                                     DateToCompare));
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterDate(TaskSummaryEnumField.SubmittedAt,
                                                                     FilterDateOperator.After,
                                                                     DateToCompare));
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterDate(TaskSummaryEnumField.StartedAt,
                                                                     FilterDateOperator.After,
                                                                     DateToCompare));
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterDate(TaskSummaryEnumField.EndedAt,
                                                                     FilterDateOperator.After,
                                                                     DateToCompare));
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterDate(TaskSummaryEnumField.PodTtl,
                                                                     FilterDateOperator.After,
                                                                     DateToCompare));
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterDate(TaskSummaryEnumField.CreationToEndDuration,
                                                                     FilterDateOperator.After,
                                                                     DateToCompare));
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterDate(TaskSummaryEnumField.ProcessingToEndDuration,
                                                                     FilterDateOperator.After,
                                                                     DateToCompare));
    yield return CaseFalse(new FilterField());
    yield return CaseFalse(new FilterField
                           {
                             Field = new TaskField(),
                           });
    yield return CaseFalse(new FilterField
                           {
                             Field = new TaskField
                                     {
                                       TaskSummaryField = new TaskSummaryField(),
                                     },
                           });
  }

  public static IEnumerable<TestCaseData> TestCasesResults()
  {
    TestCaseData CaseTrue(Api.gRPC.V1.Results.FilterField filterField)
      => new TestCaseData(new EventSubscriptionRequest
                          {
                            ResultsFilters = new Api.gRPC.V1.Results.Filters
                                             {
                                               Or =
                                               {
                                                 new Api.gRPC.V1.Results.FiltersAnd
                                                 {
                                                   And =
                                                   {
                                                     filterField,
                                                   },
                                                 },
                                               },
                                             },
                          },
                          true).SetArgDisplayNames(filterField.ToString()!);


    TestCaseData CaseFalse(Api.gRPC.V1.Results.FilterField filterField)
      => new TestCaseData(new EventSubscriptionRequest
                          {
                            ResultsFilters = new Api.gRPC.V1.Results.Filters
                                             {
                                               Or =
                                               {
                                                 new Api.gRPC.V1.Results.FiltersAnd
                                                 {
                                                   And =
                                                   {
                                                     filterField,
                                                   },
                                                 },
                                               },
                                             },
                          },
                          false).SetArgDisplayNames(filterField.ToString()!);

    yield return CaseFalse(ListResultsHelper.CreateListResultsFilterDate(ResultRawEnumField.CreatedAt,
                                                                         FilterDateOperator.After,
                                                                         DateToCompare));
    yield return CaseFalse(ListResultsHelper.CreateListResultsFilterDate(ResultRawEnumField.CompletedAt,
                                                                         FilterDateOperator.After,
                                                                         DateToCompare));
    yield return CaseFalse(ListResultsHelper.CreateListResultsFilterStatus(ResultRawEnumField.Status,
                                                                           FilterStatusOperator.Equal,
                                                                           ResultStatus.Aborted));
    yield return CaseFalse(ListResultsHelper.CreateListResultsFilterString(ResultRawEnumField.Unspecified,
                                                                           FilterStringOperator.Equal,
                                                                           string.Empty));

    yield return CaseTrue(ListResultsHelper.CreateListResultsFilterString(ResultRawEnumField.ResultId,
                                                                          FilterStringOperator.Equal,
                                                                          string.Empty));
    yield return CaseTrue(ListResultsHelper.CreateListResultsFilterString(ResultRawEnumField.OwnerTaskId,
                                                                          FilterStringOperator.Equal,
                                                                          string.Empty));
    yield return CaseTrue(ListResultsHelper.CreateListResultsFilterString(ResultRawEnumField.SessionId,
                                                                          FilterStringOperator.Equal,
                                                                          string.Empty));
    yield return CaseTrue(ListResultsHelper.CreateListResultsFilterString(ResultRawEnumField.Name,
                                                                          FilterStringOperator.Equal,
                                                                          string.Empty));
    yield return CaseFalse(new Api.gRPC.V1.Results.FilterField());
    yield return CaseFalse(new Api.gRPC.V1.Results.FilterField
                           {
                             Field = new ResultField(),
                           });
    yield return CaseFalse(new Api.gRPC.V1.Results.FilterField
                           {
                             Field = new ResultField
                                     {
                                       ResultRawField = new ResultRawField
                                                        {
                                                          Field = ResultRawEnumField.Unspecified,
                                                        },
                                     },
                           });
    yield return CaseFalse(new Api.gRPC.V1.Results.FilterField
                           {
                             Field = new ResultField
                                     {
                                       ResultRawField = new ResultRawField(),
                                     },
                           });
  }
}
