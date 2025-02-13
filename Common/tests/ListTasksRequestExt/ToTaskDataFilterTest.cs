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
using ArmoniK.Api.gRPC.V1.SortDirection;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

using Output = ArmoniK.Core.Common.Storage.Output;
using TaskOptions = ArmoniK.Core.Base.DataStructures.TaskOptions;
using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

namespace ArmoniK.Core.Common.Tests.ListTasksRequestExt;

[TestFixture(TestOf = typeof(ToTaskDataFilterTest))]
public class ToTaskDataFilterTest
{
  private static readonly TaskOptions Options = new(new Dictionary<string, string>
                                                    {
                                                      {
                                                        "key1", "val1"
                                                      },
                                                    },
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
                                            new List<string>
                                            {
                                              "parentId",
                                            },
                                            new List<string>(),
                                            new Dictionary<string, bool>(),
                                            new List<string>(),
                                            "InitialTaskId",
                                            "CreatedBy",
                                            new List<string>(),
                                            TaskStatus.Completed,
                                            "StatusMessage",
                                            Options,
                                            new DateTime(2020,
                                                         3,
                                                         14),
                                            new DateTime(2020,
                                                         3,
                                                         16),
                                            null,
                                            null,
                                            null,
                                            null,
                                            null,
                                            null,
                                            null,
                                            TimeSpan.FromDays(1),
                                            TimeSpan.FromDays(2),
                                            TimeSpan.FromDays(3),
                                            new Output(OutputStatus.Success,
                                                       ""));

  private static readonly DateTime DateToCompare = new DateTime(2020,
                                                                3,
                                                                15).ToUniversalTime();

  private static readonly TimeSpan TimeSpanToCompare = TimeSpan.FromDays(1.5);
  private static readonly TimeSpan TimeSpan3Days     = TimeSpan.FromDays(3);

  private static readonly ListTasksRequest.Types.Sort Sort = new()
                                                             {
                                                               Direction = SortDirection.Asc,
                                                               Field = new TaskField
                                                                       {
                                                                         TaskSummaryField = new TaskSummaryField
                                                                                            {
                                                                                              Field = TaskSummaryEnumField.StartedAt,
                                                                                            },
                                                                       },
                                                             };

  private static Func<TaskData, bool> RequestToFunc(ListTasksRequest.Types.Sort sort,
                                                    IEnumerable<FilterField>    filterFields)
    => ListTasksHelper.CreateListSessionsRequest(sort,
                                                 filterFields)
                      .Filters.ToTaskDataFilter()
                      .Compile();

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
    TestCaseData CaseTrue(FilterField filterField)
      => new TestCaseData(new[]
                          {
                            filterField,
                          },
                          true).SetArgDisplayNames(filterField.ToString());

    TestCaseData CaseFalse(FilterField filterField)
      => new TestCaseData(new[]
                          {
                            filterField,
                          },
                          false).SetArgDisplayNames(filterField.ToString());

    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterDate(TaskSummaryEnumField.CreatedAt,
                                                                    FilterDateOperator.Before,
                                                                    DateToCompare));
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterDate(TaskSummaryEnumField.CreatedAt,
                                                                     FilterDateOperator.After,
                                                                     DateToCompare));

    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterDate(TaskSummaryEnumField.SubmittedAt,
                                                                    FilterDateOperator.AfterOrEqual,
                                                                    DateToCompare));
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterDate(TaskSummaryEnumField.SubmittedAt,
                                                                     FilterDateOperator.BeforeOrEqual,
                                                                     DateToCompare));

    // end date is null
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterDate(TaskSummaryEnumField.EndedAt,
                                                                     FilterDateOperator.After,
                                                                     DateToCompare));
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterDate(TaskSummaryEnumField.EndedAt,
                                                                     FilterDateOperator.Before,
                                                                     DateToCompare));

    // start date is null
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterDate(TaskSummaryEnumField.StartedAt,
                                                                     FilterDateOperator.After,
                                                                     DateToCompare));
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterDate(TaskSummaryEnumField.StartedAt,
                                                                     FilterDateOperator.Before,
                                                                     DateToCompare));
    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterDate(TaskSummaryEnumField.StartedAt,
                                                                    FilterDateOperator.Equal,
                                                                    null));
    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterDate(TaskSummaryEnumField.ProcessedAt,
                                                                    FilterDateOperator.Equal,
                                                                    null));
    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterDate(TaskSummaryEnumField.FetchedAt,
                                                                    FilterDateOperator.Equal,
                                                                    null));
    yield return CaseTrue(new FilterField
                          {
                            Field = new TaskField
                                    {
                                      TaskSummaryField = new TaskSummaryField
                                                         {
                                                           Field = TaskSummaryEnumField.StartedAt,
                                                         },
                                    },
                            FilterDate = new FilterDate
                                         {
                                           Operator = FilterDateOperator.Equal,
                                         },
                          });

    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterString(TaskSummaryEnumField.SessionId,
                                                                      FilterStringOperator.Equal,
                                                                      "SessionId"));
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterString(TaskSummaryEnumField.SessionId,
                                                                       FilterStringOperator.Equal,
                                                                       "BadSessionId"));

    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterString(TaskOptionEnumField.PartitionId,
                                                                      FilterStringOperator.Equal,
                                                                      "part1"));
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterString(TaskOptionEnumField.PartitionId,
                                                                       FilterStringOperator.Equal,
                                                                       "BadPartitionId"));

    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterString(TaskSummaryEnumField.PayloadId,
                                                                      FilterStringOperator.Equal,
                                                                      "PayloadId"));
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterString(TaskSummaryEnumField.PayloadId,
                                                                       FilterStringOperator.Equal,
                                                                       "BadPayloadId"));

    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterString(TaskSummaryEnumField.CreatedBy,
                                                                      FilterStringOperator.Equal,
                                                                      "CreatedBy"));
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterString(TaskSummaryEnumField.CreatedBy,
                                                                       FilterStringOperator.Equal,
                                                                       "BadCreatedBy"));

    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterStatus(TaskSummaryEnumField.Status,
                                                                      FilterStatusOperator.Equal,
                                                                      Api.gRPC.V1.TaskStatus.Completed));
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterStatus(TaskSummaryEnumField.Status,
                                                                       FilterStatusOperator.Equal,
                                                                       Api.gRPC.V1.TaskStatus.Cancelling));

    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterNumber(TaskOptionEnumField.MaxRetries,
                                                                      FilterNumberOperator.LessThanOrEqual,
                                                                      5));
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterNumber(TaskOptionEnumField.MaxRetries,
                                                                       FilterNumberOperator.LessThan,
                                                                       5));

    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterString("key1",
                                                                      FilterStringOperator.Equal,
                                                                      "val1"));
    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterString("key1",
                                                                      FilterStringOperator.Contains,
                                                                      "val1"));
    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterString("key1",
                                                                      FilterStringOperator.StartsWith,
                                                                      "val1"));
    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterString("key1",
                                                                      FilterStringOperator.EndsWith,
                                                                      "val1"));
    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterDuration(TaskSummaryEnumField.ProcessingToEndDuration,
                                                                        FilterDurationOperator.ShorterThanOrEqual,
                                                                        TimeSpanToCompare));
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterDuration(TaskSummaryEnumField.CreationToEndDuration,
                                                                         FilterDurationOperator.ShorterThanOrEqual,
                                                                         TimeSpanToCompare));
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterDuration(TaskSummaryEnumField.ProcessingToEndDuration,
                                                                         FilterDurationOperator.LongerThan,
                                                                         TimeSpanToCompare));
    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterDuration(TaskSummaryEnumField.CreationToEndDuration,
                                                                        FilterDurationOperator.LongerThan,
                                                                        TimeSpanToCompare));
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterDuration(TaskSummaryEnumField.ReceivedToEndDuration,
                                                                         FilterDurationOperator.ShorterThan,
                                                                         TimeSpanToCompare));
    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterDuration(TaskSummaryEnumField.ReceivedToEndDuration,
                                                                        FilterDurationOperator.LongerThanOrEqual,
                                                                        TimeSpanToCompare));
    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterDuration(TaskSummaryEnumField.ReceivedToEndDuration,
                                                                        FilterDurationOperator.Equal,
                                                                        TimeSpan3Days));
    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterDuration(TaskSummaryEnumField.ReceivedToEndDuration,
                                                                        FilterDurationOperator.LongerThanOrEqual,
                                                                        TimeSpan3Days));
    yield return CaseTrue(ListTasksHelper.CreateListTasksFilterDuration(TaskSummaryEnumField.ReceivedToEndDuration,
                                                                        FilterDurationOperator.ShorterThanOrEqual,
                                                                        TimeSpan3Days));
    yield return CaseFalse(ListTasksHelper.CreateListTasksFilterDuration(TaskSummaryEnumField.ReceivedToEndDuration,
                                                                         FilterDurationOperator.NotEqual,
                                                                         TimeSpan3Days));
  }
}
