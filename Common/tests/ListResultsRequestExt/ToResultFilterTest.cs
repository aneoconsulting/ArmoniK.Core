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
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.SortDirection;
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

using ResultStatus = ArmoniK.Core.Common.Storage.ResultStatus;

namespace ArmoniK.Core.Common.Tests.ListResultsRequestExt;

[TestFixture(TestOf = typeof(ToResultFilterTest))]
public class ToResultFilterTest
{
  private readonly Result result_ = new("SessionId",
                                        "ResultId",
                                        "Name",
                                        "CreatedBy",
                                        "OwnerTaskId",
                                        ResultStatus.Created,
                                        new List<string>(),
                                        DateTime.UtcNow,
                                        null,
                                        0,
                                        Array.Empty<byte>(),
                                        false);

  private static readonly ListResultsRequest.Types.Sort Sort = new()
                                                               {
                                                                 Field = new ResultField
                                                                         {
                                                                           ResultRawField = new ResultRawField
                                                                                            {
                                                                                              Field = ResultRawEnumField.CreatedAt,
                                                                                            },
                                                                         },
                                                                 Direction = SortDirection.Asc,
                                                               };

  private static Func<Result, bool> RequestToFunc(ListResultsRequest.Types.Sort sort,
                                                  IEnumerable<FilterField>      filterFields)
    => ListResultsHelper.CreateListResultsRequest(sort,
                                                  filterFields)
                        .Filters.ToResultFilter()
                        .Compile();


  [Test]
  [TestCaseSource(nameof(TestCasesFilter))]
  public void Filter(IEnumerable<FilterField> filterFields,
                     bool                     expected)
  {
    var func = RequestToFunc(Sort,
                             filterFields);

    Assert.AreEqual(expected,
                    func.Invoke(result_));
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

    yield return CaseTrue(ListResultsHelper.CreateListResultsFilterStatus(ResultRawEnumField.Status,
                                                                          FilterStatusOperator.Equal,
                                                                          Api.gRPC.V1.ResultStatus.Created));
    yield return CaseFalse(ListResultsHelper.CreateListResultsFilterStatus(ResultRawEnumField.Status,
                                                                           FilterStatusOperator.Equal,
                                                                           Api.gRPC.V1.ResultStatus.Aborted));
    yield return CaseTrue(ListResultsHelper.CreateListResultsFilterStatus(ResultRawEnumField.Status,
                                                                          FilterStatusOperator.NotEqual,
                                                                          Api.gRPC.V1.ResultStatus.Aborted));

    yield return CaseTrue(ListResultsHelper.CreateListResultsFilterString(ResultRawEnumField.SessionId,
                                                                          FilterStringOperator.Equal,
                                                                          "SessionId"));
    yield return CaseFalse(ListResultsHelper.CreateListResultsFilterString(ResultRawEnumField.SessionId,
                                                                           FilterStringOperator.Equal,
                                                                           "BadSessionId"));

    yield return CaseTrue(ListResultsHelper.CreateListResultsFilterString(ResultRawEnumField.Name,
                                                                          FilterStringOperator.Equal,
                                                                          "Name"));
    yield return CaseFalse(ListResultsHelper.CreateListResultsFilterString(ResultRawEnumField.Name,
                                                                           FilterStringOperator.Equal,
                                                                           "BadName"));

    yield return CaseTrue(ListResultsHelper.CreateListResultsFilterString(ResultRawEnumField.CreatedBy,
                                                                          FilterStringOperator.Equal,
                                                                          "CreatedBy"));
    yield return CaseFalse(ListResultsHelper.CreateListResultsFilterString(ResultRawEnumField.CreatedBy,
                                                                           FilterStringOperator.Equal,
                                                                           "BadCreatedBy"));

    yield return CaseTrue(ListResultsHelper.CreateListResultsFilterString(ResultRawEnumField.OwnerTaskId,
                                                                          FilterStringOperator.Equal,
                                                                          "OwnerTaskId"));
    yield return CaseFalse(ListResultsHelper.CreateListResultsFilterString(ResultRawEnumField.OwnerTaskId,
                                                                           FilterStringOperator.Equal,
                                                                           "BadOwnerTaskId"));

    yield return CaseTrue(ListResultsHelper.CreateListResultsFilterDate(ResultRawEnumField.CreatedAt,
                                                                        FilterDateOperator.After,
                                                                        DateTime.UtcNow));
    yield return CaseFalse(ListResultsHelper.CreateListResultsFilterDate(ResultRawEnumField.CreatedAt,
                                                                         FilterDateOperator.Before,
                                                                         DateTime.UtcNow));

    yield return CaseTrue(ListResultsHelper.CreateListResultsFilterDate(ResultRawEnumField.CompletedAt,
                                                                        FilterDateOperator.Equal,
                                                                        null));
    yield return CaseFalse(ListResultsHelper.CreateListResultsFilterDate(ResultRawEnumField.CompletedAt,
                                                                         FilterDateOperator.BeforeOrEqual,
                                                                         DateTime.UtcNow));
    yield return CaseFalse(ListResultsHelper.CreateListResultsFilterDate(ResultRawEnumField.CompletedAt,
                                                                         FilterDateOperator.AfterOrEqual,
                                                                         DateTime.UtcNow));

    yield return CaseTrue(ListResultsHelper.CreateListResultsFilterNumber(ResultRawEnumField.Size,
                                                                          FilterNumberOperator.LessThan,
                                                                          1));
    yield return CaseFalse(ListResultsHelper.CreateListResultsFilterNumber(ResultRawEnumField.Size,
                                                                           FilterNumberOperator.GreaterThan,
                                                                           1));
  }
}
