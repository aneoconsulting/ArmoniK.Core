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

using Armonik.Api.gRPC.V1;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Results;

using Armonik.Api.Grpc.V1.SortDirection;

using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.Helpers;

using NUnit.Framework;

using static Google.Protobuf.WellKnownTypes.Timestamp;

namespace ArmoniK.Core.Common.Tests.ListResultsRequestExt;

[TestFixture(TestOf = typeof(ToResultFilterTest))]
public class ToResultFilterTest
{
  private readonly Result result_ = new("SessionId",
                                        "ResultId",
                                        "Name",
                                        "OwnerTaskId",
                                        ResultStatus.Created,
                                        new List<string>(),
                                        DateTime.UtcNow,
                                        Array.Empty<byte>());

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
    => CreateListResultsRequest(sort,
                                filterFields)
       .Filters.ToResultFilter()
       .Compile();


  public static ListResultsRequest CreateListResultsRequest(ListResultsRequest.Types.Sort sort,
                                                            IEnumerable<FilterField>      filterFields)
    => new()
       {
         Filters = new Filters
                   {
                     Filters_ = new FiltersOr
                                {
                                  Filters =
                                  {
                                    new FiltersAnd
                                    {
                                      Filters =
                                      {
                                        filterFields,
                                      },
                                    },
                                  },
                                },
                   },
         Sort = sort,
       };

  public static FilterField CreateListResultsFilterString(ResultRawEnumField   field,
                                                          FilterStringOperator op,
                                                          string               value)
    => new()
       {
         String = new FilterString
                  {
                    Field = new ResultField
                            {
                              ResultRawField = new ResultRawField
                                               {
                                                 Field = field,
                                               },
                            },
                    Operator = op,
                    Value    = value,
                  },
       };

  public static FilterField CreateListResultsFilterNumber(ResultRawEnumField   field,
                                                          FilterNumberOperator op,
                                                          long                 value)
    => new()
       {
         Number = new FilterNumber
                  {
                    Field = new ResultField
                            {
                              ResultRawField = new ResultRawField
                                               {
                                                 Field = field,
                                               },
                            },
                    Operator = op,
                    Value    = value,
                  },
       };

  public static FilterField CreateListResultsFilterArray(ResultRawEnumField  field,
                                                         FilterArrayOperator op,
                                                         string              value)
    => new()
       {
         Array = new FilterArray
                 {
                   Field = new ResultField
                           {
                             ResultRawField = new ResultRawField
                                              {
                                                Field = field,
                                              },
                           },
                   Operator = op,
                   Value    = value,
                 },
       };

  public static FilterField CreateListResultsFilterStatus(ResultRawEnumField   field,
                                                          FilterStatusOperator op,
                                                          ResultStatus         value)
    => new()
       {
         Status = new FilterStatus
                  {
                    Field = new ResultField
                            {
                              ResultRawField = new ResultRawField
                                               {
                                                 Field = field,
                                               },
                            },
                    Operator = op,
                    Value    = value,
                  },
       };

  public static FilterField CreateListResultsFilterDate(ResultRawEnumField field,
                                                        FilterDateOperator op,
                                                        DateTime           value)
    => new()
       {
         Date = new FilterDate
                {
                  Field = new ResultField
                          {
                            ResultRawField = new ResultRawField
                                             {
                                               Field = field,
                                             },
                          },
                  Operator = op,
                  Value    = FromDateTime(value),
                },
       };

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
                          true).SetArgDisplayNames(filterField.ToDisplay());

    TestCaseData CaseFalse(FilterField filterField)
      => new TestCaseData(new[]
                          {
                            filterField,
                          },
                          false).SetArgDisplayNames(filterField.ToDisplay());

    yield return CaseTrue(CreateListResultsFilterStatus(ResultRawEnumField.Status,
                                                        FilterStatusOperator.Equal,
                                                        ResultStatus.Created));
    yield return CaseFalse(CreateListResultsFilterStatus(ResultRawEnumField.Status,
                                                         FilterStatusOperator.Equal,
                                                         ResultStatus.Aborted));
    yield return CaseTrue(CreateListResultsFilterStatus(ResultRawEnumField.Status,
                                                        FilterStatusOperator.NotEqual,
                                                        ResultStatus.Aborted));

    yield return CaseTrue(CreateListResultsFilterString(ResultRawEnumField.SessionId,
                                                        FilterStringOperator.Equal,
                                                        "SessionId"));
    yield return CaseFalse(CreateListResultsFilterString(ResultRawEnumField.SessionId,
                                                         FilterStringOperator.Equal,
                                                         "BadSessionId"));

    yield return CaseTrue(CreateListResultsFilterString(ResultRawEnumField.Name,
                                                        FilterStringOperator.Equal,
                                                        "Name"));
    yield return CaseFalse(CreateListResultsFilterString(ResultRawEnumField.Name,
                                                         FilterStringOperator.Equal,
                                                         "BadName"));

    yield return CaseTrue(CreateListResultsFilterString(ResultRawEnumField.OwnerTaskId,
                                                        FilterStringOperator.Equal,
                                                        "OwnerTaskId"));
    yield return CaseFalse(CreateListResultsFilterString(ResultRawEnumField.OwnerTaskId,
                                                         FilterStringOperator.Equal,
                                                         "BadOwnerTaskId"));

    yield return CaseTrue(CreateListResultsFilterDate(ResultRawEnumField.CreatedAt,
                                                      FilterDateOperator.After,
                                                      DateTime.UtcNow));
    yield return CaseFalse(CreateListResultsFilterDate(ResultRawEnumField.CreatedAt,
                                                       FilterDateOperator.Before,
                                                       DateTime.UtcNow));
  }
}
