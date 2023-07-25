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
using ArmoniK.Api.gRPC.V1.Sessions;

using Armonik.Api.Grpc.V1.SortDirection;

using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.Helpers;

using NUnit.Framework;

using static Google.Protobuf.WellKnownTypes.Timestamp;

using TaskOptions = ArmoniK.Core.Base.DataStructures.TaskOptions;

namespace ArmoniK.Core.Common.Tests.ListSessionsRequestExt;

[TestFixture(TestOf = typeof(ToSessionDataFilterTest))]
public class ToSessionDataFilterTest
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

  private readonly SessionData sessionData_ = new("SessionId",
                                                  SessionStatus.Running,
                                                  DateTime.UtcNow,
                                                  DateTime.UtcNow,
                                                  new List<string>(),
                                                  Options);

  private static readonly ListSessionsRequest.Types.Sort Sort = new()
                                                                {
                                                                  Direction = SortDirection.Asc,
                                                                  Field = new SessionField
                                                                          {
                                                                            SessionRawField = new SessionRawField
                                                                                              {
                                                                                                Field = SessionRawEnumField.CreatedAt,
                                                                                              },
                                                                          },
                                                                };

  private static Func<SessionData, bool> RequestToFunc(ListSessionsRequest.Types.Sort sort,
                                                       IEnumerable<FilterField>       filterFields)
    => CreateListSessionsRequest(sort,
                                 filterFields)
       .Filters.ToSessionDataFilter()
       .Compile();


  public static ListSessionsRequest CreateListSessionsRequest(ListSessionsRequest.Types.Sort sort,
                                                              IEnumerable<FilterField>       filterFields)
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

  public static FilterField CreateListSessionsFilterString(SessionField         field,
                                                           FilterStringOperator op,
                                                           string               value)
    => new()
       {
         String = new FilterString
                  {
                    Field    = field,
                    Operator = op,
                    Value    = value,
                  },
       };


  public static FilterField CreateListSessionsFilterString(SessionRawEnumField  field,
                                                           FilterStringOperator op,
                                                           string               value)
    => CreateListSessionsFilterString(new SessionField
                                      {
                                        SessionRawField = new SessionRawField
                                                          {
                                                            Field = field,
                                                          },
                                      },
                                      op,
                                      value);

  public static FilterField CreateListSessionsFilterString(TaskOptionEnumField  field,
                                                           FilterStringOperator op,
                                                           string               value)
    => CreateListSessionsFilterString(new SessionField
                                      {
                                        TaskOptionField = new TaskOptionField
                                                          {
                                                            Field = field,
                                                          },
                                      },
                                      op,
                                      value);

  public static FilterField CreateListSessionsFilterNumber(SessionRawEnumField  field,
                                                           FilterNumberOperator op,
                                                           long                 value)
    => new()
       {
         Number = new FilterNumber
                  {
                    Field = new SessionField
                            {
                              SessionRawField = new SessionRawField
                                                {
                                                  Field = field,
                                                },
                            },
                    Operator = op,
                    Value    = value,
                  },
       };

  public static FilterField CreateListSessionsFilterArray(SessionRawEnumField field,
                                                          FilterArrayOperator op,
                                                          string              value)
    => new()
       {
         Array = new FilterArray
                 {
                   Field = new SessionField
                           {
                             SessionRawField = new SessionRawField
                                               {
                                                 Field = field,
                                               },
                           },
                   Operator = op,
                   Value    = value,
                 },
       };

  public static FilterField CreateListSessionsFilterStatus(SessionRawEnumField  field,
                                                           FilterStatusOperator op,
                                                           SessionStatus        value)
    => new()
       {
         Status = new FilterStatus
                  {
                    Field = new SessionField
                            {
                              SessionRawField = new SessionRawField
                                                {
                                                  Field = field,
                                                },
                            },
                    Operator = op,
                    Value    = value,
                  },
       };

  public static FilterField CreateListSessionsFilterDate(SessionRawEnumField field,
                                                         FilterDateOperator  op,
                                                         DateTime            value)
    => new()
       {
         Date = new FilterDate
                {
                  Field = new SessionField
                          {
                            SessionRawField = new SessionRawField
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
                    func.Invoke(sessionData_));
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

    yield return CaseTrue(CreateListSessionsFilterStatus(SessionRawEnumField.Status,
                                                         FilterStatusOperator.Equal,
                                                         SessionStatus.Running));
    yield return CaseFalse(CreateListSessionsFilterStatus(SessionRawEnumField.Status,
                                                          FilterStatusOperator.Equal,
                                                          SessionStatus.Cancelled));

    yield return CaseTrue(CreateListSessionsFilterString(SessionRawEnumField.SessionId,
                                                         FilterStringOperator.Equal,
                                                         "SessionId"));
    yield return CaseFalse(CreateListSessionsFilterString(SessionRawEnumField.SessionId,
                                                          FilterStringOperator.Equal,
                                                          "BadSessionId"));

    yield return CaseTrue(CreateListSessionsFilterString(TaskOptionEnumField.ApplicationName,
                                                         FilterStringOperator.Equal,
                                                         "applicationName"));
    yield return CaseFalse(CreateListSessionsFilterString(TaskOptionEnumField.ApplicationName,
                                                          FilterStringOperator.Equal,
                                                          "BadApplicationName"));

    yield return CaseTrue(CreateListSessionsFilterString(TaskOptionEnumField.ApplicationVersion,
                                                         FilterStringOperator.Equal,
                                                         "applicationVersion"));
    yield return CaseFalse(CreateListSessionsFilterString(TaskOptionEnumField.ApplicationVersion,
                                                          FilterStringOperator.Equal,
                                                          "BadVersion"));

    yield return CaseTrue(CreateListSessionsFilterDate(SessionRawEnumField.CreatedAt,
                                                       FilterDateOperator.After,
                                                       DateTime.UtcNow));
    yield return CaseFalse(CreateListSessionsFilterDate(SessionRawEnumField.CreatedAt,
                                                        FilterDateOperator.Before,
                                                        DateTime.UtcNow));

    yield return CaseTrue(CreateListSessionsFilterDate(SessionRawEnumField.CancelledAt,
                                                       FilterDateOperator.After,
                                                       DateTime.UtcNow));
    yield return CaseFalse(CreateListSessionsFilterDate(SessionRawEnumField.CancelledAt,
                                                        FilterDateOperator.Before,
                                                        DateTime.UtcNow));
  }
}
