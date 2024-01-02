// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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

using static Google.Protobuf.WellKnownTypes.Duration;
using static Google.Protobuf.WellKnownTypes.Timestamp;

namespace ArmoniK.Core.Common.Tests.ListTasksRequestExt;

public static class ListTasksHelper
{
  public static ListTasksRequest CreateListSessionsRequest(ListTasksRequest.Types.Sort sort,
                                                           IEnumerable<FilterField>    filterFields)
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

  public static FilterField CreateListTasksFilterString(TaskField            field,
                                                        FilterStringOperator op,
                                                        string               value)
    => new()
       {
         Field = field,
         FilterString = new FilterString
                        {
                          Operator = op,
                          Value    = value,
                        },
       };


  public static FilterField CreateListTasksFilterString(TaskSummaryEnumField field,
                                                        FilterStringOperator op,
                                                        string               value)
    => CreateListTasksFilterString(new TaskField
                                   {
                                     TaskSummaryField = new TaskSummaryField
                                                        {
                                                          Field = field,
                                                        },
                                   },
                                   op,
                                   value);

  public static FilterField CreateListTasksFilterString(TaskOptionEnumField  field,
                                                        FilterStringOperator op,
                                                        string               value)
    => CreateListTasksFilterString(new TaskField
                                   {
                                     TaskOptionField = new TaskOptionField
                                                       {
                                                         Field = field,
                                                       },
                                   },
                                   op,
                                   value);

  public static FilterField CreateListTasksFilterString(string               field,
                                                        FilterStringOperator op,
                                                        string               value)
    => CreateListTasksFilterString(new TaskField
                                   {
                                     TaskOptionGenericField = new TaskOptionGenericField
                                                              {
                                                                Field = field,
                                                              },
                                   },
                                   op,
                                   value);

  public static FilterField CreateListTasksFilterNumber(TaskOptionEnumField  field,
                                                        FilterNumberOperator op,
                                                        long                 value)
    => new()
       {
         Field = new TaskField
                 {
                   TaskOptionField = new TaskOptionField
                                     {
                                       Field = field,
                                     },
                 },

         FilterNumber = new FilterNumber
                        {
                          Operator = op,
                          Value    = value,
                        },
       };

  public static FilterField CreateListTasksFilterStatus(TaskSummaryEnumField field,
                                                        FilterStatusOperator op,
                                                        TaskStatus           value)
    => new()
       {
         Field = new TaskField
                 {
                   TaskSummaryField = new TaskSummaryField
                                      {
                                        Field = field,
                                      },
                 },
         FilterStatus = new FilterStatus
                        {
                          Operator = op,
                          Value    = value,
                        },
       };

  public static FilterField CreateListTasksFilterDate(TaskSummaryEnumField field,
                                                      FilterDateOperator   op,
                                                      DateTime?            value)
    => new()
       {
         Field = new TaskField
                 {
                   TaskSummaryField = new TaskSummaryField
                                      {
                                        Field = field,
                                      },
                 },
         FilterDate = new FilterDate
                      {
                        Operator = op,
                        Value = value is null
                                  ? null
                                  : FromDateTime(value.Value),
                      },
       };

  public static FilterField CreateListTasksFilterDuration(TaskSummaryEnumField   field,
                                                          FilterDurationOperator op,
                                                          TimeSpan?              value)
    => new()
       {
         Field = new TaskField
                 {
                   TaskSummaryField = new TaskSummaryField
                                      {
                                        Field = field,
                                      },
                 },
         FilterDuration = new FilterDuration
                          {
                            Operator = op,
                            Value = value is null
                                      ? null
                                      : FromTimeSpan(value.Value),
                          },
       };
}
