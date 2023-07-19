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

using Armonik.Api.gRPC.V1.Tasks;

using ArmoniK.Api.gRPC.V1.Tasks;

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

  public static FilterField CreateListTasksFilterString(TaskField            field,
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
         Number = new FilterNumber
                  {
                    Field = new TaskField
                            {
                              TaskOptionField = new TaskOptionField
                                                {
                                                  Field = field,
                                                },
                            },
                    Operator = op,
                    Value    = value,
                  },
       };

  public static FilterField CreateListTasksFilterStatus(TaskSummaryEnumField field,
                                                        FilterStatusOperator op,
                                                        TaskStatus           value)
    => new()
       {
         Status = new FilterStatus
                  {
                    Field = new TaskField
                            {
                              TaskSummaryField = new TaskSummaryField
                                                 {
                                                   Field = field,
                                                 },
                            },
                    Operator = op,
                    Value    = value,
                  },
       };

  public static FilterField CreateListTasksFilterDate(TaskSummaryEnumField field,
                                                      FilterDateOperator   op,
                                                      DateTime?            value)
    => new()
       {
         Date = new FilterDate
                {
                  Field = new TaskField
                          {
                            TaskSummaryField = new TaskSummaryField
                                               {
                                                 Field = field,
                                               },
                          },
                  Operator = op,
                  Value = value is null
                            ? null
                            : FromDateTime(value.Value),
                },
       };
}
