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
using ArmoniK.Api.gRPC.V1.Results;

using static Google.Protobuf.WellKnownTypes.Timestamp;

namespace ArmoniK.Core.Common.Tests.ListResultsRequestExt;

public static class ListResultsHelper
{
  public static ListResultsRequest CreateListResultsRequest(ListResultsRequest.Types.Sort sort,
                                                            IEnumerable<FilterField>      filterFields)
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

  public static FilterField CreateListResultsFilterString(ResultRawEnumField   field,
                                                          FilterStringOperator op,
                                                          string               value)
    => new()
       {
         Field = new ResultField
                 {
                   ResultRawField = new ResultRawField
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


  public static FilterField CreateListResultsFilterArray(ResultRawEnumField  field,
                                                         FilterArrayOperator op,
                                                         string              value)
    => new()
       {
         Field = new ResultField
                 {
                   ResultRawField = new ResultRawField
                                    {
                                      Field = field,
                                    },
                 },
         FilterArray = new FilterArray
                       {
                         Operator = op,
                         Value    = value,
                       },
       };

  public static FilterField CreateListResultsFilterStatus(ResultRawEnumField   field,
                                                          FilterStatusOperator op,
                                                          ResultStatus         value)
    => new()
       {
         Field = new ResultField
                 {
                   ResultRawField = new ResultRawField
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

  public static FilterField CreateListResultsFilterDate(ResultRawEnumField field,
                                                        FilterDateOperator op,
                                                        DateTime           value)
    => new()
       {
         Field = new ResultField
                 {
                   ResultRawField = new ResultRawField
                                    {
                                      Field = field,
                                    },
                 },
         FilterDate = new FilterDate
                      {
                        Operator = op,
                        Value    = FromDateTime(value),
                      },
       };
}
