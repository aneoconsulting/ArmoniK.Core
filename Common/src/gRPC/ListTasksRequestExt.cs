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
using System.Linq;
using System.Linq.Expressions;

using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Core.Common.Storage;

namespace ArmoniK.Core.Common.gRPC;

public static class ListTasksRequestExt
{
  /// <summary>
  ///   Converts gRPC message into the associated <see cref="TaskData" /> field
  /// </summary>
  /// <param name="sort">The gPRC message</param>
  /// <returns>
  ///   The <see cref="Expression" /> that access the field from the object
  /// </returns>
  /// <exception cref="ArgumentOutOfRangeException">the given message is not recognized</exception>
  public static Expression<Func<TaskData, object?>> ToField(this ListTasksRequest.Types.Sort sort)
    => sort.Field.FieldCase switch
       {
         TaskField.FieldOneofCase.TaskSummaryField       => sort.Field.TaskSummaryField.Field.ToField(),
         TaskField.FieldOneofCase.TaskOptionField        => sort.Field.TaskOptionField.Field.ToField(),
         TaskField.FieldOneofCase.TaskOptionGenericField => sort.Field.TaskOptionGenericField.ToField(),
         _                                               => throw new ArgumentOutOfRangeException(nameof(sort)),
       };

  public static Expression<Func<TaskData, object?>> ToField(this TaskField taskField)
    => taskField.FieldCase switch
       {
         TaskField.FieldOneofCase.None                   => throw new ArgumentOutOfRangeException(nameof(taskField)),
         TaskField.FieldOneofCase.TaskSummaryField       => taskField.TaskSummaryField.Field.ToField(),
         TaskField.FieldOneofCase.TaskOptionField        => taskField.TaskOptionField.Field.ToField(),
         TaskField.FieldOneofCase.TaskOptionGenericField => taskField.TaskOptionGenericField.ToField(),
         _                                               => throw new ArgumentOutOfRangeException(nameof(taskField)),
       };

  /// <summary>
  ///   Converts gRPC message filters into an <see cref="Expression" /> that represents the filter conditions
  /// </summary>
  /// <param name="filters">The gPRC filters</param>
  /// <returns>
  ///   The <see cref="Expression" /> that represents the filter conditions
  /// </returns>
  /// <exception cref="ArgumentOutOfRangeException">the given message is not recognized</exception>
  public static Expression<Func<TaskData, bool>> ToTaskDataFilter(this Filters filters)
  {
    Expression<Func<TaskData, bool>> expr = data => false;

    if (filters.Or is null || !filters.Or.Any())
    {
      return data => true;
    }

    foreach (var filtersAnd in filters.Or)
    {
      Expression<Func<TaskData, bool>> exprAnd = data => true;
      foreach (var filterField in filtersAnd.And)
      {
        switch (filterField.ValueConditionCase)
        {
          case FilterField.ValueConditionOneofCase.FilterString:
            exprAnd = exprAnd.ExpressionAnd(filterField.FilterString.Operator.ToFilter(filterField.Field.ToField(),
                                                                                       filterField.FilterString.Value));
            break;
          case FilterField.ValueConditionOneofCase.FilterNumber:
            exprAnd = exprAnd.ExpressionAnd(filterField.FilterNumber.Operator.ToFilter(filterField.Field.ToField(),
                                                                                       filterField.FilterNumber.Value));
            break;
          case FilterField.ValueConditionOneofCase.FilterBoolean:
            exprAnd = exprAnd.ExpressionAnd(filterField.FilterBoolean.Operator.ToFilter(filterField.Field.ToField(),
                                                                                        filterField.FilterBoolean.Value));
            break;
          case FilterField.ValueConditionOneofCase.FilterStatus:
            exprAnd = exprAnd.ExpressionAnd(filterField.FilterStatus.Operator.ToFilter(filterField.Field.ToField(),
                                                                                       filterField.FilterStatus.Value));
            break;
          case FilterField.ValueConditionOneofCase.FilterDate:
            var val = filterField.FilterDate.Value;
            exprAnd = exprAnd.ExpressionAnd(filterField.FilterDate.Operator.ToFilter(filterField.Field.ToField(),
                                                                                     val?.ToDateTime()));
            break;
          case FilterField.ValueConditionOneofCase.FilterArray:
            exprAnd = exprAnd.ExpressionAnd(filterField.FilterArray.Operator.ToFilter(filterField.Field.ToField(),
                                                                                      filterField.FilterArray.Value));
            break;
          case FilterField.ValueConditionOneofCase.FilterDuration:
            exprAnd = exprAnd.ExpressionAnd(filterField.FilterDuration.Operator.ToFilter(filterField.Field.ToField(),
                                                                                         filterField.FilterDuration.Value?.ToTimeSpan()));
            break;
          case FilterField.ValueConditionOneofCase.None:
          default:
            throw new ArgumentOutOfRangeException(nameof(filters));
        }
      }

      expr = expr.ExpressionOr(exprAnd);
    }

    return expr;
  }
}
