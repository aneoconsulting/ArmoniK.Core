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

using ArmoniK.Api.gRPC.V1.Partitions;
using ArmoniK.Core.Common.Storage;

namespace ArmoniK.Core.Common.gRPC;

/// <summary>
///   Provides extension methods for converting gRPC partition requests and filters into LINQ expressions for querying
///   <see cref="PartitionData" /> objects.
///   Includes utilities for mapping gRPC fields and filters to strongly-typed expressions used in partition-related logic.
/// </summary>
public static class ListPartitionsRequestExt
{
  /// <summary>
  ///   Converts gRPC message into the associated <see cref="PartitionData" /> field
  /// </summary>
  /// <param name="sort">The gPRC message</param>
  /// <returns>
  ///   The <see cref="Expression" /> that access the field from the object
  /// </returns>
  /// <exception cref="ArgumentOutOfRangeException">the given message is not recognized</exception>
  public static Expression<Func<PartitionData, object?>> ToField(this ListPartitionsRequest.Types.Sort sort)
    => sort.Field.FieldCase switch
       {
         PartitionField.FieldOneofCase.PartitionRawField => sort.Field.PartitionRawField.Field.ToField(),
         _                                               => throw new ArgumentOutOfRangeException(nameof(sort)),
       };

  /// <summary>
  ///   Converts gRPC message into the associated <see cref="PartitionData" /> field
  /// </summary>
  /// <param name="taskField">The gPRC message field</param>
  /// <returns>
  ///   The <see cref="Expression" /> that access the field from the object
  /// </returns>
  /// <exception cref="ArgumentOutOfRangeException">the given message is not recognized</exception>
  public static Expression<Func<PartitionData, object?>> ToField(this PartitionField taskField)
    => taskField.FieldCase switch
       {
         PartitionField.FieldOneofCase.None              => throw new ArgumentOutOfRangeException(nameof(taskField)),
         PartitionField.FieldOneofCase.PartitionRawField => taskField.PartitionRawField.Field.ToField(),
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
  public static Expression<Func<PartitionData, bool>> ToPartitionFilter(this Filters filters)
  {
    Expression<Func<PartitionData, bool>> expr = data => false;

    if (filters.Or is null || !filters.Or.Any())
    {
      return data => true;
    }

    foreach (var filtersAnd in filters.Or)
    {
      Expression<Func<PartitionData, bool>> exprAnd = data => true;
      foreach (var filterField in filtersAnd.And)
      {
        exprAnd = filterField.ValueConditionCase switch
                  {
                    FilterField.ValueConditionOneofCase.FilterString => exprAnd.ExpressionAnd(filterField.FilterString.Operator.ToFilter(filterField.Field.ToField(),
                                                                                                                                         filterField.FilterString
                                                                                                                                                    .Value)),
                    FilterField.ValueConditionOneofCase.FilterNumber => exprAnd.ExpressionAnd(filterField.FilterNumber.Operator.ToFilter(filterField.Field.ToField(),
                                                                                                                                         filterField.FilterNumber
                                                                                                                                                    .Value)),
                    FilterField.ValueConditionOneofCase.FilterBoolean => exprAnd.ExpressionAnd(filterField.FilterBoolean.Operator.ToFilter(filterField.Field.ToField(),
                                                                                                                                           filterField.FilterBoolean
                                                                                                                                                      .Value)),
                    FilterField.ValueConditionOneofCase.FilterArray => exprAnd.ExpressionAnd(filterField.FilterArray.Operator.ToFilter(filterField.Field.ToField(),
                                                                                                                                       filterField.FilterArray.Value)),
                    _ => throw new ArgumentOutOfRangeException(nameof(filters)),
                  };
      }

      expr = expr.ExpressionOr(exprAnd);
    }

    return expr;
  }
}
