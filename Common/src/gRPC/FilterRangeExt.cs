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
using System.Linq;
using System.Linq.Expressions;

using Armonik.Api.gRPC.V1;

namespace ArmoniK.Core.Common.gRPC;

/// <summary>
///   Class to extend gRPC messages to convert them in <see cref="ExpressionType" /> in order to convert them in requests
///   to the database
/// </summary>
public static class FilterRangeExt
{
  /// <summary>
  ///   Convert gRPC field to <see cref="ExpressionType" /> that represent which operation has to be performed
  /// </summary>
  /// <param name="filterOperator">The gRPC Enum</param>
  /// <returns>
  ///   The <see cref="ExpressionType" /> that represent the operation
  /// </returns>
  public static ExpressionType ToExpressionType(this FilterStatusOperator filterOperator)
    => filterOperator switch
       {
         FilterStatusOperator.Unspecified => throw new ArgumentOutOfRangeException(),
         FilterStatusOperator.Equal       => ExpressionType.Equal,
         FilterStatusOperator.NotEqual    => ExpressionType.NotEqual,
         _                                => throw new ArgumentOutOfRangeException(),
       };


  /// <summary>
  ///   Convert gRPC field to <see cref="ExpressionType" /> that represent which operation has to be performed
  /// </summary>
  /// <param name="filterOperator">The gRPC Enum</param>
  /// <returns>
  ///   The <see cref="ExpressionType" /> that represent the operation
  /// </returns>
  public static ExpressionType ToExpressionType(this FilterDateOperator filterOperator)
    => filterOperator switch
       {
         FilterDateOperator.Before        => ExpressionType.LessThan,
         FilterDateOperator.BeforeOrEqual => ExpressionType.LessThanOrEqual,
         FilterDateOperator.Equal         => ExpressionType.Equal,
         FilterDateOperator.NotEqual      => ExpressionType.NotEqual,
         FilterDateOperator.AfterOrEqual  => ExpressionType.GreaterThanOrEqual,
         FilterDateOperator.After         => ExpressionType.GreaterThan,
         FilterDateOperator.Unspecified   => throw new ArgumentOutOfRangeException(),
         _                                => throw new ArgumentOutOfRangeException(),
       };

  /// <summary>
  ///   Convert gRPC field to <see cref="ExpressionType" /> that represent which operation has to be performed
  /// </summary>
  /// <param name="filterOperator">The gRPC Enum</param>
  /// <returns>
  ///   The <see cref="ExpressionType" /> that represent the operation
  /// </returns>
  public static ExpressionType ToExpressionType(this FilterNumberOperator filterOperator)
    => filterOperator switch
       {
         FilterNumberOperator.LessThan           => ExpressionType.LessThan,
         FilterNumberOperator.LessThanOrEqual    => ExpressionType.LessThanOrEqual,
         FilterNumberOperator.Equal              => ExpressionType.Equal,
         FilterNumberOperator.NotEqual           => ExpressionType.NotEqual,
         FilterNumberOperator.GreaterThanOrEqual => ExpressionType.GreaterThanOrEqual,
         FilterNumberOperator.GreaterThan        => ExpressionType.GreaterThan,
         FilterNumberOperator.Unspecified        => throw new ArgumentOutOfRangeException(),
         _                                       => throw new ArgumentOutOfRangeException(),
       };

  /// <summary>
  ///   Generate a filter <see cref="Expression" /> for operations on strings
  /// </summary>
  /// <typeparam name="T">Type of the value and field on which the operation is applied</typeparam>
  /// <param name="filterOperator">The gRPC enum that selects the operation</param>
  /// <param name="field">The <see cref="Expression" /> to select the field on which to apply the operation</param>
  /// <param name="value">Value for the operation</param>
  /// <returns>
  ///   The <see cref="Expression" /> that represents the operation on the field with the given value
  /// </returns>
  public static Expression<Func<T, bool>> ToFilter<T>(this FilterStringOperator    filterOperator,
                                                      Expression<Func<T, object?>> field,
                                                      string                       value)
    => filterOperator switch
       {
         FilterStringOperator.Equal => ExpressionBuilders.MakeBinary(field,
                                                                     value,
                                                                     ExpressionType.Equal),
         FilterStringOperator.NotEqual => ExpressionBuilders.MakeBinary(field,
                                                                        value,
                                                                        ExpressionType.NotEqual),
         FilterStringOperator.Contains => ExpressionBuilders.MakeCallString(field,
                                                                            value,
                                                                            nameof(string.Contains)),
         FilterStringOperator.NotContains => ExpressionBuilders.MakeCallString(field,
                                                                               value,
                                                                               nameof(string.Contains),
                                                                               true),
         FilterStringOperator.StartsWith => ExpressionBuilders.MakeCallString(field,
                                                                              value,
                                                                              nameof(string.StartsWith)),
         FilterStringOperator.EndsWith => ExpressionBuilders.MakeCallString(field,
                                                                            value,
                                                                            nameof(string.EndsWith)),
         FilterStringOperator.Unspecified => throw new ArgumentOutOfRangeException(),
         _                                => throw new ArgumentOutOfRangeException(),
       };

  /// <summary>
  ///   Generate a filter <see cref="Expression" /> for operations on arrays
  /// </summary>
  /// <typeparam name="T">Type of the value and field on which the operation is applied</typeparam>
  /// <param name="filterOperator">The gRPC enum that selects the operation</param>
  /// <param name="field">The <see cref="Expression" /> to select the field on which to apply the operation</param>
  /// <param name="value">Value for the operation</param>
  /// <returns>
  ///   The <see cref="Expression" /> that represents the operation on the field with the given value
  /// </returns>
  public static Expression<Func<T, bool>> ToFilter<T>(this FilterArrayOperator     filterOperator,
                                                      Expression<Func<T, object?>> field,
                                                      string                       value)
    => filterOperator switch
       {
         FilterArrayOperator.Unspecified => throw new ArgumentOutOfRangeException(),
         FilterArrayOperator.Contains => ExpressionBuilders.MakeCall(field,
                                                                     value,
                                                                     typeof(Enumerable),
                                                                     nameof(Enumerable.Contains)),
         FilterArrayOperator.NotContains => ExpressionBuilders.MakeCall(field,
                                                                        value,
                                                                        typeof(Enumerable),
                                                                        nameof(Enumerable.Contains),
                                                                        true),
         _ => throw new ArgumentOutOfRangeException(),
       };
}
