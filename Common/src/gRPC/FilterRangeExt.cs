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

using ArmoniK.Api.gRPC.V1;

namespace ArmoniK.Core.Common.gRPC;

/// <summary>
///   Class to extend gRPC messages to convert them in <see cref="ExpressionType" /> in order to convert them in requests
///   to the database
/// </summary>
public static class FilterRangeExt
{
  /// <summary>
  ///   Generate a filter <see cref="Expression" /> for operations on strings
  /// </summary>
  /// <typeparam name="T">Type of the value and field on which the operation is applied</typeparam>
  /// <typeparam name="TStatus">Status enum type used for the comparison</typeparam>
  /// <param name="filterOperator">The gRPC enum that selects the operation</param>
  /// <param name="field">The <see cref="Expression" /> to select the field on which to apply the operation</param>
  /// <param name="value">Value for the operation</param>
  /// <returns>
  ///   The <see cref="Expression" /> that represents the operation on the field with the given value
  /// </returns>
  public static Expression<Func<T, bool>> ToFilter<T, TStatus>(this FilterStatusOperator    filterOperator,
                                                               Expression<Func<T, object?>> field,
                                                               TStatus                      value)
    => filterOperator switch
       {
         FilterStatusOperator.Equal => ExpressionBuilders.MakeBinary(field,
                                                                     value,
                                                                     ExpressionType.Equal),
         FilterStatusOperator.NotEqual => ExpressionBuilders.MakeBinary(field,
                                                                        value,
                                                                        ExpressionType.NotEqual),
         _ => throw new ArgumentOutOfRangeException(nameof(filterOperator)),
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
  public static Expression<Func<T, bool>> ToFilter<T>(this FilterDateOperator      filterOperator,
                                                      Expression<Func<T, object?>> field,
                                                      DateTime?                    value)
    => filterOperator switch
       {
         FilterDateOperator.Before => ExpressionBuilders.MakeBinary(field,
                                                                    value,
                                                                    ExpressionType.LessThan),
         FilterDateOperator.BeforeOrEqual => ExpressionBuilders.MakeBinary(field,
                                                                           value,
                                                                           ExpressionType.LessThanOrEqual),
         FilterDateOperator.Equal => ExpressionBuilders.MakeBinary(field,
                                                                   value,
                                                                   ExpressionType.Equal),
         FilterDateOperator.AfterOrEqual => ExpressionBuilders.MakeBinary(field,
                                                                          value,
                                                                          ExpressionType.GreaterThanOrEqual),
         FilterDateOperator.After => ExpressionBuilders.MakeBinary(field,
                                                                   value,
                                                                   ExpressionType.GreaterThan),
         FilterDateOperator.NotEqual => ExpressionBuilders.MakeBinary(field,
                                                                      value,
                                                                      ExpressionType.NotEqual),
         _ => throw new ArgumentOutOfRangeException(nameof(filterOperator)),
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
  public static Expression<Func<T, bool>> ToFilter<T>(this FilterNumberOperator    filterOperator,
                                                      Expression<Func<T, object?>> field,
                                                      long                         value)
    => filterOperator switch
       {
         FilterNumberOperator.LessThan => ExpressionBuilders.MakeBinary(field,
                                                                        value,
                                                                        ExpressionType.LessThan),
         FilterNumberOperator.LessThanOrEqual => ExpressionBuilders.MakeBinary(field,
                                                                               value,
                                                                               ExpressionType.LessThanOrEqual),
         FilterNumberOperator.Equal => ExpressionBuilders.MakeBinary(field,
                                                                     value,
                                                                     ExpressionType.Equal),
         FilterNumberOperator.NotEqual => ExpressionBuilders.MakeBinary(field,
                                                                        value,
                                                                        ExpressionType.NotEqual),
         FilterNumberOperator.GreaterThanOrEqual => ExpressionBuilders.MakeBinary(field,
                                                                                  value,
                                                                                  ExpressionType.GreaterThanOrEqual),
         FilterNumberOperator.GreaterThan => ExpressionBuilders.MakeBinary(field,
                                                                           value,
                                                                           ExpressionType.GreaterThan),
         _ => throw new ArgumentOutOfRangeException(nameof(filterOperator)),
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
         _ => throw new ArgumentOutOfRangeException(nameof(filterOperator)),
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
         FilterArrayOperator.Contains => ExpressionBuilders.MakeCall(field,
                                                                     value,
                                                                     typeof(Enumerable),
                                                                     nameof(Enumerable.Contains)),
         FilterArrayOperator.NotContains => ExpressionBuilders.MakeCall(field,
                                                                        value,
                                                                        typeof(Enumerable),
                                                                        nameof(Enumerable.Contains),
                                                                        true),
         _ => throw new ArgumentOutOfRangeException(nameof(filterOperator)),
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
  public static Expression<Func<T, bool>> ToFilter<T>(this FilterBooleanOperator   filterOperator,
                                                      Expression<Func<T, object?>> field,
                                                      bool                         value)
    => filterOperator switch
       {
         FilterBooleanOperator.Is => ExpressionBuilders.MakeBinary(field,
                                                                   value,
                                                                   ExpressionType.Equal),
         _ => throw new ArgumentOutOfRangeException(nameof(filterOperator)),
       };

  /// <summary>
  ///   Generate a filter <see cref="Expression" /> for operations on durations
  /// </summary>
  /// <typeparam name="T">Type of the value and field on which the operation is applied</typeparam>
  /// <param name="filterOperator">The gRPC enum that selects the operation</param>
  /// <param name="field">The <see cref="Expression" /> to select the field on which to apply the operation</param>
  /// <param name="value">Value for the operation</param>
  /// <returns>
  ///   The <see cref="Expression" /> that represents the operation on the field with the given value
  /// </returns>
  public static Expression<Func<T, bool>> ToFilter<T>(this FilterDurationOperator  filterOperator,
                                                      Expression<Func<T, object?>> field,
                                                      TimeSpan?                     value)

    => filterOperator switch
    {
      FilterDurationOperator.LessThan => ExpressionBuilders.MakeBinary(field,
                                                                       value,
                                                                       ExpressionType.LessThan),
      FilterDurationOperator.LessThanOrEqual => ExpressionBuilders.MakeBinary(field,
                                                                              value,
                                                                              ExpressionType.LessThanOrEqual),
      FilterDurationOperator.Equal => ExpressionBuilders.MakeBinary(field,
                                                                    value,
                                                                    ExpressionType.Equal),
      FilterDurationOperator.NotEqual => ExpressionBuilders.MakeBinary(field,
                                                                       value,
                                                                       ExpressionType.NotEqual),
      FilterDurationOperator.GreaterThanOrEqual => ExpressionBuilders.MakeBinary(field,
                                                                                 value,
                                                                                 ExpressionType.GreaterThanOrEqual),
      FilterDurationOperator.GreaterThan => ExpressionBuilders.MakeBinary(field,
                                                                          value,
                                                                          ExpressionType.GreaterThan),
      _ => throw new ArgumentOutOfRangeException(nameof(filterOperator)),
    };
}
