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

using ArmoniK.Api.gRPC.V1.Sessions;
using ArmoniK.Core.Common.Storage;

using LinqKit;

namespace ArmoniK.Core.Common.gRPC;

public static class ListSessionsRequestExt
{
  /// <summary>
  ///   Converts gRPC message into the associated <see cref="SessionData" /> field
  /// </summary>
  /// <param name="sort">The gPRC message</param>
  /// <returns>
  ///   The <see cref="Expression" /> that access the field from the object
  /// </returns>
  /// <exception cref="ArgumentOutOfRangeException">the given message is not recognized</exception>
  public static Expression<Func<SessionData, object?>> ToField(this ListSessionsRequest.Types.Sort sort)
  {
    switch (sort.Field.FieldCase)
    {
      case SessionField.FieldOneofCase.SessionRawField:
        return sort.Field.SessionRawField.Field.ToField();
      case SessionField.FieldOneofCase.TaskOptionField:
        return sort.Field.TaskOptionField.Field.ToField();
      case SessionField.FieldOneofCase.TaskOptionGenericField:
        return sort.Field.TaskOptionGenericField.ToField();
      case SessionField.FieldOneofCase.None:
      default:
        throw new ArgumentOutOfRangeException();
    }
  }

  /// <summary>
  ///   Converts gRPC message filter into an <see cref="Expression" /> that represents the filter condition
  /// </summary>
  /// <param name="filter">The gPRC filter</param>
  /// <returns>
  ///   The <see cref="Expression" /> that represents the filter condition
  /// </returns>
  /// <exception cref="ArgumentOutOfRangeException">the given message is not recognized</exception>
  public static Expression<Func<SessionData, bool>> ToExpression(this FilterString filter)
    => filter.Field.FieldCase switch
       {
         SessionField.FieldOneofCase.None => throw new ArgumentOutOfRangeException(),
         SessionField.FieldOneofCase.SessionRawField => filter.Operator.ToFilter(filter.Field.SessionRawField.Field.ToField(),
                                                                                 filter.Value),
         SessionField.FieldOneofCase.TaskOptionField => filter.Operator.ToFilter(filter.Field.TaskOptionField.Field.ToField(),
                                                                                 filter.Value),
         SessionField.FieldOneofCase.TaskOptionGenericField => filter.Operator.ToFilter(filter.Field.TaskOptionGenericField.ToField(),
                                                                                        filter.Value),
         _ => throw new ArgumentOutOfRangeException(),
       };

  /// <summary>
  ///   Converts gRPC message filter into an <see cref="Expression" /> that represents the filter condition
  /// </summary>
  /// <param name="filter">The gPRC filter</param>
  /// <returns>
  ///   The <see cref="Expression" /> that represents the filter condition
  /// </returns>
  /// <exception cref="ArgumentOutOfRangeException">the given message is not recognized</exception>
  public static Expression<Func<SessionData, bool>> ToExpression(this FilterNumber filter)
    => filter.Field.FieldCase switch
       {
         SessionField.FieldOneofCase.None => throw new ArgumentOutOfRangeException(),
         SessionField.FieldOneofCase.SessionRawField => ExpressionBuilders.MakeBinary(filter.Field.SessionRawField.Field.ToField(),
                                                                                      filter.Value,
                                                                                      filter.Operator.ToExpressionType()),
         SessionField.FieldOneofCase.TaskOptionField => ExpressionBuilders.MakeBinary(filter.Field.TaskOptionField.Field.ToField(),
                                                                                      filter.Value,
                                                                                      filter.Operator.ToExpressionType()),
         SessionField.FieldOneofCase.TaskOptionGenericField => ExpressionBuilders.MakeBinary(filter.Field.TaskOptionGenericField.ToField(),
                                                                                             filter.Value,
                                                                                             filter.Operator.ToExpressionType()),
         _ => throw new ArgumentOutOfRangeException(),
       };

  /// <summary>
  ///   Converts gRPC message filter into an <see cref="Expression" /> that represents the filter condition
  /// </summary>
  /// <param name="filter">The gPRC filter</param>
  /// <returns>
  ///   The <see cref="Expression" /> that represents the filter condition
  /// </returns>
  /// <exception cref="ArgumentOutOfRangeException">the given message is not recognized</exception>
  public static Expression<Func<SessionData, bool>> ToExpression(this FilterDate filter)
    => filter.Field.FieldCase switch
       {
         SessionField.FieldOneofCase.None => throw new ArgumentOutOfRangeException(),
         SessionField.FieldOneofCase.SessionRawField => ExpressionBuilders.MakeBinary(filter.Field.SessionRawField.Field.ToField(),
                                                                                      filter.Value.ToDateTime(),
                                                                                      filter.Operator.ToExpressionType()),
         SessionField.FieldOneofCase.TaskOptionField => ExpressionBuilders.MakeBinary(filter.Field.TaskOptionField.Field.ToField(),
                                                                                      filter.Value.ToDateTime(),
                                                                                      filter.Operator.ToExpressionType()),
         SessionField.FieldOneofCase.TaskOptionGenericField => ExpressionBuilders.MakeBinary(filter.Field.TaskOptionGenericField.ToField(),
                                                                                             filter.Value.ToDateTime(),
                                                                                             filter.Operator.ToExpressionType()),
         _ => throw new ArgumentOutOfRangeException(),
       };

  /// <summary>
  ///   Converts gRPC message filter into an <see cref="Expression" /> that represents the filter condition
  /// </summary>
  /// <param name="filter">The gPRC filter</param>
  /// <returns>
  ///   The <see cref="Expression" /> that represents the filter condition
  /// </returns>
  /// <exception cref="ArgumentOutOfRangeException">the given message is not recognized</exception>
  public static Expression<Func<SessionData, bool>> ToExpression(this FilterBoolean filter)
    => filter.Field.FieldCase switch
       {
         SessionField.FieldOneofCase.None => throw new ArgumentOutOfRangeException(),
         SessionField.FieldOneofCase.SessionRawField => ExpressionBuilders.MakeBinary(filter.Field.SessionRawField.Field.ToField(),
                                                                                      filter.Value,
                                                                                      ExpressionType.Equal),
         SessionField.FieldOneofCase.TaskOptionField => ExpressionBuilders.MakeBinary(filter.Field.TaskOptionField.Field.ToField(),
                                                                                      filter.Value,
                                                                                      ExpressionType.Equal),
         SessionField.FieldOneofCase.TaskOptionGenericField => ExpressionBuilders.MakeBinary(filter.Field.TaskOptionGenericField.ToField(),
                                                                                             filter.Value,
                                                                                             ExpressionType.Equal),
         _ => throw new ArgumentOutOfRangeException(),
       };

  /// <summary>
  ///   Converts gRPC message filter into an <see cref="Expression" /> that represents the filter condition
  /// </summary>
  /// <param name="filter">The gPRC filter</param>
  /// <returns>
  ///   The <see cref="Expression" /> that represents the filter condition
  /// </returns>
  /// <exception cref="ArgumentOutOfRangeException">the given message is not recognized</exception>
  public static Expression<Func<SessionData, bool>> ToExpression(this FilterArray filter)
    => filter.Field.FieldCase switch
       {
         SessionField.FieldOneofCase.None => throw new ArgumentOutOfRangeException(),
         SessionField.FieldOneofCase.SessionRawField => filter.Operator.ToFilter(filter.Field.SessionRawField.Field.ToField(),
                                                                                 filter.Value),
         SessionField.FieldOneofCase.TaskOptionField => filter.Operator.ToFilter(filter.Field.TaskOptionField.Field.ToField(),
                                                                                 filter.Value),
         SessionField.FieldOneofCase.TaskOptionGenericField => filter.Operator.ToFilter(filter.Field.TaskOptionGenericField.ToField(),
                                                                                        filter.Value),
         _ => throw new ArgumentOutOfRangeException(),
       };

  /// <summary>
  ///   Converts gRPC message filter into an <see cref="Expression" /> that represents the filter condition
  /// </summary>
  /// <param name="filter">The gPRC filter</param>
  /// <returns>
  ///   The <see cref="Expression" /> that represents the filter condition
  /// </returns>
  /// <exception cref="ArgumentOutOfRangeException">the given message is not recognized</exception>
  public static Expression<Func<SessionData, bool>> ToExpression(this FilterStatus filter)
    => filter.Field.FieldCase switch
       {
         SessionField.FieldOneofCase.None => throw new ArgumentOutOfRangeException(),
         SessionField.FieldOneofCase.SessionRawField => ExpressionBuilders.MakeBinary(filter.Field.SessionRawField.Field.ToField(),
                                                                                      filter.Value,
                                                                                      filter.Operator.ToExpressionType()),
         SessionField.FieldOneofCase.TaskOptionField => ExpressionBuilders.MakeBinary(filter.Field.TaskOptionField.Field.ToField(),
                                                                                      filter.Value,
                                                                                      filter.Operator.ToExpressionType()),
         SessionField.FieldOneofCase.TaskOptionGenericField => ExpressionBuilders.MakeBinary(filter.Field.TaskOptionGenericField.ToField(),
                                                                                             filter.Value,
                                                                                             filter.Operator.ToExpressionType()),
         _ => throw new ArgumentOutOfRangeException(),
       };

  /// <summary>
  ///   Converts gRPC message filters into an <see cref="Expression" /> that represents the filter conditions
  /// </summary>
  /// <param name="filters">The gPRC filters</param>
  /// <returns>
  ///   The <see cref="Expression" /> that represents the filter conditions
  /// </returns>
  /// <exception cref="ArgumentOutOfRangeException">the given message is not recognized</exception>
  public static Expression<Func<SessionData, bool>> ToSessionDataFilter(this Filters filters)
  {
    var predicate = PredicateBuilder.New<SessionData>();

    if (filters.Filters_?.Filters == null || !filters.Filters_.Filters.Any())
    {
      return data => true;
    }

    foreach (var filtersAnd in filters.Filters_.Filters)
    {
      var predicateAnd = PredicateBuilder.New<SessionData>(data => true);
      foreach (var filterField in filtersAnd.Filters)
      {
        switch (filterField.FilterCase)
        {
          case FilterField.FilterOneofCase.String:
            predicateAnd = predicateAnd.And(filterField.String.ToExpression());
            break;
          case FilterField.FilterOneofCase.Number:
            predicateAnd = predicateAnd.And(filterField.Number.ToExpression());
            break;
          case FilterField.FilterOneofCase.Date:
            predicateAnd = predicateAnd.And(filterField.Date.ToExpression());
            break;
          case FilterField.FilterOneofCase.Boolean:
            predicateAnd = predicateAnd.And(filterField.Boolean.ToExpression());
            break;
          case FilterField.FilterOneofCase.Array:
            predicateAnd = predicateAnd.And(filterField.Array.ToExpression());
            break;
          case FilterField.FilterOneofCase.Status:
            predicateAnd = predicateAnd.And(filterField.Status.ToExpression());
            break;
          case FilterField.FilterOneofCase.None:
          default:
            throw new ArgumentOutOfRangeException();
        }
      }

      predicate = predicate.Or(predicateAnd);
    }

    return predicate;
  }
}
