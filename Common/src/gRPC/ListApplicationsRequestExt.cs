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
using System.Linq.Expressions;

using ArmoniK.Api.gRPC.V1.Applications;
using ArmoniK.Core.Common.Storage;

using LinqKit;

namespace ArmoniK.Core.Common.gRPC;

public static class ListApplicationsRequestExt
{
  /// <summary>
  ///   Converts gRPC message into the associated <see cref="Application" /> field
  /// </summary>
  /// <param name="field">The gPRC message</param>
  /// <returns>
  ///   The <see cref="Expression" /> that access the field from the object
  /// </returns>
  /// <exception cref="ArgumentOutOfRangeException">the given message is not recognized</exception>
  public static Expression<Func<Application, object?>> ToField(this ApplicationField field)
  {
    switch (field.FieldCase)
    {
      case ApplicationField.FieldOneofCase.ApplicationField_:
        return field.ApplicationField_.Field switch
               {
                 ApplicationRawEnumField.Name        => application => application.Name,
                 ApplicationRawEnumField.Version     => application => application.Version,
                 ApplicationRawEnumField.Namespace   => application => application.Namespace,
                 ApplicationRawEnumField.Service     => application => application.Service,
                 ApplicationRawEnumField.Unspecified => throw new ArgumentOutOfRangeException(),
                 _                                   => throw new ArgumentOutOfRangeException(),
               };

      case ApplicationField.FieldOneofCase.None:
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
  public static Expression<Func<TaskData, bool>> ToExpression(this FilterString filter)
    => filter.Field.FieldCase switch
       {
         ApplicationField.FieldOneofCase.None => throw new ArgumentOutOfRangeException(),
         ApplicationField.FieldOneofCase.ApplicationField_ => filter.Operator.ToFilter(filter.Field.ApplicationField_.Field.ToField(),
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
  public static Expression<Func<TaskData, bool>> ToExpression(this FilterNumber filter)
    => filter.Field.FieldCase switch
       {
         ApplicationField.FieldOneofCase.None => throw new ArgumentOutOfRangeException(),
         ApplicationField.FieldOneofCase.ApplicationField_ => ExpressionBuilders.MakeBinary(filter.Field.ApplicationField_.Field.ToField(),
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
  public static Expression<Func<TaskData, bool>> ToExpression(this FilterDate filter)
    => filter.Field.FieldCase switch
       {
         ApplicationField.FieldOneofCase.None => throw new ArgumentOutOfRangeException(),
         ApplicationField.FieldOneofCase.ApplicationField_ => ExpressionBuilders.MakeBinary(filter.Field.ApplicationField_.Field.ToField(),
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
  public static Expression<Func<TaskData, bool>> ToExpression(this FilterBoolean filter)
    => filter.Field.FieldCase switch
       {
         ApplicationField.FieldOneofCase.None => throw new ArgumentOutOfRangeException(),
         ApplicationField.FieldOneofCase.ApplicationField_ => ExpressionBuilders.MakeBinary(filter.Field.ApplicationField_.Field.ToField(),
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
  public static Expression<Func<TaskData, bool>> ToExpression(this FilterArray filter)
    => filter.Field.FieldCase switch
       {
         ApplicationField.FieldOneofCase.None => throw new ArgumentOutOfRangeException(),
         ApplicationField.FieldOneofCase.ApplicationField_ => filter.Operator.ToFilter(filter.Field.ApplicationField_.Field.ToField(),
                                                                                       filter.Value),
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
  public static Expression<Func<TaskData, bool>> ToApplicationFilter(this Filters filters)
  {
    var predicate = PredicateBuilder.New<TaskData>(data => false);

    if (filters.Filters_?.Filters == null)
    {
      return predicate;
    }

    foreach (var filtersAnd in filters.Filters_.Filters)
    {
      var predicateAnd = PredicateBuilder.New<TaskData>(data => true);
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
