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

  public static Expression<Func<SessionData, object?>> ToField(this SessionField taskField)
    => taskField.FieldCase switch
       {
         SessionField.FieldOneofCase.None                   => throw new ArgumentOutOfRangeException(),
         SessionField.FieldOneofCase.SessionRawField        => taskField.SessionRawField.Field.ToField(),
         SessionField.FieldOneofCase.TaskOptionField        => taskField.TaskOptionField.Field.ToField(),
         SessionField.FieldOneofCase.TaskOptionGenericField => taskField.TaskOptionGenericField.ToField(),
         _                                                  => throw new ArgumentOutOfRangeException(),
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

    if (filters.Or == null || !filters.Or.Any())
    {
      return data => true;
    }

    foreach (var filtersAnd in filters.Or)
    {
      var predicateAnd = PredicateBuilder.New<SessionData>(data => true);
      foreach (var filterField in filtersAnd.And)
      {
        switch (filterField.ValueConditionCase)
        {
          case FilterField.ValueConditionOneofCase.FilterString:
            predicateAnd = predicateAnd.And(filterField.FilterString.Operator.ToFilter(filterField.Field.ToField(),
                                                                                       filterField.FilterString.Value));
            break;
          case FilterField.ValueConditionOneofCase.FilterNumber:
            predicateAnd = predicateAnd.And(filterField.FilterNumber.Operator.ToFilter(filterField.Field.ToField(),
                                                                                       filterField.FilterNumber.Value));
            break;
          case FilterField.ValueConditionOneofCase.FilterBoolean:
            predicateAnd = predicateAnd.And(ExpressionBuilders.MakeBinary(filterField.Field.ToField(),
                                                                          filterField.FilterBoolean.Value,
                                                                          ExpressionType.Equal));
            break;
          case FilterField.ValueConditionOneofCase.FilterStatus:
            predicateAnd = predicateAnd.And(filterField.FilterStatus.Operator.ToFilter(filterField.Field.ToField(),
                                                                                       filterField.FilterStatus.Value));
            break;
          case FilterField.ValueConditionOneofCase.FilterDate:
            predicateAnd = predicateAnd.And(filterField.FilterDate.Operator.ToFilter(filterField.Field.ToField(),
                                                                                     filterField.FilterDate.Value.ToDateTime()));
            break;
          case FilterField.ValueConditionOneofCase.FilterArray:
            predicateAnd = predicateAnd.And(filterField.FilterArray.Operator.ToFilter(filterField.Field.ToField(),
                                                                                      filterField.FilterArray.Value));
            break;
          case FilterField.ValueConditionOneofCase.None:
          default:
            throw new ArgumentOutOfRangeException();
        }
      }

      predicate = predicate.Or(predicateAnd);
    }

    return predicate;
  }
}
