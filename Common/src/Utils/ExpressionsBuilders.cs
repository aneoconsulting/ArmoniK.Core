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
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace ArmoniK.Core.Common.Utils;

/// <summary>
///   Provides utility methods for building LINQ expressions to filter data models based on field values.
/// </summary>
public static class ExpressionsBuilders
{
  /// <summary>
  ///   Builds a filter expression for a field based on a list of values.
  /// </summary>
  /// <typeparam name="TData">The type of the data model.</typeparam>
  /// <typeparam name="TField">The type of the field to filter.</typeparam>
  /// <param name="expression">An expression representing the field to filter.</param>
  /// <param name="values">The list of values to filter by.</param>
  /// <param name="include">
  ///   If true, filter represented by <paramref name="expression" /> is a whitelist; otherwise, it is a
  ///   blacklist.
  /// </param>
  /// <returns>An expression that can be used to filter the data.</returns>
  public static Expression<Func<TData, bool>> FieldFilterExpression<TData, TField>(Expression<Func<TData, TField>> expression,
                                                                                   IList<TField>                   values,
                                                                                   bool                            include = true)
  {
    var x = Expression.Parameter(typeof(TData),
                                 "model");

    return (Expression<Func<TData, bool>>)Expression.Lambda(FieldFilterInternal(expression,
                                                                                values,
                                                                                include,
                                                                                x),
                                                            x);
  }

  /// <summary>
  ///   Builds an internal filter expression for a field based on a list of values.
  /// </summary>
  /// <typeparam name="TData">The type of the data model.</typeparam>
  /// <typeparam name="TField">The type of the field to filter.</typeparam>
  /// <param name="expression">An expression representing the field to filter.</param>
  /// <param name="values">The list of values to filter by.</param>
  /// <param name="include">
  ///   If true, filter represented by <paramref name="expression" /> is a whitelist; otherwise, it is a
  ///   blacklist.
  /// </param>
  /// <param name="x">The parameter expression representing the data model.</param>
  /// <returns>An expression that can be used to filter the data.</returns>
  public static Expression FieldFilterInternal<TData, TField>(Expression<Func<TData, TField>> expression,
                                                              IList<TField>                   values,
                                                              bool                            include,
                                                              Expression                      x)
  {
    if (!values.Any())
    {
      return Expression.Constant(true);
    }

    var fieldName = ((MemberExpression)expression.Body).Member.Name;

    var property = Expression.Property(x,
                                       typeof(TData),
                                       fieldName);

    if (values.Count == 1)
    {
      return include
               ? Expression.Equal(property,
                                  Expression.Constant(values[0]))
               : Expression.NotEqual(property,
                                     Expression.Constant(values[0]));
    }

    var containsMethodInfo = typeof(Enumerable).GetMethods(BindingFlags.Static | BindingFlags.Public)
                                               .Single(m => m.Name == nameof(Enumerable.Contains) && m.GetParameters()
                                                                                                      .Length == 2)
                                               .GetGenericMethodDefinition()
                                               .MakeGenericMethod(typeof(TField));

    var valueExpr = Expression.Constant(values);

    var body = Expression.Call(null,
                               containsMethodInfo,
                               valueExpr,
                               property);

    return include
             ? body
             : Expression.Not(body);
  }

  /// <summary>
  ///   Builds an internal filter expression for a collection field based on a list of values.
  /// </summary>
  /// <typeparam name="TData">The type of the data model.</typeparam>
  /// <typeparam name="TField">The type of the elements in the collection field to filter.</typeparam>
  /// <param name="expression">An expression representing the collection field to filter.</param>
  /// <param name="values">The list of values to filter by.</param>
  /// <param name="include">
  ///   If true, filter represented by <paramref name="expression" /> is a whitelist; otherwise, it is a
  ///   blacklist.
  /// </param>
  /// <param name="x">The parameter expression representing the data model.</param>
  /// <returns>An expression that can be used to filter the data.</returns>
  public static Expression FieldFilterInternal<TData, TField>(Expression<Func<TData, IEnumerable<TField>>> expression,
                                                              IList<TField>                                values,
                                                              bool                                         include,
                                                              Expression                                   x)
  {
    if (!values.Any())
    {
      return Expression.Constant(true);
    }

    var fieldName = ((MemberExpression)expression.Body).Member.Name;

    var property = Expression.Property(x,
                                       typeof(TData),
                                       fieldName);

    var containsMethodInfo = typeof(Enumerable).GetMethods(BindingFlags.Static | BindingFlags.Public)
                                               .Single(m => m.Name == nameof(Enumerable.Contains) && m.GetParameters()
                                                                                                      .Length == 2)
                                               .GetGenericMethodDefinition()
                                               .MakeGenericMethod(typeof(TField));


    Expression body = Expression.Call(null,
                                      containsMethodInfo,
                                      property,
                                      Expression.Constant(values[0]));

    body = values.Skip(1)
                 .Aggregate(body,
                            (accumulator,
                             value) => Expression.AndAlso(accumulator,
                                                          Expression.Call(null,
                                                                          containsMethodInfo,
                                                                          property,
                                                                          Expression.Constant(value))));

    return include
             ? body
             : Expression.Not(body);
  }
}
