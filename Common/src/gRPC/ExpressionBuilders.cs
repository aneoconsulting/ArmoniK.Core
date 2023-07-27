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
using System.Reflection;

namespace ArmoniK.Core.Common.gRPC;

/// <summary>
///   Generate <see cref="Expression" /> from field accessor, the operation to apply on the field and the value to compare
///   the field
/// </summary>
public class ExpressionBuilders
{
  private static Expression GetMemberExpression<TIn>(Expression<Func<TIn, object?>> selector)
  {
    switch (selector.Body.NodeType)
    {
      case ExpressionType.Convert:
        var unary  = (UnaryExpression)selector.Body;
        var member = (MemberExpression)unary.Operand;
        return member;

      case ExpressionType.MemberAccess:
        return (MemberExpression)selector.Body;

      case ExpressionType.Call:
        var expr = (MethodCallExpression)selector.Body;
        return expr;

      default:
        throw new NotImplementedException();
    }
  }

  /// <summary>
  ///   Generate <see cref="Expression" /> for binary operations
  /// </summary>
  /// <typeparam name="TIn">Class from which to access the field</typeparam>
  /// <typeparam name="T">Type of the value and field on which the operation is applied</typeparam>
  /// <param name="selector"><see cref="Expression" /> to access the field</param>
  /// <param name="value">Value for the operation</param>
  /// <param name="binaryExpression">Operation to perform</param>
  /// <returns>
  ///   The <see cref="Expression" /> that represents the operation on the field with the given value
  /// </returns>
  public static Expression<Func<TIn, bool>> MakeBinary<TIn, T>(Expression<Func<TIn, object?>> selector,
                                                               T                              value,
                                                               ExpressionType                 binaryExpression)
  {
    var constant = Expression.Constant(value,
                                       typeof(T));

    var member = GetMemberExpression(selector);

    var binary = Expression.MakeBinary(binaryExpression,
                                       member,
                                       member.Type == typeof(T)
                                         ? constant
                                         : Expression.Convert(constant,
                                                              member.Type));

    return Expression.Lambda<Func<TIn, bool>>(binary,
                                              selector.Parameters);
  }

  /// <summary>
  ///   Generate <see cref="Expression" /> for binary operations
  /// </summary>
  /// <typeparam name="TIn">Class from which to access the field</typeparam>
  /// <typeparam name="T">Type of the value and field on which the operation is applied</typeparam>
  /// <param name="selector"><see cref="Expression" /> to access the field</param>
  /// <param name="value">Value for the operation</param>
  /// <param name="type">Type of the collection containing the method we want to apply</param>
  /// <param name="method">The name of the method from the type</param>
  /// <param name="invert">Whether we should invert the result (not operation)</param>
  /// <returns>
  ///   The <see cref="Expression" /> that represents the operation on the field with the given value
  /// </returns>
  public static Expression<Func<TIn, bool>> MakeCall<TIn, T>(Expression<Func<TIn, object?>> selector,
                                                             T                              value,
                                                             Type                           type,
                                                             string                         method,
                                                             bool                           invert = false)
  {
    var member = GetMemberExpression(selector);

    var constant = Expression.Constant(value,
                                       typeof(T));

    var methodInfo = type.GetMethods(BindingFlags.Static | BindingFlags.Public)
                         .Single(m => m.Name == method && m.GetParameters()
                                                           .Length == 2)
                         .GetGenericMethodDefinition()
                         .MakeGenericMethod(typeof(T));

    var call = Expression.Call(null,
                               methodInfo,
                               member,
                               constant);

    if (invert)
    {
      return Expression.Lambda<Func<TIn, bool>>(Expression.Not(call),
                                                selector.Parameters);
    }

    return Expression.Lambda<Func<TIn, bool>>(call,
                                              selector.Parameters);
  }

  /// <summary>
  ///   Generate <see cref="Expression" /> for operations on strings
  /// </summary>
  /// <typeparam name="TIn">Class from which to access the field</typeparam>
  /// <param name="selector"><see cref="Expression" /> to access the field</param>
  /// <param name="value">Value for the operation</param>
  /// <param name="method">The name of the method from the string type</param>
  /// <param name="invert">Whether we should invert the result (not operation)</param>
  /// <returns>
  ///   The <see cref="Expression" /> that represents the operation on the field with the given value
  /// </returns>
  public static Expression<Func<TIn, bool>> MakeCallString<TIn>(Expression<Func<TIn, object?>> selector,
                                                                string                         value,
                                                                string                         method,
                                                                bool                           invert = false)
  {
    var member = GetMemberExpression(selector);

    var constant = Expression.Constant(value,
                                       typeof(string));

    var methodInfo = typeof(string).GetMethods()
                                   .First(m => m.Name == method                                   && m.GetParameters()
                                                                                     .Length == 1 && m.GetParameters()
                                                                                                      .First()
                                                                                                      .ParameterType == typeof(string));

    var call = Expression.Call(member,
                               methodInfo,
                               constant);

    if (invert)
    {
      return Expression.Lambda<Func<TIn, bool>>(Expression.Not(call),
                                                selector.Parameters);
    }

    return Expression.Lambda<Func<TIn, bool>>(call,
                                              selector.Parameters);
  }
}
