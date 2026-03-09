// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace ArmoniK.Core.Adapters.PostgresSQL.Common;

/// <summary>
///   Translates Expression trees into parameterized SQL WHERE clauses
/// </summary>
public class ExpressionToSql<T>
{
  private readonly Dictionary<string, object?> parameters_ = new();
  private          int                         paramIndex_;

  /// <summary>
  ///   Translate an expression into a SQL WHERE clause
  /// </summary>
  /// <param name="expression">Filter expression</param>
  /// <returns>SQL WHERE clause and parameters</returns>
  public static (string sql, Dictionary<string, object?> parameters) Translate(Expression<Func<T, bool>> expression)
  {
    var translator = new ExpressionToSql<T>();
    var sql        = translator.Visit(expression.Body);
    return (sql, translator.parameters_);
  }

  /// <summary>
  ///   Translate an order-by expression into a SQL column name
  /// </summary>
  /// <param name="expression">Order expression</param>
  /// <returns>SQL column name</returns>
  public static string TranslateOrderBy(Expression<Func<T, object?>> expression)
  {
    var body = expression.Body;

    // Unwrap Convert
    while (body is UnaryExpression { NodeType: ExpressionType.Convert } unary)
    {
      body = unary.Operand;
    }

    // Handle dictionary indexer: x.Options.Options["key"] => options_options->>'key'
    if (body is MethodCallExpression { Method.Name: "get_Item" } methodCall && methodCall.Object is not null && IsParameterMember(methodCall.Object))
    {
      var member = GetMemberPath(methodCall.Object);
      var column = PropertyMapping.GetColumnName(typeof(T),
                                                  member);
      var key = EvaluateExpression(methodCall.Arguments[0]);
      return $"{column}->>'{key}'";
    }

    var path = GetMemberPath(body);
    return PropertyMapping.GetColumnName(typeof(T),
                                         path);
  }

  private string Visit(Expression expression)
    => expression switch
       {
         BinaryExpression binary     => VisitBinary(binary),
         UnaryExpression unary       => VisitUnary(unary),
         MethodCallExpression method => VisitMethodCall(method),
         MemberExpression member     => VisitMember(member),
         ConstantExpression constant => VisitConstant(constant),
         _                           => throw new NotSupportedException($"Expression type {expression.NodeType} is not supported"),
       };

  private string VisitBinary(BinaryExpression expression)
  {
    // Null check: x == null or x != null
    if (IsNullConstant(expression.Right))
    {
      var left = Visit(expression.Left);
      return expression.NodeType switch
             {
               ExpressionType.Equal    => $"({left} IS NULL)",
               ExpressionType.NotEqual => $"({left} IS NOT NULL)",
               _                       => throw new NotSupportedException($"Unsupported null comparison: {expression.NodeType}"),
             };
    }

    if (IsNullConstant(expression.Left))
    {
      var right = Visit(expression.Right);
      return expression.NodeType switch
             {
               ExpressionType.Equal    => $"({right} IS NULL)",
               ExpressionType.NotEqual => $"({right} IS NOT NULL)",
               _                       => throw new NotSupportedException($"Unsupported null comparison: {expression.NodeType}"),
             };
    }

    // Logical operators
    if (expression.NodeType is ExpressionType.AndAlso or ExpressionType.OrElse or ExpressionType.And or ExpressionType.Or)
    {
      var left  = Visit(expression.Left);
      var right = Visit(expression.Right);
      var op = expression.NodeType is ExpressionType.AndAlso or ExpressionType.And
                 ? "AND"
                 : "OR";
      return $"({left} {op} {right})";
    }

    // Comparison operators
    {
      var left  = Visit(expression.Left);
      var right = Visit(expression.Right);
      var op = expression.NodeType switch
               {
                 ExpressionType.Equal              => "=",
                 ExpressionType.NotEqual           => "!=",
                 ExpressionType.LessThan           => "<",
                 ExpressionType.GreaterThan        => ">",
                 ExpressionType.LessThanOrEqual    => "<=",
                 ExpressionType.GreaterThanOrEqual => ">=",
                 _                                 => throw new NotSupportedException($"Binary operator {expression.NodeType} is not supported"),
               };
      return $"({left} {op} {right})";
    }
  }

  private string VisitUnary(UnaryExpression expression)
  {
    switch (expression.NodeType)
    {
      case ExpressionType.Not:
        var operand = Visit(expression.Operand);
        return $"(NOT {operand})";
      case ExpressionType.Convert:
        return Visit(expression.Operand);
      default:
        throw new NotSupportedException($"Unary operator {expression.NodeType} is not supported");
    }
  }

  private string VisitMethodCall(MethodCallExpression expression)
  {
    var methodName = expression.Method.Name;

    switch (methodName)
    {
      case "Contains":
      {
        // Case 1: Enumerable.Contains(collection, value) — static extension method
        if (expression.Object is null && expression.Arguments.Count == 2)
        {
          // If the first argument is the SQL array column (parameter member),
          // generate: @value = ANY(column)
          if (IsMemberOfParameter(expression.Arguments[0]))
          {
            var member    = Visit(expression.Arguments[0]);
            var value     = EvaluateExpression(expression.Arguments[1]);
            var paramName = AddParameter(value);
            return $"({paramName} = ANY({member}))";
          }

          var collection = EvaluateExpression(expression.Arguments[0]);
          var member2    = Visit(expression.Arguments[1]);
          var pName      = AddParameter(collection);
          return $"({member2} = ANY({pName}))";
        }

        // Case 2: member.Contains(value) on string => LIKE
        if (expression.Object is not null && expression.Method.DeclaringType == typeof(string))
        {
          var member    = Visit(expression.Object);
          var value     = EvaluateExpression(expression.Arguments[0]);
          var paramName = AddParameter($"%{value}%");
          return $"({member} LIKE {paramName})";
        }

        // Case 3: list.Contains(value) => value = ANY(@p)
        if (expression.Object is not null)
        {
          // ICollection<T>.Contains or IList<T>.Contains
          if (IsMemberOfParameter(expression.Object))
          {
            // The member is a SQL array column, value is a parameter
            var member    = Visit(expression.Object);
            var value     = EvaluateExpression(expression.Arguments[0]);
            var paramName = AddParameter(value);
            return $"({paramName} = ANY({member}))";
          }

          var collection = EvaluateExpression(expression.Object);
          var arg        = Visit(expression.Arguments[0]);
          var pName      = AddParameter(collection);
          return $"({arg} = ANY({pName}))";
        }

        break;
      }

      case "Equals":
      {
        if (expression.Object is not null)
        {
          var left      = Visit(expression.Object);
          var right     = EvaluateExpression(expression.Arguments[0]);
          var paramName = AddParameter(right);
          return $"({left} = {paramName})";
        }

        break;
      }

      case "StartsWith":
      {
        if (expression.Object is not null && expression.Method.DeclaringType == typeof(string))
        {
          var member    = Visit(expression.Object);
          var value     = EvaluateExpression(expression.Arguments[0]);
          var paramName = AddParameter($"{value}%");
          return $"({member} LIKE {paramName})";
        }

        break;
      }

      case "EndsWith":
      {
        if (expression.Object is not null && expression.Method.DeclaringType == typeof(string))
        {
          var member    = Visit(expression.Object);
          var value     = EvaluateExpression(expression.Arguments[0]);
          var paramName = AddParameter($"%{value}");
          return $"({member} LIKE {paramName})";
        }

        break;
      }

      case "get_Item":
      {
        // Dictionary indexer: x.Options.Options["key"] => options_options->>'key'
        if (expression.Object is not null && IsMemberOfParameter(expression.Object))
        {
          var member    = Visit(expression.Object);
          var key       = EvaluateExpression(expression.Arguments[0]);
          var paramName = AddParameter(key);
          return $"({member}->>{paramName})";
        }

        break;
      }
    }

    throw new NotSupportedException($"Method {methodName} is not supported");
  }

  private string VisitMember(MemberExpression expression)
  {
    // Check if this is a member access on the lambda parameter (property access)
    if (IsParameterMember(expression))
    {
      var path = GetMemberPath(expression);
      return PropertyMapping.GetColumnName(typeof(T),
                                           path);
    }

    // Otherwise it's a captured variable or closure
    var value     = EvaluateExpression(expression);
    var paramName = AddParameter(value);
    return paramName;
  }

  private string VisitConstant(ConstantExpression expression)
  {
    var paramName = AddParameter(expression.Value);
    return paramName;
  }

  private string AddParameter(object? value)
  {
    var name = $"@p{paramIndex_++}";

    // Convert enums to their integer values for SQL
    if (value is not null && value.GetType()
                                  .IsEnum)
    {
      value = Convert.ToInt32(value);
    }

    // Convert collections of enums to int[] for PostgreSQL
    if (value is IEnumerable enumerable and not string and not byte[])
    {
      var elementType = value.GetType()
                             .GetInterfaces()
                             .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                             .Select(i => i.GetGenericArguments()[0])
                             .FirstOrDefault();
      if (elementType is not null && elementType.IsEnum)
      {
        value = enumerable.Cast<object>()
                          .Select(e => Convert.ToInt32(e))
                          .ToArray();
      }
    }

    // Convert IEnumerable<string> to string[] for PostgreSQL arrays
    if (value is IEnumerable<string> stringEnumerable)
    {
      value = stringEnumerable.ToArray();
    }

    // Convert IDictionary<string, bool> to its keys for PostgreSQL
    if (value is IDictionary<string, bool> dict)
    {
      value = dict.Keys.ToArray();
    }

    // Convert TimeSpan to ticks
    if (value is TimeSpan ts)
    {
      value = ts.Ticks;
    }

    parameters_[name] = value;
    return name;
  }

  private static bool IsNullConstant(Expression expression)
  {
    if (expression is ConstantExpression { Value: null })
    {
      return true;
    }

    if (expression is UnaryExpression { NodeType: ExpressionType.Convert } unary)
    {
      return IsNullConstant(unary.Operand);
    }

    return false;
  }

  private static bool IsParameterMember(Expression expression)
  {
    var current = expression;
    while (current is MemberExpression member)
    {
      current = member.Expression;
    }

    return current is ParameterExpression;
  }

  private static bool IsMemberOfParameter(Expression expression)
    => IsParameterMember(expression);

  private static string GetMemberPath(Expression expression)
  {
    var parts   = new List<string>();
    var current = expression;
    while (current is MemberExpression member)
    {
      parts.Add(member.Member.Name);
      current = member.Expression;
    }

    parts.Reverse();
    return string.Join(".",
                       parts);
  }

  private static object? EvaluateExpression(Expression expression)
  {
    // Unwrap Convert
    if (expression is UnaryExpression { NodeType: ExpressionType.Convert } unary)
    {
      expression = unary.Operand;
    }

    if (expression is ConstantExpression constant)
    {
      return constant.Value;
    }

    if (expression is MemberExpression member)
    {
      var obj = member.Expression is not null
                  ? EvaluateExpression(member.Expression)
                  : null;

      return member.Member switch
             {
               FieldInfo field    => field.GetValue(obj),
               PropertyInfo prop => prop.GetValue(obj),
               _                 => throw new NotSupportedException($"Member type {member.Member.GetType()} is not supported"),
             };
    }

    if (expression is MethodCallExpression methodCall)
    {
      var obj  = methodCall.Object is not null ? EvaluateExpression(methodCall.Object) : null;
      var args = methodCall.Arguments.Select(EvaluateExpression)
                           .ToArray();
      return methodCall.Method.Invoke(obj,
                                      args);
    }

    if (expression is NewArrayExpression newArray)
    {
      var values = newArray.Expressions.Select(EvaluateExpression)
                           .ToArray();
      var array = Array.CreateInstance(newArray.Type.GetElementType()!,
                                      values.Length);
      for (var i = 0; i < values.Length; i++)
      {
        array.SetValue(values[i],
                       i);
      }

      return array;
    }

    // Fallback: compile and evaluate
    var lambda   = Expression.Lambda(expression);
    var compiled = lambda.Compile();
    return compiled.DynamicInvoke();
  }
}
