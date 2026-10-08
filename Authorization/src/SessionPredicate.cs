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
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace ArmoniK.Core.Authorization;

/// <summary>
///   Where the session fields read by conditions live in a stored session type
/// </summary>
/// <typeparam name="TSession">The stored session type</typeparam>
/// <param name="Owner">Selector of the owner</param>
/// <param name="CreatedUnder">Selector of the principals the session was created under</param>
/// <param name="SharedWith">Selector of the principals the session is shared with</param>
/// <param name="DelegatedTo">Selector of the principals the session is delegated to</param>
public sealed record SessionFields<TSession>(Expression<Func<TSession, string?>>             Owner,
                                             Expression<Func<TSession, IEnumerable<string>>> CreatedUnder,
                                             Expression<Func<TSession, IEnumerable<string>>> SharedWith,
                                             Expression<Func<TSession, IEnumerable<string>>> DelegatedTo);

/// <summary>
///   Translates conditions into predicates over stored sessions, so that listings return exactly the sessions the
///   conditions select
/// </summary>
/// <remarks>
///   The predicates only use equality, <see cref="Enumerable.Contains{TSource}(IEnumerable{TSource},TSource)" />,
///   conjunctions, disjunctions and boolean constants, so that every storage adapter can translate them.
/// </remarks>
public static class SessionPredicate
{
  private static readonly MethodInfo EnumerableContains = typeof(Enumerable).GetMethods(BindingFlags.Public | BindingFlags.Static)
                                                                            .Single(method => method.Name == nameof(Enumerable.Contains) &&
                                                                                              method.GetParameters()
                                                                                                    .Length == 2)
                                                                            .MakeGenericMethod(typeof(string));

  /// <summary>
  ///   Build the predicate selecting the sessions a condition holds on
  /// </summary>
  /// <param name="condition">The condition</param>
  /// <param name="fields">Where the session fields live</param>
  /// <param name="caller">Principal of the caller</param>
  /// <param name="grantPrincipal">Principal of the grant being evaluated</param>
  /// <typeparam name="TSession">The stored session type</typeparam>
  /// <returns>The predicate</returns>
  /// <remarks>A term reading the partition of the request holds on no session.</remarks>
  public static Expression<Func<TSession, bool>> FromCondition<TSession>(Condition               condition,
                                                                         SessionFields<TSession> fields,
                                                                         string                  caller,
                                                                         string                  grantPrincipal)
  {
    var parameter = Expression.Parameter(typeof(TSession),
                                         "session");
    var body = condition.Terms.Select(term => TermBody(term,
                                                       fields,
                                                       parameter,
                                                       caller,
                                                       grantPrincipal))
                        .Aggregate(Expression.AndAlso);

    return Expression.Lambda<Func<TSession, bool>>(body,
                                                   parameter);
  }

  /// <summary>
  ///   Build the disjunction of predicates
  /// </summary>
  /// <param name="predicates">The predicates</param>
  /// <typeparam name="TSession">The stored session type</typeparam>
  /// <returns>A predicate holding when any of <paramref name="predicates" /> holds, false when there are none</returns>
  public static Expression<Func<TSession, bool>> Any<TSession>(IEnumerable<Expression<Func<TSession, bool>>> predicates)
  {
    var parameter = Expression.Parameter(typeof(TSession),
                                         "session");
    var bodies = predicates.Select(predicate => new ParameterReplacer(predicate.Parameters[0],
                                                                      parameter).Visit(predicate.Body))
                           .ToList();

    return Expression.Lambda<Func<TSession, bool>>(bodies.Count == 0
                                                     ? Expression.Constant(false)
                                                     : bodies.Aggregate(Expression.OrElse),
                                                   parameter);
  }

  private static Expression TermBody<TSession>(Term                    term,
                                               SessionFields<TSession> fields,
                                               ParameterExpression     parameter,
                                               string                  caller,
                                               string                  grantPrincipal)
    => term switch
       {
         OwnerIsCaller => Expression.Equal(Select(fields.Owner,
                                                  parameter),
                                           Expression.Constant(caller,
                                                               typeof(string))),
         LiteralInCreatedUnder literal => Contains(fields.CreatedUnder,
                                                   parameter,
                                                   literal.Principal),
         GrantPrincipalInCreatedUnder => Contains(fields.CreatedUnder,
                                                  parameter,
                                                  grantPrincipal),
         CallerInSharedWith => Contains(fields.SharedWith,
                                        parameter,
                                        caller),
         CallerInDelegatedTo => Contains(fields.DelegatedTo,
                                         parameter,
                                         caller),
         PartitionIs => Expression.Constant(false),
         _           => throw new ArgumentOutOfRangeException(nameof(term)),
       };

  private static MethodCallExpression Contains<TSession>(Expression<Func<TSession, IEnumerable<string>>> selector,
                                               ParameterExpression                             parameter,
                                               string                                          value)
    => Expression.Call(EnumerableContains,
                       Select(selector,
                              parameter),
                       Expression.Constant(value,
                                           typeof(string)));

  /// <summary>
  ///   The body of a selector applied to <paramref name="parameter" />, without the conversion the compiler adds when
  ///   the field type differs from the selector return type, so that adapters see a plain member access
  /// </summary>
  private static Expression Select(LambdaExpression    selector,
                                   ParameterExpression parameter)
  {
    var body = selector.Body;
    while (body is UnaryExpression
                   {
                     NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked,
                   } convert && !convert.Operand.Type.IsValueType)
    {
      body = convert.Operand;
    }

    return new ParameterReplacer(selector.Parameters[0],
                                 parameter).Visit(body);
  }

  private sealed class ParameterReplacer : ExpressionVisitor
  {
    private readonly ParameterExpression from_;
    private readonly ParameterExpression to_;

    public ParameterReplacer(ParameterExpression from,
                             ParameterExpression to)
    {
      from_ = from;
      to_   = to;
    }

    protected override Expression VisitParameter(ParameterExpression node)
      => node == from_
           ? to_
           : base.VisitParameter(node);
  }
}
