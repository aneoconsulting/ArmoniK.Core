// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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

using ArmoniK.Core.Common.Utils;

using FluentValidation.Internal;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Class to create projection from one type to another with a mask to choose the fields to fill in
/// </summary>
public static class MaskedProjection
{
  /// <summary>
  ///   Builds an <see cref="Expression" /> that converts from <typeparamref name="TOrigin" /> to
  ///   <typeparamref name="TDest" /> with a mask to select the fields to fill in <typeparamref name="TDest" />
  /// </summary>
  /// <typeparam name="TField"><see cref="Enum" /> that represents the fields to select</typeparam>
  /// <typeparam name="TOrigin">Type holding the data</typeparam>
  /// <typeparam name="TDest">Type in which to put the selected data</typeparam>
  /// <param name="fields"><see cref="IEnumerable{TField}" /> representing the fields selected to form the mask</param>
  /// <param name="toOrigin">
  ///   Function that gives the expression to convert the <typeparamref name="TField" /> to a member
  ///   of <typeparamref name="TOrigin" />
  /// </param>
  /// <param name="toDest">
  ///   Function that gives the expression to convert the <typeparamref name="TField" /> to a member
  ///   of <typeparamref name="TDest" />
  /// </param>
  /// <param name="assignments">Additional assignments to perform when building <typeparamref name="TDest" /></param>
  /// <returns>
  ///   The <see cref="Expression" /> to convert a <typeparamref name="TOrigin" /> to <typeparamref name="TDest" /> with the
  ///   given mask
  /// </returns>
  public static Expression<Func<TOrigin, TDest>> CreateMaskedProjection<TField, TOrigin, TDest>(IEnumerable<TField>                              fields,
                                                                                                Func<TField, Expression<Func<TOrigin, object?>>> toOrigin,
                                                                                                Func<TField, Expression<Func<TDest, object?>>>   toDest,
                                                                                                IEnumerable<MemberAssignment>?                   assignments = null)
    where TField : Enum
  {
    var parameter = Expression.Parameter(typeof(TOrigin));

    var visitor = new ReplaceParameterVisitor(parameter);

    var ctor = typeof(TDest).GetConstructors()
                            .First(info => info.GetParameters()
                                               .Length == 0);

    var bindings = fields.Select(field => Bind(toOrigin(field),
                                               toDest(field)))
                         .ToList();

    if (assignments is not null)
    {
      bindings.AddRange(assignments);
    }

    var expr = Expression.MemberInit(Expression.New(ctor),
                                     bindings);

    return Expression.Lambda<Func<TOrigin, TDest>>(visitor.Visit(expr)!,
                                                   parameter);
  }

  private static MemberAssignment Bind<TOrigin, TDest>(Expression<Func<TOrigin, object?>> origExpression,
                                                       Expression<Func<TDest, object?>>   destExpression)
  {
    var destMember = destExpression.GetMember();
    var origBody   = origExpression.Body;

    if (origBody is UnaryExpression
                    {
                      NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked or ExpressionType.TypeAs,
                    } unaryExpression)
    {
      origBody = unaryExpression.Operand;
    }

    var destType = ((PropertyInfo)destMember).PropertyType;
    var origType = origBody.Type;

    var assignment = Expression.Bind(destMember,
                                     destType == origType
                                       ? origBody
                                       : Expression.Convert(origBody,
                                                            destType));

    return assignment;
  }
}
