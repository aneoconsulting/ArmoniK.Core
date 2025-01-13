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
using System.Linq.Expressions;

namespace ArmoniK.Core.Common.Utils;

/// <summary>
///   Visitor to replace parameters to the given parameter
/// </summary>
public class ReplaceParameterVisitor : ExpressionVisitor
{
  private readonly ParameterExpression parameterExpression_;

  /// <summary>
  ///   Instantiate a visitor that changes all parameters to the given
  /// </summary>
  /// <param name="parameterExpression">The parameter to put in place</param>
  public ReplaceParameterVisitor(ParameterExpression parameterExpression)
    => parameterExpression_ = parameterExpression;

  /// <inheritdoc />
  public override Expression? Visit(Expression? node)
    => node switch
       {
         ParameterExpression => parameterExpression_,
         LambdaExpression    => throw new NotSupportedException("Nested lambdas are not supported"),
         _                   => base.Visit(node),
       };
}
