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

using System.Linq.Expressions;

namespace ArmoniK.Core.Common.Utils;

/// <summary>
///   Represents an expression rewriter that replaces an expression with another one in an expression tree
/// </summary>
public class ReplaceExpressionVisitor : ExpressionVisitor
{
  private readonly Expression newValue_;
  private readonly Expression oldValue_;

  /// <summary>
  ///   Instantiates a <see cref="ReplaceExpressionVisitor" /> that replaces an expression with another one in an expression
  ///   tree
  /// </summary>
  /// <param name="oldValue">Expression to replace</param>
  /// <param name="newValue">Replacement expression</param>
  public ReplaceExpressionVisitor(Expression oldValue,
                                  Expression newValue)
  {
    oldValue_ = oldValue;
    newValue_ = newValue;
  }

  /// <inheritdoc />
  public override Expression? Visit(Expression? node)
    => node == oldValue_
         ? newValue_
         : base.Visit(node);
}
