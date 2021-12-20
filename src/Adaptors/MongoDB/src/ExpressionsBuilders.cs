// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace ArmoniK.Adapters.MongoDB
{
  public static class ExpressionsBuilders
  {
    public static Expression<Func<TaskDataModel, bool>> FieldFilterExpression<TField>(Expression<Func<TaskDataModel, TField>> expression,
                                                                                      IEnumerable<TField>                     values,
                                                                                      bool                                    include = true)
    {
      var x = Expression.Parameter(typeof(TaskDataModel),
                                   "model");

      return (Expression<Func<TaskDataModel, bool>>)Expression.Lambda(FieldFilterInternal(expression,
                                                                                          values,
                                                                                          include,
                                                                                          x),
                                                                      x);
    }
    
    
    public static Expression FieldFilterInternal<TField>(Expression<Func<TaskDataModel,TField>> expression, 
                                                         IEnumerable<TField>                    values, 
                                                         bool                                   include, 
                                                         Expression                             x)
    {
      var fieldName = ((MemberExpression)expression.Body).Member.Name;

      return values.Aggregate(
                              (Expression)Expression.Constant(!include),
                              (expr, subSession) =>
                              {
                                var left = expr;

                                if (include)
                                {
                                  var right = Expression.Equal(Expression.Property(x,
                                                                                   typeof(TaskDataModel),
                                                                                   fieldName),
                                                               Expression.Constant(subSession,
                                                                                   typeof(TField)));
                                  return Expression.Or(left,
                                                       right);

                                }
                                else
                                {
                                  var right = Expression.NotEqual(Expression.Property(x,
                                                                                      typeof(TaskDataModel),
                                                                                      fieldName),
                                                                  Expression.Constant(subSession,
                                                                                      typeof(TField)));
                                  return Expression.And(left,
                                                        right);
                                }
                              }
                             );
    }
  }
}
