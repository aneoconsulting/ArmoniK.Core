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
using System.Reflection;

namespace ArmoniK.Adapters.MongoDB
{
  public static class ExpressionsBuilders
  {
    public static Expression<Func<TaskDataModel, bool>> FieldFilterExpression<TField>(Expression<Func<TaskDataModel, TField>> expression,
                                                                                      IList<TField>                           values,
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


    public static Expression FieldFilterInternal<TField>(Expression<Func<TaskDataModel, TField>> expression,
                                                         IList<TField>                           values,
                                                         bool                                    include,
                                                         Expression                              x)
    {
      if (!values.Any())
        return Expression.Constant(true);


      //var t = model => // ReSharper disable once ConvertClosureToMethodGroup for better handling by MongoDriver visitor
      //                model.ParentsSubSessions.Any(parentSubSession => values.Contains(parentSubSession));

      var fieldName = ((MemberExpression)expression.Body).Member.Name;

      var property = Expression.Property(x,
                                         typeof(TaskDataModel),
                                         fieldName);

      var containsMethodInfo = typeof(Enumerable).GetMethods(BindingFlags.Static | BindingFlags.Public)
                                                 .Single(m => m.Name == nameof(Enumerable.Contains) &&
                                                              m.GetParameters().Length == 2)
                                                 .GetGenericMethodDefinition()
                                                 .MakeGenericMethod(typeof(TField));

      var valueExpr = Expression.Constant(values);

      var body = Expression.Call(null,
                                 containsMethodInfo,
                                 valueExpr,
                                 property);
      if (include)
        return body;
      return Expression.Not(body);
    }
  }
}
