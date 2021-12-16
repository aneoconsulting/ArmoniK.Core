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

using ArmoniK.Core.gRPC.V1;

using MongoDB.Driver;
using MongoDB.Driver.Linq;

namespace ArmoniK.Adapters.MongoDB
{
  public static class MongoCollectionExt
  {
    public static IMongoQueryable<TaskDataModel> FilterCollectionAsync(this IMongoCollection<TaskDataModel> taskCollection,
                                                                  IClientSessionHandle                 sessionHandle,
                                                                  TaskFilter                           filter)
      => taskCollection.AsQueryable(sessionHandle)
                       .FilterQuery(filter);

    private static IMongoQueryable<TaskDataModel> FilterQuery(this IMongoQueryable<TaskDataModel> taskQueryable,
                                                              TaskFilter                          filter)
      => taskQueryable.Where(filter.ToFilterExpression());

    public static Expression<Func<TaskDataModel, bool>> ToFilterExpression(this TaskFilter filter)
    {

      var x = Expression.Parameter(typeof(TaskDataModel),
                                   "model");

      var output = FieldFilterInternal<TaskDataModel, string>(nameof(TaskDataModel.SessionId),
                                                              new[] { filter.SessionId },
                                                              true,
                                                              x);

      if (!string.IsNullOrEmpty(filter.SubSessionId))
        output = Expression.And(output,
                                FieldFilterInternal<TaskDataModel, string>(nameof(TaskDataModel.SubSessionId),
                                                                           new[] { filter.SubSessionId },
                                                                           true,
                                                                           x));

      if (filter.IncludedTaskIds.Any())
        output = Expression.And(output,
                                FieldFilterInternal<TaskDataModel, string>(nameof(TaskDataModel.TaskId),
                                                                           filter.IncludedTaskIds,
                                                                           true,
                                                                           x));

      if (filter.ExcludedTaskIds.Any())
        output = Expression.And(output,
                                FieldFilterInternal<TaskDataModel, string>(nameof(TaskDataModel.TaskId),
                                                                           filter.ExcludedTaskIds,
                                                                           false,
                                                                           x));

      if (filter.IncludedStatuses.Any())
        output = Expression.And(output,
                                FieldFilterInternal<TaskDataModel, TaskStatus>(nameof(TaskDataModel.Status),
                                                                           filter.IncludedStatuses,
                                                                           true,
                                                                           x));

      if (filter.ExcludedStatuses.Any())
        output = Expression.And(output,
                                FieldFilterInternal<TaskDataModel, TaskStatus>(nameof(TaskDataModel.Status),
                                                                               filter.ExcludedStatuses,
                                                                               false,
                                                                               x));

      return (Expression<Func<TaskDataModel, bool>>)Expression.Lambda(output,
                                                                      x);
    }


    public static Expression<Func<TModel, bool>> FieldFilterExpression<TModel, TField>(string              fieldName,
                                                                                       IEnumerable<TField> values,
                                                                                       bool                include = true)
    {
      var x = Expression.Parameter(typeof(TModel),
                                   "model");

      return (Expression<Func<TModel, bool>>) Expression.Lambda(FieldFilterInternal<TModel, TField>(fieldName, values, include, x), x);
    }

    private static Expression FieldFilterInternal<TModel, TField>(string fieldName, IEnumerable<TField> values, bool include, Expression x)
      => values.Aggregate(
                          (Expression)Expression.Constant(!include),
                          (expression, subSession) =>
                          {
                            var left = expression;
                            var right = Expression.Equal(Expression.Property(x,
                                                                             typeof(TModel),
                                                                             fieldName),
                                                         Expression.Constant(subSession,
                                                                             typeof(TField)));
                            if (include)
                              return Expression.Or(left,
                                                   right);

                            return Expression.And(left,
                                                  right);
                          }
                         );

    public static IMongoQueryable<TModel> FilterField<TModel, TField>(this IMongoQueryable<TModel> taskQueryable,
                                                                      string                       fieldName,
                                                                      IEnumerable<TField>          values,
                                                                      bool                         include = true)
      => taskQueryable.Where(FieldFilterExpression<TModel, TField>(fieldName,
                                                                   values,
                                                                   include));
  }
}
