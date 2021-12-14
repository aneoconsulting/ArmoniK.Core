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
using System.Linq;
using System.Linq.Expressions;

using ArmoniK.Core.gRPC.V1;

using MongoDB.Driver;
using MongoDB.Driver.Linq;

namespace ArmoniK.Adapters.MongoDB
{
  public static class MongoCollectionExt
  {
    public static IMongoQueryable<TaskDataModel> FilterQueryAsync(this IMongoCollection<TaskDataModel> taskCollection,
                                                                  IClientSessionHandle                 sessionHandle,
                                                                  TaskFilter                           filter)
    {
      return taskCollection.AsQueryable(sessionHandle)
                           .Where(ToFilterExpression(filter));
    }

    public static FilterDefinition<TaskDataModel> ToFilterDefinition(this TaskFilter filter)
    {
      return Builders<TaskDataModel>.Filter.Where(filter.ToFilterExpression());
    }

    public static Expression<Func<TaskDataModel, bool>> ToFilterExpression(this TaskFilter filter)
    {
      const TaskDataModel tdm = null;

      var x = Expression.Parameter(typeof(TaskDataModel),
                                   "taskData");

      Expression sessionCheck = string.IsNullOrEmpty(filter.SessionId)
                                  ? Expression.Constant(true,
                                                        typeof(bool))
                                  : Expression.Equal(Expression.Property(x,
                                                                         typeof(TaskDataModel),
                                                                         nameof(tdm.SessionId)),
                                                     Expression.Constant(filter.SessionId,
                                                                         typeof(string)));

      Expression subSessionCheck = string.IsNullOrEmpty(filter.SubSessionId)
                                     ? Expression.Constant(true,
                                                           typeof(bool))
                                     : Expression.Equal(Expression.Property(x,
                                                                            typeof(TaskDataModel),
                                                                            nameof(tdm.SubSessionId)),
                                                        Expression.Constant(filter.SubSessionId,
                                                                            typeof(string)));

      var globalExpression = Expression.And(sessionCheck,
                                            subSessionCheck);

      if (filter.IncludedStatuses is not null && filter.IncludedStatuses.Any())
      {
        var includeStatusesExpression = filter.IncludedStatuses.Aggregate(
                                                                          (Expression)Expression.Constant(false),
                                                                          (expression, status) =>
                                                                          {
                                                                            var left = expression;
                                                                            var right = Expression.Equal(Expression.Property(x,
                                                                                                                             typeof(TaskDataModel),
                                                                                                                             nameof(tdm.Status)),
                                                                                                         Expression.Constant(status,
                                                                                                                             typeof(TaskStatus)));
                                                                            return Expression.Or(left,
                                                                                                 right);
                                                                          }
                                                                         );
        globalExpression = Expression.And(globalExpression,
                                          includeStatusesExpression);
      }

      if (filter.ExcludedStatuses is not null && filter.ExcludedStatuses.Any())
      {
        var excludeStatusesExpression = filter.ExcludedStatuses.Aggregate(
                                                                          (Expression)Expression.Constant(true),
                                                                          (expression, status) =>
                                                                          {
                                                                            var left = expression;
                                                                            var right = Expression.Equal(Expression.Property(x,
                                                                                                                             typeof(TaskDataModel),
                                                                                                                             nameof(tdm.Status)),
                                                                                                         Expression.Constant(status,
                                                                                                                             typeof(TaskStatus)));
                                                                            return Expression.And(left,
                                                                                                  right);
                                                                          }
                                                                         );
        globalExpression = Expression.And(globalExpression,
                                          excludeStatusesExpression);
      }

      if (filter.IncludedTaskIds is not null && filter.IncludedTaskIds.Any())
      {
        var includeTaskIdExpression = filter.IncludedTaskIds.Aggregate(
                                                                       (Expression)Expression.Constant(false),
                                                                       (expression, id) =>
                                                                       {
                                                                         var left = expression;
                                                                         var right = Expression.Equal(Expression.Property(x,
                                                                                                                          typeof(TaskDataModel),
                                                                                                                          nameof(tdm.TaskId)),
                                                                                                      Expression.Constant(id,
                                                                                                                          typeof(string)));
                                                                         return Expression.Or(left,
                                                                                              right);
                                                                       }
                                                                      );
        globalExpression = Expression.And(globalExpression,
                                          includeTaskIdExpression);
      }

      if (filter.ExcludedTaskIds is not null && filter.ExcludedTaskIds.Any())
      {
        var excludeTaskIdExpression = filter.ExcludedTaskIds.Aggregate(
                                                                       (Expression)Expression.Constant(true),
                                                                       (expression, id) =>
                                                                       {
                                                                         var left = expression;
                                                                         var right = Expression.Equal(Expression.Property(x,
                                                                                                                          typeof(TaskDataModel),
                                                                                                                          nameof(tdm.TaskId)),
                                                                                                      Expression.Constant(id,
                                                                                                                          typeof(string)));
                                                                         return Expression.And(left,
                                                                                               right);
                                                                       }
                                                                      );
        globalExpression = Expression.And(globalExpression,
                                          excludeTaskIdExpression);
      }

      return (Expression<Func<TaskDataModel, bool>>)Expression.Lambda(globalExpression,
                                                                      x);
    }
  }
}
