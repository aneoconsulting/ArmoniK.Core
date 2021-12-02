// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

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
      => taskCollection.AsQueryable(sessionHandle)
                       .Where(ToFilterExpression(filter));

    public static Expression<Func<TaskDataModel, bool>> ToFilterExpression(this TaskFilter filter)
    {
      const TaskDataModel tdm = null;

      var x = Expression.Parameter(typeof(TaskDataModel), "taskData");

      Expression sessionCheck = string.IsNullOrEmpty(filter.SessionId)
                                  ? Expression.Constant(true, typeof(bool))
                                  : Expression.Equal(Expression.Property(x, typeof(TaskDataModel), nameof(tdm.SessionId)),
                                                     Expression.Constant(filter.SessionId, typeof(string)));

      Expression subSessionCheck = string.IsNullOrEmpty(filter.SubSessionId)
                                     ? Expression.Constant(true, typeof(bool))
                                     : Expression.Equal(Expression.Property(x, typeof(TaskDataModel), nameof(tdm.SubSessionId)),
                                                        Expression.Constant(filter.SubSessionId, typeof(string)));

      var globalExpression = Expression.And(sessionCheck, subSessionCheck);

      var includeStatusesExpression = filter.IncludedStatuses.Aggregate
        (
         (Expression)Expression.Constant(false),
         (expression, status) =>
         {
           var left = expression;
           var right = Expression.Equal(Expression.Property(x, typeof(TaskDataModel), nameof(tdm.Status)),
                                        Expression.Constant(status, typeof(TaskStatus)));
           return Expression.Or(left, right);
         }
        );
      globalExpression = Expression.And(globalExpression, includeStatusesExpression);

      var excludeStatusesExpression = filter.ExcludedStatuses.Aggregate
        (
         (Expression)Expression.Constant(true),
         (expression, status) =>
         {
           var left = expression;
           var right = Expression.Equal(Expression.Property(x, typeof(TaskDataModel), nameof(tdm.Status)),
                                        Expression.Constant(status, typeof(TaskStatus)));
           return Expression.And(left, right);
         }
        );
      globalExpression = Expression.And(globalExpression, excludeStatusesExpression);

      var includeTaskIdExpression = filter.IncludedTaskIds.Aggregate
        (
         (Expression)Expression.Constant(false),
         (expression, id) =>
         {
           var left = expression;
           var right = Expression.Equal(Expression.Property(x, typeof(TaskDataModel), nameof(tdm.TaskId)),
                                        Expression.Constant(id, typeof(string)));
           return Expression.Or(left, right);
         }
        );
      globalExpression = Expression.And(globalExpression, includeTaskIdExpression);

      var excludeTaskIdExpression = filter.ExcludedTaskIds.Aggregate
        (
         (Expression)Expression.Constant(true),
         (expression, id) =>
         {
           var left = expression;
           var right = Expression.Equal(Expression.Property(x, typeof(TaskDataModel), nameof(tdm.TaskId)),
                                        Expression.Constant(id, typeof(string)));
           return Expression.And(left, right);
         }
        );
      globalExpression = Expression.And(globalExpression, excludeTaskIdExpression);

      return (Expression<Func<TaskDataModel, bool>>)Expression.Lambda(globalExpression, x);

    }
  }
}
