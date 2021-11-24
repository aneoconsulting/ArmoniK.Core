// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

using ArmoniK.Core.gRPC.V1;

using MongoDB.Driver;
using MongoDB.Driver.Linq;

namespace ArmoniK.Adapters.MongoDB
{
  public static class MongoCollectionExt
  {
    public static Task InitializeIndexesAsync(this IMongoCollection<SessionDataModel> sessionCollection,
                                              IClientSessionHandle sessionHandle)
    {
      var sessionIndex = Builders<SessionDataModel>.IndexKeys.Text(model => model.SessionId);
      var subSessionIndex = Builders<SessionDataModel>.IndexKeys.Text(model => model.SubSessionId);
      var parentsIndex = Builders<SessionDataModel>.IndexKeys.Text("ParentsId.Id");
      var sessionSubSessionIndex = Builders<SessionDataModel>.IndexKeys.Combine(sessionIndex, subSessionIndex);
      var sessionParentIndex = Builders<SessionDataModel>.IndexKeys.Combine(sessionIndex, parentsIndex);

      var indexModels = new CreateIndexModel<SessionDataModel>[]
                        {
                          new(sessionIndex, new CreateIndexOptions { Name           = nameof(sessionIndex) }),
                          new(sessionSubSessionIndex, new CreateIndexOptions { Name = nameof(sessionSubSessionIndex), Unique = true }),
                          new(sessionParentIndex, new CreateIndexOptions { Name     = nameof(sessionParentIndex) }),
                        };

      return sessionCollection.Indexes.CreateManyAsync(sessionHandle, indexModels);
    }

    public static Task InitializeIndexesAsync(this IMongoCollection<TaskDataModel> taskCollection,
                                              IClientSessionHandle sessionHandle)
    {
      var sessionIndex = Builders<TaskDataModel>.IndexKeys.Text(model => model.SessionId);
      var subSessionIndex = Builders<TaskDataModel>.IndexKeys.Text(model => model.SubSessionId);
      var taskIndex = Builders<TaskDataModel>.IndexKeys.Text(model => model.TaskId);
      var statusIndex = Builders<TaskDataModel>.IndexKeys.Text(model => model.Status);
      var taskIdIndex = Builders<TaskDataModel>.IndexKeys.Combine(sessionIndex, subSessionIndex, taskIndex);
      var sessionStatusIndex = Builders<TaskDataModel>.IndexKeys.Combine(sessionIndex, statusIndex);

      var indexModels = new CreateIndexModel<TaskDataModel>[]
                        {
                          new(sessionIndex, new CreateIndexOptions { Name       = nameof(sessionIndex) }),
                          new(taskIdIndex, new CreateIndexOptions { Name        = nameof(taskIdIndex), Unique = true }),
                          new(sessionStatusIndex, new CreateIndexOptions { Name = nameof(sessionStatusIndex) }),
                        };

      return taskCollection.Indexes.CreateManyAsync(sessionHandle, indexModels);
    }


    public static IMongoQueryable<TaskDataModel> FilterQueryAsync(this IMongoCollection<TaskDataModel> taskCollection,
                                                             IClientSessionHandle sessionHandle,
                                                             TaskFilter filter)
      => taskCollection.AsQueryable(sessionHandle)
                       .Where(ToFilterExpression(filter));

    public static Expression<Func<TaskDataModel, bool>> ToFilterExpression(this TaskFilter filter)
    {
      return x => x.SessionId == filter.SessionId &&
                                         x.SubSessionId == filter.SubSessionId &&
                                         filter.IncludedStatuses.Any(status => status == x.Status) &&
                                         filter.ExcludedStatuses.All(status => status != x.Status) &&
                                         filter.IncludedTaskIds.Any(tId => tId == x.TaskId) &&
                                         filter.ExcludedTaskIds.All(tId => tId != x.TaskId);
    }
  }
}
