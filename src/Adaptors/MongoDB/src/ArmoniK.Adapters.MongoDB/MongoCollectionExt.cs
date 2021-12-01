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
                                                             IClientSessionHandle sessionHandle,
                                                             TaskFilter filter)
      => taskCollection.AsQueryable(sessionHandle)
                       .Where(ToFilterExpression(filter));

    public static Expression<Func<TaskDataModel, bool>> ToFilterExpression(this TaskFilter filter)
    {
      return x => x.SessionId == filter.SessionId &&
                  x.SubSessionId == filter.SubSessionId &&
                  filter.IncludedStatuses.Aggregate(false, (b, status) => b || status == x.Status) &&
                  filter.ExcludedStatuses.All(status => status != x.Status) &&
                  filter.IncludedTaskIds.Aggregate(false, (b, tid) => b || tid == x.TaskId) &&
                  filter.ExcludedTaskIds.All(tId => tId != x.TaskId);
    }
  }
}
