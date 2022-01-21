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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel;

namespace ArmoniK.Core.Adapters.MongoDB.Table;

public static class TaskFilterExt
{
  public static IQueryable<TaskDataModel> FilterQuery(this IQueryable<TaskDataModel> taskQueryable,
                                                      TaskFilter                     filter)
    => taskQueryable.Where(filter.ToFilterExpression());

  public static Expression<Func<TaskDataModel, bool>> ToFilterExpression(this TaskFilter filter)
  {
    var x = Expression.Parameter(typeof(TaskDataModel),
                                 "model");


    var output = (Expression)Expression.Constant(true,
                                                 typeof(bool));

    switch (filter.IdsCase)
    {
      case TaskFilter.IdsOneofCase.Unknown:
      {
        if (!string.IsNullOrEmpty(filter.Unknown.SessionId))
          output = Expression.And(output,
                                  ExpressionsBuilders.FieldFilterInternal(model => model.SessionId,
                                                                          new[] { filter.Unknown.SessionId },
                                                                          true,
                                                                          x));

        if (filter.Unknown.ExcludedTaskIds is not null && filter.Unknown.ExcludedTaskIds.Any())
          output = Expression.And(output,
                                  ExpressionsBuilders.FieldFilterInternal(model => model.TaskId,
                                                                          filter.Unknown.ExcludedTaskIds,
                                                                          false,
                                                                          x));
        break;
      }
      case TaskFilter.IdsOneofCase.Known:
      {
        if (filter.Known.TaskIds is not null && filter.Known.TaskIds.Any())
          output = Expression.And(output,
                                  ExpressionsBuilders.FieldFilterInternal(model => model.TaskId,
                                                                          filter.Known.TaskIds,
                                                                          true,
                                                                          x));
        break;
      }
      default:
        throw new ArgumentException($"{nameof(TaskFilter.IdsCase)} must be either {nameof(TaskFilter.IdsOneofCase.Known)} or {nameof(TaskFilter.IdsOneofCase.Unknown)}",
                                    nameof(filter));
    }

    switch (filter.StatusesCase)
    {
      case TaskFilter.StatusesOneofCase.Included:
      {
        if (filter.Included.IncludedStatuses is not null && filter.Included.IncludedStatuses.Any())
          output = Expression.And(output,
                                  ExpressionsBuilders.FieldFilterInternal(model => model.Status,
                                                                          filter.Included.IncludedStatuses,
                                                                          true,
                                                                          x));
        break;
      }
      case TaskFilter.StatusesOneofCase.Excluded:
      {
        if (filter.Excluded.IncludedStatuses is not null && filter.Excluded.IncludedStatuses.Any())
          output = Expression.And(output,
                                  ExpressionsBuilders.FieldFilterInternal(model => model.Status,
                                                                          filter.Excluded.IncludedStatuses,
                                                                          false,
                                                                          x));
        break;
      }
      default:
        throw new ArgumentException($"{nameof(TaskFilter.StatusesCase)} must be either {nameof(TaskFilter.StatusesOneofCase.Included)} or {nameof(TaskFilter.StatusesOneofCase.Excluded)}",
                                    nameof(filter));

    }


    return (Expression<Func<TaskDataModel, bool>>)Expression.Lambda(output,
                                                                    x);
  }
}