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

using System;
using System.Linq;
using System.Linq.Expressions;

using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Common.gRPC.Convertors;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;
using ArmoniK.Utils;

namespace ArmoniK.Core.Common.gRPC;

public static class TaskFilterExt
{
  public static Expression<Func<TaskData, bool>> ToFilterExpression(this TaskFilter filter)
  {
    var x = Expression.Parameter(typeof(TaskData),
                                 "model");


    var output = (Expression)Expression.Constant(true,
                                                 typeof(bool));

    switch (filter.IdsCase)
    {
      case TaskFilter.IdsOneofCase.Session:
      {
        if (filter.Session.Ids is not null)
        {
          output = Expression.And(output,
                                  ExpressionsBuilders.FieldFilterInternal<TaskData, string>(model => model.SessionId,
                                                                                            filter.Session.Ids,
                                                                                            true,
                                                                                            x));
        }

        break;
      }
      case TaskFilter.IdsOneofCase.Task:
      {
        if (filter.Task.Ids is not null)
        {
          output = Expression.And(output,
                                  ExpressionsBuilders.FieldFilterInternal<TaskData, string>(model => model.TaskId,
                                                                                            filter.Task.Ids,
                                                                                            true,
                                                                                            x));
        }

        break;
      }

      case TaskFilter.IdsOneofCase.None:
      default:
        throw new ArgumentException("IdsCase must be either Task or Session",
                                    nameof(filter));
    }

    switch (filter.StatusesCase)
    {
      case TaskFilter.StatusesOneofCase.Included:
      {
        if (filter.Included.Statuses is not null)
        {
          output = Expression.And(output,
                                  ExpressionsBuilders.FieldFilterInternal<TaskData, TaskStatus>(model => model.Status,
                                                                                                filter.Included.Statuses.Select(status => status.ToInternalStatus())
                                                                                                      .AsIList(),
                                                                                                true,
                                                                                                x));
        }

        break;
      }
      case TaskFilter.StatusesOneofCase.Excluded:
      {
        if (filter.Excluded.Statuses is not null)
        {
          output = Expression.And(output,
                                  ExpressionsBuilders.FieldFilterInternal<TaskData, TaskStatus>(model => model.Status,
                                                                                                filter.Excluded.Statuses.Select(status => status.ToInternalStatus())
                                                                                                      .AsIList(),
                                                                                                false,
                                                                                                x));
        }

        break;
      }
      case TaskFilter.StatusesOneofCase.None:
        break;
      default:
        throw new
          ArgumentException($"{nameof(TaskFilter.StatusesCase)} must be either {nameof(TaskFilter.StatusesOneofCase.Included)} or {nameof(TaskFilter.StatusesOneofCase.Excluded)}",
                            nameof(filter));
    }

    return (Expression<Func<TaskData, bool>>)Expression.Lambda(output,
                                                               x);
  }
}
