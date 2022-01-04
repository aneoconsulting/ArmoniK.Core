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

namespace ArmoniK.Adapters.MongoDB
{
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

      if (!string.IsNullOrEmpty(filter.SessionId))
        output = Expression.And(output,
                                ExpressionsBuilders.FieldFilterInternal(model => model.SessionId,
                                                                        new[] { filter.SessionId },
                                                                        true,
                                                                        x));

      if (!string.IsNullOrEmpty(filter.SubSessionId))
        output = Expression.And(output,
                                ExpressionsBuilders.FieldFilterInternal(model => model.SubSessionId,
                                                                        new[] { filter.SubSessionId },
                                                                        true,
                                                                        x));

      if (filter.IncludedTaskIds is not null && filter.IncludedTaskIds.Any())
        output = Expression.And(output,
                                ExpressionsBuilders.FieldFilterInternal(model => model.TaskId,
                                                                        filter.IncludedTaskIds,
                                                                        true,
                                                                        x));

      if (filter.ExcludedTaskIds is not null && filter.ExcludedTaskIds.Any())
        output = Expression.And(output,
                                ExpressionsBuilders.FieldFilterInternal(model => model.TaskId,
                                                                        filter.ExcludedTaskIds,
                                                                        false,
                                                                        x));

      if (filter.IncludedStatuses is not null && filter.IncludedStatuses.Any())
        output = Expression.And(output,
                                ExpressionsBuilders.FieldFilterInternal(model => model.Status,
                                                                        filter.IncludedStatuses,
                                                                        true,
                                                                        x));

      if (filter.ExcludedStatuses is not null && filter.ExcludedStatuses.Any())
        output = Expression.And(output,
                                ExpressionsBuilders.FieldFilterInternal(model => model.Status,
                                                                        filter.ExcludedStatuses,
                                                                        false,
                                                                        x));

      return (Expression<Func<TaskDataModel, bool>>)Expression.Lambda(output,
                                                                      x);
    }
  }
}
