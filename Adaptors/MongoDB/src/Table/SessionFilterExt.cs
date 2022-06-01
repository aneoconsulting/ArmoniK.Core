// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
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
using ArmoniK.Core.Common.Storage;

namespace ArmoniK.Core.Adapters.MongoDB.Table;

public static class SessionFilterExt
{
  public static IQueryable<SessionData> FilterQuery(this IQueryable<SessionData> sessionQueryable,
                                                    SessionFilter                filter)
    => sessionQueryable.Where(filter.ToFilterExpression());

  public static Expression<Func<SessionData, bool>> ToFilterExpression(this SessionFilter filter)
  {
    var x = Expression.Parameter(typeof(SessionData),
                                 "model");
    var output = (Expression)Expression.Constant(true,
                                                 typeof(bool));

    if (filter.Sessions.Any())
    {
      output = Expression.And(output,
                              ExpressionsBuilders.FieldFilterInternal<SessionData, string>(model => model.SessionId,
                                                                                           filter.Sessions,
                                                                                           true,
                                                                                           x));
    }

    switch (filter.StatusesCase)
    {
      case SessionFilter.StatusesOneofCase.Included:
      {
        if (filter.Included.Statuses is not null)
        {
          output = Expression.And(output,
                                  ExpressionsBuilders.FieldFilterInternal<SessionData, SessionStatus>(model => model.Status,
                                                                                                      filter.Included.Statuses,
                                                                                                      true,
                                                                                                      x));
        }

        break;
      }
      case SessionFilter.StatusesOneofCase.Excluded:
      {
        if (filter.Excluded.Statuses is not null)
        {
          output = Expression.And(output,
                                  ExpressionsBuilders.FieldFilterInternal<SessionData, SessionStatus>(model => model.Status,
                                                                                                      filter.Excluded.Statuses,
                                                                                                      false,
                                                                                                      x));
        }

        break;
      }
      case SessionFilter.StatusesOneofCase.None:
        break;
      default:
        throw new
          ArgumentException($"{nameof(TaskFilter.StatusesCase)} must be either {nameof(TaskFilter.StatusesOneofCase.Included)} or {nameof(TaskFilter.StatusesOneofCase.Excluded)}",
                            nameof(filter));
    }

    return (Expression<Func<SessionData, bool>>)Expression.Lambda(output,
                                                                  x);
  }
}
