// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-$CURRENT_YEAR$. All rights reserved.
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
// but WITHOUT ANY WARRANTY, without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Linq.Expressions;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Sessions;
using ArmoniK.Core.Common.Storage;

using LinqKit;

namespace ArmoniK.Core.Common.gRPC;

public static class ListSessionsRequestExt
{
  public static Expression<Func<SessionData, object?>> ToSessionDataField(this ListSessionsRequest.Types.Sort sort)
  {
    switch (sort.Field)
    {
      case ListSessionsRequest.Types.OrderByField.SessionId:
        return session => session.SessionId;

      case ListSessionsRequest.Types.OrderByField.Status:
        return session => session.Status;

      case ListSessionsRequest.Types.OrderByField.CreatedAt:
        return session => session.CreationDate;

      case ListSessionsRequest.Types.OrderByField.CancelledAt:
        return session => session.CancellationDate;

      case ListSessionsRequest.Types.OrderByField.Unspecified:
      default:
        throw new InvalidOperationException();
    }
  }

  public static Expression<Func<SessionData, bool>> ToSessionDataFilter(this ListSessionsRequest.Types.Filter filter)
  {
    var predicate = PredicateBuilder.New<SessionData>();
    predicate = predicate.And(data => true);

    if (!string.IsNullOrEmpty(filter.SessionId))
    {
      predicate = predicate.And(data => data.SessionId == filter.SessionId);
    }

    if (!string.IsNullOrEmpty(filter.ApplicationName))
    {
      predicate = predicate.And(data => data.Options.ApplicationName == filter.ApplicationName);
    }

    if (!string.IsNullOrEmpty(filter.ApplicationVersion))
    {
      predicate = predicate.And(data => data.Options.ApplicationVersion == filter.ApplicationVersion);
    }

    if (filter.CreatedAfter is not null)
    {
      predicate = predicate.And(data => data.CreationDate > filter.CreatedAfter.ToDateTime());
    }

    if (filter.CreatedBefore is not null)
    {
      predicate = predicate.And(data => data.CreationDate < filter.CreatedBefore.ToDateTime());
    }

    if (filter.CancelledAfter is not null)
    {
      predicate = predicate.And(data => data.CancellationDate > filter.CancelledAfter.ToDateTime());
    }

    if (filter.CancelledBefore is not null)
    {
      predicate = predicate.And(data => data.CancellationDate < filter.CancelledBefore.ToDateTime());
    }

    if (filter.Status != SessionStatus.Unspecified)
    {
      predicate = predicate.And(data => data.Status == filter.Status);
    }

    return predicate;
  }
}
