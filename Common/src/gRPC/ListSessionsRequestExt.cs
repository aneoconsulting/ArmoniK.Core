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
    switch (sort.Field.FieldCase)
    {
      case SessionField.FieldOneofCase.SessionRawField:
        return sort.Field.SessionRawField.Field switch
               {
                 SessionRawEnumField.SessionId    => session => session.SessionId,
                 SessionRawEnumField.Status       => session => session.Status,
                 SessionRawEnumField.PartitionIds => session => session.PartitionIds,
                 SessionRawEnumField.Options      => session => session.Options,
                 SessionRawEnumField.CreatedAt    => session => session.CreationDate,
                 SessionRawEnumField.CancelledAt  => session => session.CancellationDate,
                 SessionRawEnumField.Duration     => throw new ArgumentOutOfRangeException(),
                 SessionRawEnumField.Unspecified  => throw new ArgumentOutOfRangeException(),
                 _                                => throw new ArgumentOutOfRangeException(),
               };
      case SessionField.FieldOneofCase.TaskOptionField:
        return sort.Field.TaskOptionField.Field switch
               {
                 TaskOptionEnumField.MaxDuration          => data => data.Options.MaxDuration,
                 TaskOptionEnumField.MaxRetries           => data => data.Options.MaxRetries,
                 TaskOptionEnumField.Priority             => data => data.Options.Priority,
                 TaskOptionEnumField.PartitionId          => data => data.Options.PartitionId,
                 TaskOptionEnumField.ApplicationName      => data => data.Options.ApplicationName,
                 TaskOptionEnumField.ApplicationVersion   => data => data.Options.ApplicationVersion,
                 TaskOptionEnumField.ApplicationNamespace => data => data.Options.ApplicationNamespace,
                 TaskOptionEnumField.ApplicationService   => data => data.Options.ApplicationService,
                 TaskOptionEnumField.EngineType           => data => data.Options.EngineType,
                 TaskOptionEnumField.Unspecified          => throw new ArgumentOutOfRangeException(),
                 _                                        => throw new ArgumentOutOfRangeException(),
               };
      case SessionField.FieldOneofCase.TaskOptionGenericField:
        return data => data.Options.Options[sort.Field.TaskOptionGenericField.Field];
      case SessionField.FieldOneofCase.None:
      default:
        throw new ArgumentOutOfRangeException();
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
