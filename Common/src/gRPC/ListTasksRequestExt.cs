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

using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Core.Common.Storage;

using LinqKit;

namespace ArmoniK.Core.Common.gRPC;

public static class ListTasksRequestExt
{
  public static Expression<Func<TaskData, object?>> ToTaskDataField(this ListTasksRequest.Types.Sort sort)
  {
    switch (sort.Field.FieldCase)
    {
      case TaskField.FieldOneofCase.TaskSummaryField:
        return sort.Field.TaskSummaryField.Field switch
               {
                 TaskSummaryEnumField.TaskId                  => data => data.TaskId,
                 TaskSummaryEnumField.SessionId               => data => data.SessionId,
                 TaskSummaryEnumField.OwnerPodId              => data => data.OwnerPodId,
                 TaskSummaryEnumField.InitialTaskId           => data => data.InitialTaskId,
                 TaskSummaryEnumField.Status                  => data => data.Status,
                 TaskSummaryEnumField.CreatedAt               => data => data.CreationDate,
                 TaskSummaryEnumField.SubmittedAt             => data => data.SubmittedDate,
                 TaskSummaryEnumField.StartedAt               => data => data.StartDate,
                 TaskSummaryEnumField.EndedAt                 => data => data.EndDate,
                 TaskSummaryEnumField.CreationToEndDuration   => data => data.CreationToEndDuration,
                 TaskSummaryEnumField.ProcessingToEndDuration => data => data.ProcessingToEndDuration,
                 TaskSummaryEnumField.PodTtl                  => data => data.PodTtl,
                 TaskSummaryEnumField.PodHostname             => data => data.OwnerPodName,
                 TaskSummaryEnumField.ReceivedAt              => data => data.ReceptionDate,
                 TaskSummaryEnumField.AcquiredAt              => data => data.AcquisitionDate,
                 TaskSummaryEnumField.Error                   => data => data.Output.Error,
                 TaskSummaryEnumField.Unspecified             => throw new ArgumentOutOfRangeException(),
                 _                                            => throw new ArgumentOutOfRangeException(),
               };
      case TaskField.FieldOneofCase.TaskOptionField:
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
      case TaskField.FieldOneofCase.TaskOptionGenericField:
        return data => data.Options.Options[sort.Field.TaskOptionGenericField.Field];
      case TaskField.FieldOneofCase.None:
      default:
        throw new ArgumentOutOfRangeException();
    }
  }

  public static Expression<Func<TaskData, bool>> ToTaskDataFilter(this ListTasksRequest.Types.Filter filter)
  {
    var predicate = PredicateBuilder.New<TaskData>();
    predicate = predicate.And(data => true);

    if (!string.IsNullOrEmpty(filter.SessionId))
    {
      predicate = predicate.And(data => data.SessionId == filter.SessionId);
    }

    if (filter.CreatedAfter is not null)
    {
      predicate = predicate.And(data => data.CreationDate > filter.CreatedAfter.ToDateTime());
    }

    if (filter.CreatedBefore is not null)
    {
      predicate = predicate.And(data => data.CreationDate < filter.CreatedBefore.ToDateTime());
    }

    if (filter.EndedAfter is not null)
    {
      predicate = predicate.And(data => data.EndDate > filter.EndedAfter.ToDateTime());
    }

    if (filter.EndedBefore is not null)
    {
      predicate = predicate.And(data => data.EndDate < filter.EndedBefore.ToDateTime());
    }

    if (filter.StartedAfter is not null)
    {
      predicate = predicate.And(data => data.StartDate > filter.StartedAfter.ToDateTime());
    }

    if (filter.StartedBefore is not null)
    {
      predicate = predicate.And(data => data.StartDate < filter.StartedBefore.ToDateTime());
    }

    if (filter.Status.Any())
    {
      predicate = predicate.And(data => filter.Status.Contains(data.Status));
    }

    return predicate;
  }
}
