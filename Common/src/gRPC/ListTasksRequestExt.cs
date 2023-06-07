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
        return sort.Field.TaskSummaryField switch
               {
                 TaskSummaryField.TaskId                  => data => data.TaskId,
                 TaskSummaryField.SessionId               => data => data.SessionId,
                 TaskSummaryField.OwnerPodId              => data => data.OwnerPodId,
                 TaskSummaryField.InitialTaskId           => data => data.InitialTaskId,
                 TaskSummaryField.Status                  => data => data.Status,
                 TaskSummaryField.CreatedAt               => data => data.CreationDate,
                 TaskSummaryField.SubmittedAt             => data => data.SubmittedDate,
                 TaskSummaryField.StartedAt               => data => data.StartDate,
                 TaskSummaryField.EndedAt                 => data => data.EndDate,
                 TaskSummaryField.CreationToEndDuration   => data => data.CreationToEndDuration,
                 TaskSummaryField.ProcessingToEndDuration => data => data.ProcessingToEndDuration,
                 TaskSummaryField.PodTtl                  => data => data.PodTtl,
                 TaskSummaryField.PodHostname             => data => data.OwnerPodName,
                 TaskSummaryField.ReceivedAt              => data => data.ReceptionDate,
                 TaskSummaryField.AcquiredAt              => data => data.AcquisitionDate,
                 TaskSummaryField.Error                   => data => data.Output.Error,
                 TaskSummaryField.Unspecified             => throw new ArgumentOutOfRangeException(),
                 _                                        => throw new ArgumentOutOfRangeException(),
               };
      case TaskField.FieldOneofCase.TaskOptionField:
        return sort.Field.TaskOptionField switch
               {
                 TaskOptionField.MaxDuration          => data => data.Options.MaxDuration,
                 TaskOptionField.MaxRetries           => data => data.Options.MaxRetries,
                 TaskOptionField.Priority             => data => data.Options.Priority,
                 TaskOptionField.PartitionId          => data => data.Options.PartitionId,
                 TaskOptionField.ApplicationName      => data => data.Options.ApplicationName,
                 TaskOptionField.ApplicationVersion   => data => data.Options.ApplicationVersion,
                 TaskOptionField.ApplicationNamespace => data => data.Options.ApplicationNamespace,
                 TaskOptionField.ApplicationService   => data => data.Options.ApplicationService,
                 TaskOptionField.EngineType           => data => data.Options.EngineType,
                 TaskOptionField.Unspecified          => throw new ArgumentOutOfRangeException(),
                 _                                    => throw new ArgumentOutOfRangeException(),
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
