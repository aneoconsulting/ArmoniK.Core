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

using Armonik.Api.Grpc.V1.Partitions;

using ArmoniK.Core.Common.Storage;

using LinqKit;

namespace ArmoniK.Core.Common.gRPC;

public static class ListPartitionsRequestExt
{
  public static Expression<Func<PartitionData, object?>> ToPartitionField(this ListPartitionsRequest.Types.Sort sort)
  {
    switch (sort.Field.FieldCase)
    {
      case PartitionField.FieldOneofCase.PartitionRawField:
        return sort.Field.PartitionRawField.Field switch
               {
                 PartitionRawEnumField.Id                   => partitionData => partitionData.PartitionId,
                 PartitionRawEnumField.ParentPartitionIds   => partitionData => partitionData.ParentPartitionIds,
                 PartitionRawEnumField.PodReserved          => partitionData => partitionData.PodReserved,
                 PartitionRawEnumField.PodMax               => partitionData => partitionData.PodMax,
                 PartitionRawEnumField.PreemptionPercentage => partitionData => partitionData.PreemptionPercentage,
                 PartitionRawEnumField.Priority             => partitionData => partitionData.Priority,
                 PartitionRawEnumField.Unspecified          => throw new ArgumentOutOfRangeException(),
                 _                                          => throw new ArgumentOutOfRangeException(),
               };

      case PartitionField.FieldOneofCase.None:
      default:
        throw new ArgumentOutOfRangeException();
    }
  }

  public static Expression<Func<PartitionData, bool>> ToPartitionFilter(this ListPartitionsRequest.Types.Filter filter)
  {
    var predicate = PredicateBuilder.New<PartitionData>();
    predicate = predicate.And(data => true);

    if (!string.IsNullOrEmpty(filter.Id))
    {
      predicate = predicate.And(data => data.PartitionId == filter.Id);
    }

    if (!string.IsNullOrEmpty(filter.ParentPartitionId))
    {
      predicate = predicate.And(data => data.ParentPartitionIds.Contains(filter.ParentPartitionId));
    }

    if (filter.Priority > 0)
    {
      predicate = predicate.And(data => data.Priority == filter.Priority);
    }

    if (filter.PodMax > 0)
    {
      predicate = predicate.And(data => data.PodMax == filter.PodMax);
    }

    if (filter.PodReserved > 0)
    {
      predicate = predicate.And(data => data.PodReserved == filter.PodReserved);
    }

    if (filter.PreemptionPercentage > 0)
    {
      predicate = predicate.And(data => data.PreemptionPercentage == filter.PreemptionPercentage);
    }

    return predicate;
  }
}
