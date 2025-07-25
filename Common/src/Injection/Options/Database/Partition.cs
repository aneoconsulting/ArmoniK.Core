// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;

using ArmoniK.Core.Common.Storage;

namespace ArmoniK.Core.Common.Injection.Options.Database;

/// <inheritdoc cref="PartitionData" />
public record Partition
{
  /// <inheritdoc cref="PartitionData.PartitionId" />
  public required string PartitionId { get; init; }

  /// <inheritdoc cref="PartitionData.ParentPartitionIds" />
  public required IList<string> ParentPartitionIds { get; init; }

  /// <inheritdoc cref="PartitionData.PodReserved" />
  public required int PodReserved { get; init; }

  /// <inheritdoc cref="PartitionData.PodMax" />
  public required int PodMax { get; init; }

  /// <inheritdoc cref="PartitionData.PreemptionPercentage" />
  public required int PreemptionPercentage { get; init; }

  /// <inheritdoc cref="PartitionData.Priority" />
  public required int Priority { get; init; }

  /// <inheritdoc cref="PartitionData.PodConfiguration" />
  public IDictionary<string, string> PodConfiguration { get; init; } = new Dictionary<string, string>();

  /// <inheritdoc />
  public virtual bool Equals(Partition? other)
    => !ReferenceEquals(other,
                        null)                && PartitionId.Equals(other.PartitionId) && ParentPartitionIds.SequenceEqual(other.ParentPartitionIds) &&
       PodReserved.Equals(other.PodReserved) && PodMax.Equals(other.PodMax)           && PreemptionPercentage.Equals(other.PreemptionPercentage)    &&
       Priority.Equals(other.Priority)       && PodConfiguration.SequenceEqual(other.PodConfiguration);

  /// <summary>
  ///   Convert <inheritdoc cref="Partition" /> to JSON
  /// </summary>
  /// <returns>
  ///   <inheritdoc cref="string" /> representing the JSON object of this instance
  /// </returns>
  public string ToJson()
    => JsonSerializer.Serialize(this);

  /// <summary>
  ///   Build a <inheritdoc cref="Partition" /> from a JSON <inheritdoc cref="string" />
  /// </summary>
  /// <param name="json">JSON value</param>
  /// <returns>
  ///   <inheritdoc cref="Partition" /> built from the provided JSON
  /// </returns>
  public static Partition FromJson(string json)
    => JsonSerializer.Deserialize<Partition>(json)!;

  /// <inheritdoc />
  public override int GetHashCode()
    => HashCode.Combine(PartitionId,
                        ParentPartitionIds,
                        PodReserved,
                        PodMax,
                        PreemptionPercentage,
                        Priority,
                        PodConfiguration);
}
