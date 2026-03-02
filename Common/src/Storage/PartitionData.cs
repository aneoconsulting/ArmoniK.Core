// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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

using System.Collections.Generic;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Data structure to hold partitions metadata
/// </summary>
/// <param name="PartitionId">Unique name of the partition</param>
/// <param name="ParentPartitionIds">List of parents up to the first partition</param>
/// <param name="PodReserved">Number of pods permanently reserved for this partition</param>
/// <param name="PodMax">Maximum number of pods that may be allocated to this partition</param>
/// <param name="PreemptionPercentage">Percentage of this partition's pods that higher-priority work may preempt</param>
/// <param name="Priority">Scheduling priority of the partition relative to others</param>
/// <param name="PodConfiguration">Arbitrary key-value pairs passed as configuration to partition deployment tool</param>
public record PartitionData(string            PartitionId,
                            IList<string>     ParentPartitionIds,
                            int               PodReserved,
                            int               PodMax,
                            int               PreemptionPercentage,
                            int               Priority,
                            PodConfiguration? PodConfiguration);
