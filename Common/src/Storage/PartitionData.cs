// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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
/// <param name="PodReserved">Number of reserved pods</param>
/// <param name="PodMax">Max number of pods</param>
/// <param name="PreemptionPercentage">Percentage of pods that can be preempted</param>
/// <param name="Priority">Priority of the partition</param>
/// <param name="PodConfiguration">Pod configuration used to select machines</param>
public record PartitionData(string            PartitionId,
                            IList<string>     ParentPartitionIds,
                            int               PodReserved,
                            int               PodMax,
                            int               PreemptionPercentage,
                            int               Priority,
                            PodConfiguration? PodConfiguration);
