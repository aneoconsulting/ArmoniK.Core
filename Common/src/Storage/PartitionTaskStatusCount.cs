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

using ArmoniK.Api.gRPC.V1;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Data structure to hold the count of tasks given their status and partition id
/// </summary>
/// <param name="PartitionId">Partition identifier</param>
/// <param name="Status">Task status</param>
/// <param name="Count">Number of task with the corresponding status and partition id</param>
public record PartitionTaskStatusCount(string     PartitionId,
                                       TaskStatus Status,
                                       int        Count);
