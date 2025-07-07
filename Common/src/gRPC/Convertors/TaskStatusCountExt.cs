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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Storage;

namespace ArmoniK.Core.Common.gRPC.Convertors;

/// <summary>
///   Extension methods for converting between internal and gRPC <see cref="StatusCount" /> statuses.
/// </summary>
public static class TaskStatusCountExt
{
  /// <summary>
  ///   Conversion operator from <see cref="TaskStatusCount" /> to <see cref="StatusCount" />
  /// </summary>
  /// <param name="taskStatusCount">The input status count</param>
  /// <returns>
  ///   The converted status count
  /// </returns>
  public static StatusCount ToGrpcStatusCount(this TaskStatusCount taskStatusCount)
    => new()
       {
         Count  = taskStatusCount.Count,
         Status = taskStatusCount.Status.ToGrpcStatus(),
       };
}
