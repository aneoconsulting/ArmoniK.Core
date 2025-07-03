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

using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Storage;

using Google.Protobuf.WellKnownTypes;

namespace ArmoniK.Core.Common.gRPC.Convertors;

/// <summary>
///   Provides extension methods for converting between internal TaskOptions and gRPC TaskOptions formats.
/// </summary>
/// <remarks>
///   This static class facilitates the bidirectional conversion between the internal representation
///   of task options and their gRPC counterparts used for external API communication.
/// </remarks>
public static class TaskOptionsExt
{
  /// <summary>
  ///   Converts a gRPC TaskOptions message to the internal TaskOptions representation.
  /// </summary>
  /// <param name="taskOption">The gRPC task options to convert.</param>
  /// <returns>A new TaskOptions instance with values copied from the gRPC message.</returns>
  public static TaskOptions ToTaskOptions(this Api.gRPC.V1.TaskOptions taskOption)
    => new(taskOption.Options,
           taskOption.MaxDuration.ToTimeSpan(),
           taskOption.MaxRetries,
           taskOption.Priority,
           taskOption.PartitionId,
           taskOption.ApplicationName,
           taskOption.ApplicationVersion,
           taskOption.ApplicationNamespace,
           taskOption.ApplicationService,
           taskOption.EngineType);

  /// <summary>
  ///   Converts a nullable gRPC TaskOptions to a nullable internal TaskOptions.
  /// </summary>
  /// <param name="taskOption">The nullable gRPC task options to convert.</param>
  /// <returns>
  ///   A new TaskOptions instance filled with the values from the gRPC message if the input is not null; otherwise, null.
  /// </returns>
  public static TaskOptions? ToNullableTaskOptions(this Api.gRPC.V1.TaskOptions? taskOption)
    => taskOption?.ToTaskOptions();

  /// <summary>
  ///   Converts an internal TaskOptions to its gRPC representation.
  /// </summary>
  /// <param name="taskOption">The internal task options to convert.</param>
  /// <returns>A new gRPC TaskOptions message populated with values from the internal representation.</returns>
  public static Api.gRPC.V1.TaskOptions ToGrpcTaskOptions(this TaskOptions taskOption)
    => new()
       {
         MaxDuration          = Duration.FromTimeSpan(taskOption.MaxDuration),
         ApplicationName      = taskOption.ApplicationName,
         ApplicationVersion   = taskOption.ApplicationVersion,
         ApplicationNamespace = taskOption.ApplicationNamespace,
         ApplicationService   = taskOption.ApplicationService,
         EngineType           = taskOption.EngineType,
         MaxRetries           = taskOption.MaxRetries,
         Options =
         {
           taskOption.Options,
         },
         Priority    = taskOption.Priority,
         PartitionId = taskOption.PartitionId,
       };

  /// <summary>
  ///   Converts a TaskOptionsHolder to its gRPC representation.
  /// </summary>
  /// <param name="taskOption">The task options holder to convert.</param>
  /// <returns>A new gRPC TaskOptions message populated with values from the task options holder.</returns>
  /// <remarks>
  ///   This method handles the TaskOptionsHolder class, which is a storage-specific implementation
  ///   of task options used within the ArmoniK system.
  /// </remarks>
  public static Api.gRPC.V1.TaskOptions ToGrpcTaskOptions(this TaskOptionsHolder taskOption)
    => new()
       {
         MaxDuration          = Duration.FromTimeSpan(taskOption.MaxDuration),
         ApplicationName      = taskOption.ApplicationName,
         ApplicationVersion   = taskOption.ApplicationVersion,
         ApplicationNamespace = taskOption.ApplicationNamespace,
         ApplicationService   = taskOption.ApplicationService,
         EngineType           = taskOption.EngineType,
         MaxRetries           = taskOption.MaxRetries,
         Options =
         {
           taskOption.Options,
         },
         Priority    = taskOption.Priority,
         PartitionId = taskOption.PartitionId,
       };
}
