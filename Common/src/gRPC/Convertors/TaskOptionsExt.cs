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

using ArmoniK.Core.Base.DataStructures;

using Google.Protobuf.WellKnownTypes;

namespace ArmoniK.Core.Common.gRPC.Convertors;

public static class TaskOptionsExt
{
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

  public static TaskOptions? ToNullableTaskOptions(this Api.gRPC.V1.TaskOptions? taskOption)
    => taskOption?.ToTaskOptions();

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
}
