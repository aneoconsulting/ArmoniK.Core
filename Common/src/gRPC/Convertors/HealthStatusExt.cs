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

using ArmoniK.Api.gRPC.V1.HealthChecks;

using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ArmoniK.Core.Common.gRPC.Convertors;

internal static class HealthStatusExt
{
  public static HealthStatusEnum ToGrpcHealthStatusEnum(this HealthStatus status)
    => status switch
       {
         HealthStatus.Unhealthy => HealthStatusEnum.Unhealthy,
         HealthStatus.Degraded  => HealthStatusEnum.Degraded,
         HealthStatus.Healthy   => HealthStatusEnum.Healthy,
         _ => throw new ArgumentOutOfRangeException(nameof(status),
                                                    status,
                                                    "Unrecognized value"),
       };
}
