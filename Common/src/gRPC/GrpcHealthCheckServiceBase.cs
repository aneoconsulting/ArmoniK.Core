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

using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Core.Base.DataStructures;

using Grpc.Core;
using Grpc.Health.V1;

using JetBrains.Annotations;

using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ArmoniK.Core.Common.gRPC;

[PublicAPI]
/// <inheritdoc cref="Health" />
public abstract class GrpcHealthCheckServiceBase : Health.HealthBase
{
  private readonly string[]           grpcServices_;
  private readonly HealthCheckService healthCheckService_;

  /// <summary>
  ///   Initializes a new instance of the <see cref="GrpcHealthCheckServiceBase" /> class.
  /// </summary>
  /// <param name="healthCheckService">The ASP.NET Core health check service to use for health status reporting.</param>
  /// <param name="grpcServices">An array of gRPC service names that this health check service supports.</param>
  protected GrpcHealthCheckServiceBase(HealthCheckService healthCheckService,
                                       string[]           grpcServices)
  {
    healthCheckService_ = healthCheckService;
    grpcServices_       = grpcServices;
  }

  /// <inheritdoc />
  public override async Task<HealthCheckResponse> Check(HealthCheckRequest request,
                                                        ServerCallContext  context)
  {
    if (string.IsNullOrEmpty(request.Service) || grpcServices_.Contains(request.Service))
    {
      var healthReport = await healthCheckService_.CheckHealthAsync(registration => registration.Tags.Contains(nameof(HealthCheckTag.Readiness)),
                                                                    context.CancellationToken)
                                                  .ConfigureAwait(false);

      if (healthReport.Status == HealthStatus.Healthy)
      {
        context.Status = Status.DefaultSuccess;
        return new HealthCheckResponse
               {
                 Status = HealthCheckResponse.Types.ServingStatus.Serving,
               };
      }

      context.Status = Status.DefaultSuccess;
      return new HealthCheckResponse
             {
               Status = HealthCheckResponse.Types.ServingStatus.NotServing,
             };
    }

    context.Status = Status.DefaultSuccess;
    return new HealthCheckResponse
           {
             Status = HealthCheckResponse.Types.ServingStatus.Unknown,
           };
  }
}
