// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System.Linq;
using System.Threading.Tasks;

using Grpc.Core;
using Grpc.Health.V1;

using JetBrains.Annotations;

using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ArmoniK.Core.Common.gRPC;

[PublicAPI]
public abstract class GrpcHealthCheckServiceBase : Health.HealthBase
{
  private readonly string[]           grpcServices_;
  private readonly HealthCheckService healthCheckService_;

  protected GrpcHealthCheckServiceBase(HealthCheckService healthCheckService,
                                       string[]           grpcServices)
  {
    healthCheckService_ = healthCheckService;
    grpcServices_       = grpcServices;
  }

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
