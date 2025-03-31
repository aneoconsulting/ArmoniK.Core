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

using ArmoniK.Api.gRPC.V1.HealthChecks;
using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Auth.Authorization;
using ArmoniK.Core.Common.gRPC.Convertors;
using ArmoniK.Core.Common.Meter;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Utils;

using Grpc.Core;

using Microsoft.AspNetCore.Authorization;

namespace ArmoniK.Core.Common.gRPC.Services;

/// <inheritdoc cref="HealthChecksService" />
[Authorize(AuthenticationSchemes = Authenticator.SchemeName)]
public class GrpcHealthChecksService : HealthChecksService.HealthChecksServiceBase
{
  private readonly FunctionExecutionMetrics<GrpcHealthChecksService> meter_;
  private readonly IObjectStorage                                    objectStorage_;
  private readonly IPushQueueStorage                                 queueStorage_;
  private readonly ITaskTable                                        taskTable_;

  /// <summary>
  ///   Initializes a new instance of the <see cref="GrpcHealthChecksService" /> class.
  /// </summary>
  /// <param name="taskTable">The task table for checking database health.</param>
  /// <param name="objectStorage">The object storage for checking storage health.</param>
  /// <param name="queueStorage">The queue storage for checking queue health.</param>
  /// <param name="meter">The metrics for function execution.</param>
  public GrpcHealthChecksService(ITaskTable                                        taskTable,
                                 IObjectStorage                                    objectStorage,
                                 IPushQueueStorage                                 queueStorage,
                                 FunctionExecutionMetrics<GrpcHealthChecksService> meter)
  {
    taskTable_     = taskTable;
    objectStorage_ = objectStorage;
    queueStorage_  = queueStorage;
    meter_         = meter;
  }

  /// <summary>
  ///   Checks the health of various services.
  /// </summary>
  /// <param name="request">The health check request.</param>
  /// <param name="context">The server call context.</param>
  /// <returns>
  ///   A task representing the asynchronous operation, with a <see cref="CheckHealthResponse" /> result containing
  ///   the health status of services.
  /// </returns>
  [RequiresPermission(typeof(GrpcHealthChecksService),
                      nameof(CheckHealth))]
  public override async Task<CheckHealthResponse> CheckHealth(CheckHealthRequest request,
                                                              ServerCallContext  context)
  {
    using var measure = meter_.CountAndTime();
    var checks = await new[]
                       {
                         ("database", taskTable_.Check(HealthCheckTag.Liveness)),
                         ("object", objectStorage_.Check(HealthCheckTag.Liveness)),
                         ("queue", queueStorage_.Check(HealthCheckTag.Liveness)),
                       }.Select(async service => (Name: service.Item1, Check: await service.Item2.ConfigureAwait(false)))
                        .WhenAll()
                        .ConfigureAwait(false);

    return new CheckHealthResponse
           {
             Services =
             {
               checks.Select(pair => new CheckHealthResponse.Types.ServiceHealth
                                     {
                                       Name    = pair.Name,
                                       Healthy = pair.Check.Status.ToGrpcHealthStatusEnum(),
                                       Message = pair.Check.Description ?? string.Empty,
                                     }),
             },
           };
  }
}
