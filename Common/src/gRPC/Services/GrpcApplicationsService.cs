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
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Applications;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Auth.Authorization;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using Grpc.Core;

using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.gRPC.Services;

[Authorize(AuthenticationSchemes = Authenticator.SchemeName)]
public class GrpcApplicationsService : Applications.ApplicationsBase
{
  private readonly ILogger<GrpcApplicationsService> logger_;
  private readonly ITaskTable                       taskTable_;

  public GrpcApplicationsService(ITaskTable                       taskTable,
                                 ILogger<GrpcApplicationsService> logger)
  {
    logger_    = logger;
    taskTable_ = taskTable;
  }

  [RequiresPermission(typeof(GrpcApplicationsService),
                      nameof(ListApplications))]
  public override async Task<ListApplicationsResponse> ListApplications(ListApplicationsRequest request,
                                                                        ServerCallContext       context)
  {
    var tasks = await taskTable_.ListApplicationsAsync(request.Filter.ToApplicationFilter(),
                                                       request.Sort.Fields.Select(field => field.ToApplicationField())
                                                              .ToList(),
                                                       request.Sort.Direction == ListApplicationsRequest.Types.OrderDirection.Asc,
                                                       request.Page,
                                                       request.PageSize,
                                                       context.CancellationToken)
                                .ConfigureAwait(false);
    return new ListApplicationsResponse
           {
             Page     = request.Page,
             PageSize = request.PageSize,
             Applications =
             {
               tasks.applications.Select(data => new ApplicationRaw
                                                 {
                                                   Name      = data.Name,
                                                   Namespace = data.Namespace,
                                                   Version   = data.Version,
                                                   Service   = data.Service,
                                                 }),
             },
             Total = tasks.totalCount,
           };
  }

  [RequiresPermission(typeof(GrpcApplicationsService),
                      nameof(CountTasksByStatus))]
  public override async Task<CountTasksByStatusResponse> CountTasksByStatus(CountTasksByStatusRequest request,
                                                                            ServerCallContext         context)
  {
    try
    {
      return new CountTasksByStatusResponse
             {
               Status =
               {
                 (await taskTable_.CountTasksAsync(data => data.Options.ApplicationName == request.Name && data.Options.ApplicationVersion == request.Version,
                                                   context.CancellationToken)
                                  .ConfigureAwait(false)).Select(count => new StatusCount
                                                                          {
                                                                            Status = count.Status,
                                                                            Count  = count.Count,
                                                                          }),
               },
             };
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                         "Error while counting tasks by applications");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal ArmoniK Exception, see application logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                         "Error while counting tasks by applications");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see application logs"));
    }
  }
}
