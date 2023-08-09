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
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Applications;

using Armonik.Api.Grpc.V1.SortDirection;

using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Auth.Authorization;
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
    var (applications, totalCount) = await taskTable_.ListApplicationsAsync(request.Filters is null
                                                         ? data => true
                                                         : request.Filters.ToApplicationFilter(),
                                                       request.Sort is null
                                                         ? new List<Expression<Func<Application, object?>>>
                                                           {
                                                             application => application.Name,
                                                           }
                                                         : request.Sort.Fields.Select(field => field.ToField())
                                                                  .ToList(),
                                                       request.Sort is null || request.Sort.Direction == SortDirection.Asc,
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
               applications.Select(data => new ApplicationRaw
                                                 {
                                                   Name      = data.Name,
                                                   Namespace = data.Namespace,
                                                   Version   = data.Version,
                                                   Service   = data.Service,
                                                 }),
             },
             Total = totalCount,
           };
  }
}
