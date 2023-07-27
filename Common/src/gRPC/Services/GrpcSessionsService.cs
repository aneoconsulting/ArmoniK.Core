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

using ArmoniK.Api.gRPC.V1.Sessions;

using Armonik.Api.Grpc.V1.SortDirection;

using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Auth.Authorization;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using Grpc.Core;

using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.gRPC.Services;

[Authorize(AuthenticationSchemes = Authenticator.SchemeName)]
public class GrpcSessionsService : Sessions.SessionsBase
{
  private readonly ILogger<GrpcSessionsService> logger_;
  private readonly ISessionTable                sessionTable_;

  public GrpcSessionsService(ISessionTable                sessionTable,
                             ILogger<GrpcSessionsService> logger)
  {
    logger_       = logger;
    sessionTable_ = sessionTable;
  }

  [RequiresPermission(typeof(GrpcSessionsService),
                      nameof(CancelSession))]
  public override async Task<CancelSessionResponse> CancelSession(CancelSessionRequest request,
                                                                  ServerCallContext    context)
  {
    try
    {
      return new CancelSessionResponse
             {
               Session = await sessionTable_.CancelSessionAsync(request.SessionId,
                                                                context.CancellationToken)
                                            .ConfigureAwait(false),
             };
    }
    catch (SessionNotFoundException e)
    {
      logger_.LogWarning(e,
                         "Error while cancelling session");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Session not found"));
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                         "Error while cancelling session");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see application logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                         "Error while cancelling session");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see application logs"));
    }
  }

  [RequiresPermission(typeof(GrpcSessionsService),
                      nameof(GetSession))]
  public override async Task<GetSessionResponse> GetSession(GetSessionRequest request,
                                                            ServerCallContext context)
  {
    try
    {
      return new GetSessionResponse
             {
               Session = await sessionTable_.GetSessionAsync(request.SessionId,
                                                             context.CancellationToken)
                                            .ConfigureAwait(false),
             };
    }
    catch (SessionNotFoundException e)
    {
      logger_.LogWarning(e,
                         "Error while getting session");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Session not found"));
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                         "Error while getting session");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see application logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                         "Error while getting session");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see application logs"));
    }
  }

  [RequiresPermission(typeof(GrpcSessionsService),
                      nameof(ListSessions))]
  public override async Task<ListSessionsResponse> ListSessions(ListSessionsRequest request,
                                                                ServerCallContext   context)
  {
    try
    {
      var sessionData = await sessionTable_.ListSessionsAsync(request.Filters is null
                                                                ? data => true
                                                                : request.Filters.ToSessionDataFilter(),
                                                              request.Sort is null
                                                                ? data => data.SessionId
                                                                : request.Sort.ToField(),
                                                              request.Sort is null || request.Sort.Direction == SortDirection.Asc,
                                                              request.Page,
                                                              request.PageSize,
                                                              context.CancellationToken)
                                           .ConfigureAwait(false);

      return new ListSessionsResponse
             {
               Page     = request.Page,
               PageSize = request.PageSize,
               Sessions =
               {
                 sessionData.sessions.Select(data => new SessionRaw(data)),
               },
               Total = (int)sessionData.totalCount,
             };
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                         "Error while listing sessions");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see application logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                         "Error while listing sessions");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see application logs"));
    }
  }
}
