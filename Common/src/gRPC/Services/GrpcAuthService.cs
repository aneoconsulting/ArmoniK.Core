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
//   D. Brasseur       <dbrasseur@aneo.fr>
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

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Auth;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Auth.Authorization.Permissions;
using ArmoniK.Core.Common.Exceptions;

using Grpc.Core;

using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ArmoniK.Core.Common.gRPC.Services;

[Authorize(AuthenticationSchemes = Authenticator.SchemeName)]
public class GrpcAuthService : Authentication.AuthenticationBase
{
  private readonly ILogger<GrpcAuthService> logger_;
  private readonly IAuthenticationTable     authTable_;
  private readonly bool                     requireAuthentication_;
  private readonly bool                     requireAuthorization_;

  public GrpcAuthService(IOptionsMonitor<AuthenticatorOptions> options,
                         IAuthenticationTable                  authTable,
                         ILogger<GrpcAuthService>              logger)
  {
    authTable_             = authTable;
    logger_                = logger;
    requireAuthentication_ = options.CurrentValue.RequireAuthentication;
    requireAuthorization_  = requireAuthentication_ && options.CurrentValue.RequireAuthorization;
  }

  public override Task<GetCurrentUserResponse> GetCurrentUser(GetCurrentUserRequest request,
                                                              ServerCallContext     context)
  {
    // No authentication required, anonymous user with all permissions
    if (!requireAuthentication_)
    {
      var user = new User
                 {
                   Username = "Anonymous",
                 };
      user.Permissions.AddRange(ServicesPermissions.PermissionsLists[ServicesPermissions.All]
                                                   .Select(p => p.ToString()));
      return Task.FromResult(new GetCurrentUserResponse
                             {
                               User = user,
                             });
    }

    // Authentication is required, the user should have been authenticated to access this endpoint so we should have all the necessary information
    var principal = context.GetHttpContext()
                           .User;
    if (principal.Identity is
        {
          IsAuthenticated: true,
          AuthenticationType: Authenticator.SchemeName,
        } && principal is UserIdentity userIdentity)
    {
      var user = new User
                 {
                   Username = userIdentity.UserName,
                 };
      // Do not return roles if authorization is not required
      user.Roles.AddRange(requireAuthorization_ ? userIdentity.Roles : Enumerable.Empty<string>());
      // Return the user's permissions if authorization is required, otherwise they have all permissions
      user.Permissions.AddRange(((IEnumerable<Permission>)(requireAuthorization_
                                                             ? userIdentity.Permissions
                                                             : ServicesPermissions.PermissionsLists[ServicesPermissions.All])).Select(p => p.ToString()));
      return Task.FromResult(new GetCurrentUserResponse
                             {
                               User = user,
                             });
    }

    // Authenticated, but not with the required scheme or not authenticated or the principal is not of the right type... something's wrong
    throw new ArmoniKException("Failed to get the authenticated user's info, something's wrong");

  }
    
}
