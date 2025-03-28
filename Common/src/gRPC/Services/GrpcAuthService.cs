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

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Auth;
using ArmoniK.Core.Base.Exceptions;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Auth.Authorization;
using ArmoniK.Core.Common.Auth.Authorization.Permissions;
using ArmoniK.Core.Common.Meter;

using Grpc.Core;

using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Options;

namespace ArmoniK.Core.Common.gRPC.Services;

/// <summary>
///   The gRPC service that provides methods for user authentication and authorization.
/// </summary>
[Authorize(AuthenticationSchemes = Authenticator.SchemeName)]
public class GrpcAuthService : Authentication.AuthenticationBase
{
  private readonly FunctionExecutionMetrics<GrpcAuthService> meter_;
  private readonly bool                                      requireAuthentication_;
  private readonly bool                                      requireAuthorization_;

  /// <summary>
  ///   Initializes a new instance of the <see cref="GrpcAuthService" /> class.
  /// </summary>
  /// <param name="options">The options monitor for authenticator configuration.</param>
  /// <param name="meter">The metrics object for function execution.</param>
  public GrpcAuthService(IOptionsMonitor<AuthenticatorOptions>     options,
                         FunctionExecutionMetrics<GrpcAuthService> meter)
  {
    requireAuthentication_ = options.CurrentValue.RequireAuthentication;
    requireAuthorization_  = requireAuthentication_ && options.CurrentValue.RequireAuthorization;
    meter_                 = meter;
  }

  /// <inheritdoc />
  [IgnoreAuthorization]
  public override Task<GetCurrentUserResponse> GetCurrentUser(GetCurrentUserRequest request,
                                                              ServerCallContext     context)
  {
    using var measure = meter_.CountAndTime();
    var       user    = new User();

    // No authentication required, anonymous user with all permissions
    if (!requireAuthentication_)
    {
      user.Username = "Anonymous";
      user.Permissions.AddRange(ServicesPermissions.PermissionsLists[ServicesPermissions.All]
                                                   .Select(p => p.ToString()));
      return Task.FromResult(new GetCurrentUserResponse
                             {
                               User = user,
                             });
    }


    var principal = context.GetHttpContext()
                           .User;
    if (principal.Identity is not ({
                                     IsAuthenticated   : true,
                                     AuthenticationType: Authenticator.SchemeName,
                                   } and UserIdentity userIdentity))
    {
      // Authenticated, but not with the required scheme or not authenticated or the identity is not of the right type... something's wrong
      throw new ArmoniKException("Failed to get the authenticated user's info, something's wrong");
    }

    // Authentication is required, the user should have been authenticated to access this endpoint so we should have all the necessary information
    user.Username = userIdentity.UserName;
    // Do not return roles if authorization is not required
    user.Roles.AddRange(requireAuthorization_
                          ? userIdentity.Roles
                          : Enumerable.Empty<string>());
    // Return the user's permissions if authorization is required, otherwise they have all permissions
    user.Permissions.AddRange(((IEnumerable<Permission>)(requireAuthorization_
                                                           ? userIdentity.Permissions
                                                           : ServicesPermissions.PermissionsLists[ServicesPermissions.All])).Select(p => p.ToString()));
    return Task.FromResult(new GetCurrentUserResponse
                           {
                             User = user,
                           });
  }
}
