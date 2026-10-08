// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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

using ArmoniK.Api.gRPC.V1.Auth;
using ArmoniK.Core.Authorization;
using ArmoniK.Core.Common.Enforcement;
using ArmoniK.Core.Common.Meter;

using Grpc.Core;

namespace ArmoniK.Core.Common.gRPC.Services;

/// <inheritdoc cref="Authentication" />
public class GrpcAuthService : Authentication.AuthenticationBase
{
  private readonly FunctionExecutionMetrics<GrpcAuthService> meter_;

  /// <summary>
  ///   Initializes a new instance of the <see cref="GrpcAuthService" /> class.
  /// </summary>
  /// <param name="meter">The metrics object for function execution.</param>
  public GrpcAuthService(FunctionExecutionMetrics<GrpcAuthService> meter)
    => meter_ = meter;

  /// <inheritdoc />
  /// <remarks>
  ///   When authorization is enabled, the caller, its roles followed by the permission sets it holds, and its
  ///   permissions written <c>permission@cluster</c> or <c>permission@condition</c>; nothing for a caller the cluster
  ///   holds no grant for. When authorization is disabled, an anonymous caller holding every permission.
  /// </remarks>
  public override Task<GetCurrentUserResponse> GetCurrentUser(GetCurrentUserRequest request,
                                                              ServerCallContext     context)
  {
    using var measure = meter_.CountAndTime();
    var       user    = new User();

    if (context.DescribeCaller() is var (caller, description))
    {
      if (description.Admitted)
      {
        user.Username = caller.Caller.Identity.Value;
        user.Roles.AddRange(caller.Caller.Roles.Select(role => role.Value)
                                  .Concat(description.PermissionSets));
        user.Permissions.AddRange(description.Permissions);
      }
    }
    else
    {
      user.Username = "Anonymous";
      user.Permissions.AddRange(Permission.All.Select(permission => permission.Name + "@cluster"));
    }

    return Task.FromResult(new GetCurrentUserResponse
                           {
                             User = user,
                           });
  }
}
