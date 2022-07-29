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

using System.Threading.Tasks;

using ArmoniK.Core.Common.Auth.Authentication;

using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Options;

namespace ArmoniK.Core.Common.Auth.Authorization
{
  public class AuthorizationPolicyProvider : IAuthorizationPolicyProvider
  {
    private readonly bool requireAuthentication_;
    private readonly bool requireAuthorization_;

    public AuthorizationPolicyProvider(IOptionsMonitor<AuthenticatorOptions> options)
    {
      requireAuthentication_ = options.CurrentValue.RequireAuthentication;
      requireAuthorization_  = options.CurrentValue.RequireAuthorization;
    }

    public Task<AuthorizationPolicy?> GetPolicyAsync(string policyName)
    {
      // If authentication is disabled, no check is required
      if (!requireAuthentication_)
        return GetAlwaysTruePolicyAsync()!;

      // If authorization is disabled, only check for an authenticated user
      if (!requireAuthorization_)
        return GetDefaultPolicyAsync()!;

      // If the policy name doesn't match ours, ignore
      if (!policyName.StartsWith(RequiresPermissionAttribute.PolicyPrefix))
        return GetFallbackPolicyAsync();

      // Require the authenticated user to have the right permission type
      var permission = Permissions.Parse(policyName[RequiresPermissionAttribute.PolicyPrefix.Length..]);
      return Task.FromResult<AuthorizationPolicy?>(new AuthorizationPolicyBuilder(Authenticator.SchemeName).RequireAuthenticatedUser()
                                                                                                           .RequireClaim(permission.ToBasePermission())
                                                                                                           .Build());
    }

    public static Task<AuthorizationPolicy> GetAlwaysTruePolicyAsync()
      => Task.FromResult(new AuthorizationPolicyBuilder(Authenticator.SchemeName).RequireAssertion(_ => true)
                                                                                 .Build());

    public Task<AuthorizationPolicy> GetDefaultPolicyAsync()
      => Task.FromResult(new AuthorizationPolicyBuilder(Authenticator.SchemeName).RequireAuthenticatedUser()
                                                                                 .Build());

    public Task<AuthorizationPolicy?> GetFallbackPolicyAsync()
      => Task.FromResult<AuthorizationPolicy?>(null);
  }
}
