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
using ArmoniK.Core.Common.Auth.Authorization.Permissions;

using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Options;

namespace ArmoniK.Core.Common.Auth.Authorization;

/// <summary>
///   Class used by the authorization middleware to generate the policy from its name and configuration
/// </summary>
public class AuthorizationPolicyProvider : IAuthorizationPolicyProvider
{
  private readonly bool requireAuthentication_;
  private readonly bool requireAuthorization_;

  /// <summary>
  ///   Creates the authorization policy provider from the options
  /// </summary>
  /// <param name="options">Options</param>
  public AuthorizationPolicyProvider(IOptionsMonitor<AuthenticatorOptions> options)
  {
    requireAuthentication_ = options.CurrentValue.RequireAuthentication;
    requireAuthorization_  = options.CurrentValue.RequireAuthorization;
  }

  /// <summary>
  ///   Get the policy associated with the given name
  /// </summary>
  /// <param name="policyName">Name of the policy</param>
  /// <returns>Authorization policy</returns>
  public Task<AuthorizationPolicy?> GetPolicyAsync(string policyName)
  {
    // If authentication is disabled, no check is required
    if (!requireAuthentication_)
    {
      return GetAlwaysTruePolicyAsync()!;
    }

    // If authorization is disabled, only check for an authenticated user
    if (!requireAuthorization_)
    {
      return GetDefaultPolicyAsync()!;
    }

    // If the policy name doesn't match ours, ignore
    if (!policyName.StartsWith(RequiresPermissionAttribute.PolicyPrefix))
    {
      return GetFallbackPolicyAsync();
    }

    // Require the authenticated user to have the right permission type
    var permission = new Permission(policyName[RequiresPermissionAttribute.PolicyPrefix.Length..]);
    return Task.FromResult<AuthorizationPolicy?>(new AuthorizationPolicyBuilder(Authenticator.SchemeName).RequireAuthenticatedUser()
                                                                                                         .RequireClaim(permission.ToBasePermission())
                                                                                                         .Build());
  }

  /// <summary>
  ///   Get an authorization policy where the user needs to be authenticated
  /// </summary>
  /// <returns>Default authorization policy</returns>
  public Task<AuthorizationPolicy> GetDefaultPolicyAsync()
    => Task.FromResult(new AuthorizationPolicyBuilder(Authenticator.SchemeName).RequireAuthenticatedUser()
                                                                               .Build());

  /// <summary>
  ///   Get a null authorization policy, falling back to other policies
  /// </summary>
  /// <returns>Null policy (fallback)</returns>
  public Task<AuthorizationPolicy?> GetFallbackPolicyAsync()
    => Task.FromResult<AuthorizationPolicy?>(null);

  /// <summary>
  ///   Get an authorization policy accepting anything
  /// </summary>
  /// <returns>Policy returning true all the time</returns>
  public static Task<AuthorizationPolicy> GetAlwaysTruePolicyAsync()
    => Task.FromResult(new AuthorizationPolicyBuilder(Authenticator.SchemeName).RequireAssertion(_ => true)
                                                                               .Build());
}
