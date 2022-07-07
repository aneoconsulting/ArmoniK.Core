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

using System;
using System.Threading.Tasks;

using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;

namespace ArmoniK.Core.Common.Auth.Authorization
{
  public class AuthorizationPolicyProvider : IAuthorizationPolicyProvider
  {

    private readonly AuthenticationScheme authScheme_;

    public AuthorizationPolicyProvider(AuthenticationScheme scheme)
    {
      authScheme_ = scheme;
    }

    public Task<AuthorizationPolicy?> GetPolicyAsync(string policyName)
    {
      if (!policyName.StartsWith(RequiresActionAttribute.PolicyPrefix))
      {
        return Task.FromResult<AuthorizationPolicy?>(null);
      }

      Actions.Parse(policyName[RequiresActionAttribute.PolicyPrefix.Length..],
                    out var prefix,
                    out var action,
                    out _);
      return Task.FromResult<AuthorizationPolicy?>(
                                                   new AuthorizationPolicyBuilder(authScheme_.Name)
                                                     .AddRequirements(new HasActionAccessRequirement(Actions.Get(prefix, action)))
                                                     .Build());

    }

    public Task<AuthorizationPolicy> GetDefaultPolicyAsync()
      => Task.FromResult(new AuthorizationPolicyBuilder(authScheme_.Name).RequireAuthenticatedUser()
                                                                         .Build());

    public Task<AuthorizationPolicy?> GetFallbackPolicyAsync()
      => Task.FromResult<AuthorizationPolicy?>(null);
  }
}
