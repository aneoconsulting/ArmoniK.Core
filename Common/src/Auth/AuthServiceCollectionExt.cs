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

using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Auth.Authorization;

using JetBrains.Annotations;

using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace ArmoniK.Core.Common.Auth;

/// <summary>
///   Registration of the authentication and authorization services of the control plane
/// </summary>
public static class AuthServiceCollectionExt
{
  /// <summary>
  ///   Add the authentication service to the service collection
  /// </summary>
  /// <remarks>
  ///   The storage of the identities (<see cref="IAuthenticationTable" />) is registered separately, by the database
  ///   adapter.
  /// </remarks>
  /// <param name="services">Services</param>
  /// <param name="configuration">Configuration</param>
  /// <param name="authCache">Created authentication cache</param>
  /// <returns>Services</returns>
  [PublicAPI]
  public static IServiceCollection AddClientSubmitterAuthServices(this IServiceCollection services,
                                                                  IConfiguration          configuration,
                                                                  out AuthenticationCache authCache)
  {
    authCache = new AuthenticationCache();
    services.Configure<AuthenticatorOptions>(configuration.GetSection(AuthenticatorOptions.SectionName))
            .AddSingleton(authCache)
            .AddAuthentication()
            .AddScheme<AuthenticatorOptions, Authenticator>(Authenticator.SchemeName,
                                                            _ =>
                                                            {
                                                            });
    services.AddSingleton<IAuthorizationPolicyProvider, AuthorizationPolicyProvider>()
            .AddAuthorization();
    return services;
  }
}
