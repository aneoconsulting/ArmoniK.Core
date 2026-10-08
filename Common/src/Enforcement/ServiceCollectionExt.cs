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

using System;
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Core.Authorization;

using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Utils;

using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.IdentityModel.Tokens;

namespace ArmoniK.Core.Common.Enforcement;

/// <summary>
///   Registration of the authorization of requests
/// </summary>
public static class ServiceCollectionExt
{
  /// <summary>
  ///   Register the authorization of requests when it is enabled
  /// </summary>
  /// <param name="services">Collection of services</param>
  /// <param name="configuration">Configuration of the application</param>
  /// <returns>Whether authorization is enabled, in which case <see cref="AuthorizationInterceptor" /> must be added to gRPC</returns>
  /// <exception cref="InvalidOperationException">The configuration is invalid</exception>
  public static bool AddArmoniKAuthorization(this IServiceCollection services,
                                             IConfiguration          configuration)
  {
    var options = configuration.GetInitializedValue<AuthorizationOptions>(AuthorizationOptions.SettingSection);

    if (!options.Enabled)
    {
      return false;
    }

    var legacy = configuration.GetSection(AuthenticatorOptions.SectionName)
                              .Get<AuthenticatorOptions>() ?? AuthenticatorOptions.Default;
    if (legacy.RequireAuthentication || legacy.RequireAuthorization)
    {
      throw new InvalidOperationException($"{AuthorizationOptions.SettingSection}:{nameof(AuthorizationOptions.Enabled)} cannot be combined with " +
                                          $"{AuthenticatorOptions.SectionName}:{nameof(AuthenticatorOptions.RequireAuthentication)} or "          +
                                          $"{nameof(AuthenticatorOptions.RequireAuthorization)}: disable the latter");
    }

    var identity = configuration.GetInitializedValue<IdentityOptions>(IdentityOptions.SettingSection);

    if (string.IsNullOrWhiteSpace(identity.CertificateHeader))
    {
      throw new InvalidOperationException($"{IdentityOptions.SettingSection}:{nameof(IdentityOptions.CertificateHeader)} cannot be empty");
    }

    // Refuse to start on a configuration whose grants would be refused
    var sets = AuthorizationConfiguration.PermissionSets(options.PermissionSets);
    DecisionEngine.Build(AuthorizationConfiguration.Grants(options.Grants),
                         sets,
                         out var rejected);
    if (rejected.Count != 0)
    {
      throw new InvalidOperationException($"The configuration holds grants that are refused: {string.Join("; ", rejected.Select(r => r.Error.Message))}");
    }

    services.TryAddSingleton(TimeProvider.System);
    services.AddSingleton(options)
            .AddSingleton(identity)
            .AddSingleton<IDecisionEngineProvider, StoredDecisionEngineProvider>()
            .AddSingleton<AuthorizationReconciler>()
            .AddSingleton<CallerResolver>()
            .AddSingleton<AuthorizationAudit>()
            .AddSingleton<AuthorizationEnforcer>()
            .AddSingleton<AuthorizationInterceptor>();

    if (!string.IsNullOrWhiteSpace(identity.Authority))
    {
      AddTokenValidation(services,
                         identity);
    }

    return true;
  }

  private static void AddTokenValidation(IServiceCollection services,
                                         IdentityOptions    identity)
  {
    if (string.IsNullOrWhiteSpace(identity.Audience))
    {
      throw new InvalidOperationException($"{IdentityOptions.SettingSection}:{nameof(IdentityOptions.Audience)} is required with an authority: " +
                                          "without it, a token issued for another application of the same provider is accepted");
    }

    if (!Uri.TryCreate(identity.Authority,
                       UriKind.Absolute,
                       out var authority) || (identity.RequireHttpsMetadata && authority.Scheme != Uri.UriSchemeHttps))
    {
      throw new InvalidOperationException($"{IdentityOptions.SettingSection}:{nameof(IdentityOptions.Authority)} '{identity.Authority}' must be an " +
                                          $"absolute URI, using HTTPS while {nameof(IdentityOptions.RequireHttpsMetadata)} is set");
    }

    if (string.IsNullOrWhiteSpace(identity.SubjectClaim) || string.IsNullOrWhiteSpace(identity.RolesClaim))
    {
      throw new InvalidOperationException($"{IdentityOptions.SettingSection}:{nameof(IdentityOptions.SubjectClaim)} and " +
                                          $"{nameof(IdentityOptions.RolesClaim)} cannot be empty");
    }

    services.AddAuthentication()
            .AddJwtBearer(jwt =>
                          {
                            jwt.Authority            = identity.Authority;
                            jwt.Audience             = identity.Audience;
                            jwt.RequireHttpsMetadata = identity.RequireHttpsMetadata;
                            jwt.MapInboundClaims     = false;
                            jwt.TokenValidationParameters = new TokenValidationParameters
                                                            {
                                                              ValidateIssuer           = true,
                                                              ValidateAudience         = true,
                                                              ValidAudience            = identity.Audience,
                                                              ValidateIssuerSigningKey = true,
                                                              ValidateLifetime         = true,
                                                            };

                            if (!string.IsNullOrWhiteSpace(identity.TenantId))
                            {
                              jwt.Events = new JwtBearerEvents
                                           {
                                             OnTokenValidated = context =>
                                                                {
                                                                  var tenant = context.Principal?.FindFirst("tid")
                                                                                      ?.Value;
                                                                  if (!string.Equals(tenant,
                                                                                     identity.TenantId,
                                                                                     StringComparison.Ordinal))
                                                                  {
                                                                    context.Fail($"Token tenant '{tenant}' is not the configured tenant");
                                                                  }

                                                                  return Task.CompletedTask;
                                                                },
                                           };
                            }
                          });
  }
}
