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
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Utils;

using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Tokens;

namespace ArmoniK.Core.Common.Enforcement;

/// <summary>
///   Registration of the authorization of requests
/// </summary>
public static class ServiceCollectionExt
{
  /// <summary>
  ///   Every method a caller can authenticate with
  /// </summary>
  private const CallerAuthentication AllMethods = CallerAuthentication.Xfcc | CallerAuthentication.Token;

  private static readonly string AuthenticationKey = $"{AuthorizationOptions.SettingSection}:{nameof(AuthorizationOptions.Authentication)}";

  /// <summary>
  ///   Settings of <see cref="IdentityOptions" /> read only with <see cref="CallerAuthentication.Token" />
  /// </summary>
  private static readonly string[] TokenSettings =
  [
    nameof(IdentityOptions.Authority),
    nameof(IdentityOptions.Audience),
    nameof(IdentityOptions.TenantId),
    nameof(IdentityOptions.SubjectClaim),
    nameof(IdentityOptions.RolesClaim),
    nameof(IdentityOptions.RequireHttpsMetadata),
    nameof(IdentityOptions.MetadataRefreshInterval),
    nameof(IdentityOptions.ValidateTokens),
  ];

  /// <summary>
  ///   Settings of <see cref="IdentityOptions" /> read only with <see cref="CallerAuthentication.Xfcc" />
  /// </summary>
  private static readonly string[] CertificateSettings =
  [
    nameof(IdentityOptions.CertificateHeader),
  ];

  /// <summary>
  ///   Register the authorization of requests when a method of authentication is selected
  /// </summary>
  /// <param name="services">Collection of services</param>
  /// <param name="configuration">Configuration of the application</param>
  /// <param name="logger">Logger warning about the settings of methods that are not selected</param>
  /// <returns>Whether authorization is enabled, in which case <see cref="AuthorizationInterceptor" /> must be added to gRPC</returns>
  /// <exception cref="InvalidOperationException">The configuration is invalid</exception>
  public static bool AddArmoniKAuthorization(this IServiceCollection services,
                                             IConfiguration          configuration,
                                             ILogger                 logger)
  {
    ThrowIfLegacyConfiguration(configuration);

    var options = configuration.GetInitializedValue<AuthorizationOptions>(AuthorizationOptions.SettingSection);

    // The binder takes any number, such as 4 or -1, as a value of the enumeration
    if ((options.Authentication & ~AllMethods) != 0)
    {
      throw new InvalidOperationException($"{AuthenticationKey} '{configuration[AuthenticationKey]}' holds an unknown method: " +
                                          $"use {CallerAuthentication.None}, {CallerAuthentication.Xfcc}, {CallerAuthentication.Token} " +
                                          $"or {CallerAuthentication.Xfcc},{CallerAuthentication.Token}");
    }

    // Only warned about, so that authorization can be disabled without removing the settings of the methods
    WarnUnusedSettings(configuration,
                       options.Authentication,
                       logger);

    if (options.Authentication == CallerAuthentication.None)
    {
      return false;
    }

    // Past the maximum, twice the delay a revocation waits for is no longer a delay; at zero or below, nothing is cached
    if (options.GrantCacheTtl <= TimeSpan.Zero || options.GrantCacheTtl > AuthorizationOptions.MaxGrantCacheTtl)
    {
      throw new InvalidOperationException($"{AuthorizationOptions.SettingSection}:{nameof(AuthorizationOptions.GrantCacheTtl)} " +
                                          $"'{options.GrantCacheTtl}' must be greater than zero and at most {AuthorizationOptions.MaxGrantCacheTtl}");
    }

    var identity = configuration.GetInitializedValue<IdentityOptions>(IdentityOptions.SettingSection);

    if (options.Authentication.HasFlag(CallerAuthentication.Xfcc) && string.IsNullOrWhiteSpace(identity.CertificateHeader))
    {
      throw new InvalidOperationException($"{IdentityOptions.SettingSection}:{nameof(IdentityOptions.CertificateHeader)} cannot be empty " +
                                          $"when {AuthenticationKey} holds {CallerAuthentication.Xfcc}");
    }

    if (options.Authentication.HasFlag(CallerAuthentication.Token) && identity.ValidateTokens && string.IsNullOrWhiteSpace(identity.Authority))
    {
      throw new InvalidOperationException($"{IdentityOptions.SettingSection}:{nameof(IdentityOptions.Authority)} is required when " +
                                          $"{AuthenticationKey} holds {CallerAuthentication.Token}, unless {nameof(IdentityOptions.ValidateTokens)} " +
                                          "is false and the tokens are written by a trusted proxy");
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
            .AddSingleton<AuthorizationAudit>()
            .AddSingleton<AuthorizationEnforcer>()
            .AddSingleton<AuthorizationInterceptor>();
    services.AddSingletonWithHealthCheck<CallerResolver>(nameof(CallerResolver));

    if (!options.Authentication.HasFlag(CallerAuthentication.Token))
    {
      return true;
    }

    if (identity.ValidateTokens)
    {
      AddTokenValidation(services,
                         identity);
    }
    else
    {
      AddUnvalidatedTokens(services,
                           identity);
    }

    return true;
  }

  /// <summary>
  ///   Warn about the identity settings written for a method that is not selected, which are ignored
  /// </summary>
  private static void WarnUnusedSettings(IConfiguration       configuration,
                                         CallerAuthentication authentication,
                                         ILogger              logger)
  {
    foreach (var (method, settings) in new[]
                                       {
                                         (CallerAuthentication.Token, TokenSettings),
                                         (CallerAuthentication.Xfcc, CertificateSettings),
                                       })
    {
      if (authentication.HasFlag(method))
      {
        continue;
      }

      foreach (var setting in settings.Select(setting => $"{IdentityOptions.SettingSection}:{setting}")
                                      .Where(key => configuration[key] is not null))
      {
        logger.LogWarning("{Setting} is ignored: {Method} is not selected in {AuthenticationSetting} ({Authentication})",
                          setting,
                          method,
                          AuthenticationKey,
                          authentication);
      }
    }
  }

  /// <summary>
  ///   Refuse to start on a configuration of the removed authentication without an explicit choice about authorization,
  ///   since removing the former flags alone would leave every request allowed, and on the removed
  ///   <c>Authorization:Enabled</c>
  /// </summary>
  /// <param name="configuration">Configuration of the application</param>
  /// <exception cref="InvalidOperationException">The configuration of the removed authentication is present</exception>
  public static void ThrowIfLegacyConfiguration(IConfiguration configuration)
  {
    // Ignoring it would allow every request of a deployment that enabled authorization
    if (configuration[$"{AuthorizationOptions.SettingSection}:Enabled"] is not null)
    {
      throw new InvalidOperationException($"{AuthorizationOptions.SettingSection}:Enabled is replaced by {AuthenticationKey}, the methods callers " +
                                          $"authenticate with: {CallerAuthentication.Xfcc} for client certificates forwarded by the ingress, "     +
                                          $"{CallerAuthentication.Token} for tokens, {CallerAuthentication.Xfcc},{CallerAuthentication.Token} for " +
                                          $"both, or {CallerAuthentication.None} to allow every request. Remove {AuthorizationOptions.SettingSection}:Enabled");
    }

    var legacy = new[]
      {
        "Authenticator",
        "InitServices:Authentication",
        "Components:AuthenticationStorage",
      }.Where(key => configuration.GetSection(key)
                                  .Exists())
       .ToList();

    if (legacy.Count != 0 && configuration[AuthenticationKey] is null)
    {
      throw new InvalidOperationException($"The configuration holds settings of the removed authentication ({string.Join(", ", legacy)}). Set " +
                                          $"{AuthenticationKey} explicitly, to {CallerAuthentication.Xfcc}, {CallerAuthentication.Token} or "      +
                                          $"{CallerAuthentication.Xfcc},{CallerAuthentication.Token} to authorize requests with grants, or to "    +
                                          $"{CallerAuthentication.None} to allow every request, and remove these settings");
    }
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

    // JwtBearer only checks the interval when it first reads the metadata, which would refuse every token instead of starting
    if (identity.MetadataRefreshInterval < BaseConfigurationManager.MinimumRefreshInterval)
    {
      throw new InvalidOperationException($"{IdentityOptions.SettingSection}:{nameof(IdentityOptions.MetadataRefreshInterval)} " +
                                          $"'{identity.MetadataRefreshInterval}' must be at least {BaseConfigurationManager.MinimumRefreshInterval}");
    }

    ThrowIfNoClaims(identity);

    services.AddAuthentication()
            .AddJwtBearer(jwt =>
                          {
                            jwt.Authority            = identity.Authority;
                            jwt.Audience             = identity.Audience;
                            jwt.RequireHttpsMetadata = identity.RequireHttpsMetadata;
                            jwt.RefreshInterval      = identity.MetadataRefreshInterval;
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

  private static void AddUnvalidatedTokens(IServiceCollection services,
                                           IdentityOptions    identity)
  {
    if (!string.IsNullOrWhiteSpace(identity.Authority))
    {
      throw new InvalidOperationException($"{IdentityOptions.SettingSection}:{nameof(IdentityOptions.Authority)} is set while " +
                                          $"{nameof(IdentityOptions.ValidateTokens)} is false: tokens would not be validated against it");
    }

    ThrowIfNoClaims(identity);

    // The proxy writing the tokens is trusted with their claims: neither signature, issuer, audience nor lifetime is checked
    services.AddAuthentication()
            .AddJwtBearer(jwt =>
                          {
                            jwt.MapInboundClaims = false;
                            jwt.TokenValidationParameters = new TokenValidationParameters
                                                            {
                                                              ValidateIssuer           = false,
                                                              ValidateAudience         = false,
                                                              ValidateIssuerSigningKey = false,
                                                              ValidateLifetime         = false,
                                                              RequireExpirationTime    = false,
                                                              RequireSignedTokens      = false,
                                                              SignatureValidator = (token,
                                                                                    _) => new JsonWebToken(token),
                                                            };
                          });
  }

  private static void ThrowIfNoClaims(IdentityOptions identity)
  {
    if (string.IsNullOrWhiteSpace(identity.SubjectClaim) || string.IsNullOrWhiteSpace(identity.RolesClaim))
    {
      throw new InvalidOperationException($"{IdentityOptions.SettingSection}:{nameof(IdentityOptions.SubjectClaim)} and " +
                                          $"{nameof(IdentityOptions.RolesClaim)} cannot be empty");
    }
  }
}
