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
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Authorization;
using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Injection.Options;

using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Protocols;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;

namespace ArmoniK.Core.Common.Enforcement;

/// <summary>
///   Derives the caller of a request from the credential presented: a token or a client certificate forwarded by the
///   ingress, never both
/// </summary>
public class CallerResolver : IHealthCheckProvider
{
  private const string BearerPrefix = "Bearer ";

  private readonly IOptionsMonitor<JwtBearerOptions>? jwt_;
  private readonly IdentityOptions                    options_;

  /// <summary>
  ///   Build the resolver
  /// </summary>
  /// <param name="options">Options of the identification of callers</param>
  public CallerResolver(IdentityOptions                    options,
                        IOptionsMonitor<JwtBearerOptions>? jwt = null)
  {
    options_ = options;
    jwt_     = jwt;
  }

  /// <summary>
  ///   The metadata of the authority tokens are validated against, null when tokens are not validated against one
  /// </summary>
  private IConfigurationManager<OpenIdConnectConfiguration>? Metadata
    => options_.ValidateTokens && !string.IsNullOrWhiteSpace(options_.Authority)
         ? jwt_?.Get(JwtBearerDefaults.AuthenticationScheme)
               .ConfigurationManager
         : null;

  /// <inheritdoc />
  /// <remarks>
  ///   Ready once the metadata of the authority was read: before, no token can be validated. Started and alive while
  ///   the authority cannot be reached, since a restart would not reach it either.
  /// </remarks>
  public async Task<HealthCheckResult> Check(HealthCheckTag tag)
  {
    if (tag != HealthCheckTag.Readiness || Metadata is not { } metadata)
    {
      return HealthCheckResult.Healthy();
    }

    try
    {
      await metadata.GetConfigurationAsync(CancellationToken.None)
                    .ConfigureAwait(false);
      return HealthCheckResult.Healthy();
    }
    catch (Exception e)
    {
      return HealthCheckResult.Unhealthy("The metadata of the token authority could not be read",
                                         e);
    }
  }

  /// <summary>
  ///   Whether tokens are accepted: validated against an authority, or taken as they are
  /// </summary>
  public bool AcceptsTokens
    => !options_.ValidateTokens || !string.IsNullOrWhiteSpace(options_.Authority);

  /// <summary>
  ///   Derive the caller of a request
  /// </summary>
  /// <param name="context">The request</param>
  /// <returns>The caller, null when the request carries no credential</returns>
  /// <exception cref="FormatException">The credential is invalid, or a token and a certificate were both presented</exception>
  public async Task<AuthenticatedCaller?> ResolveAsync(HttpContext context)
  {
    var authorization = context.Request.Headers.Authorization.ToString();
    var hasToken      = authorization.StartsWith(BearerPrefix,
                                                 StringComparison.OrdinalIgnoreCase);
    var certificate = context.Request.Headers[options_.CertificateHeader]
                             .ToString();
    var hasCertificate = !string.IsNullOrEmpty(certificate);

    if (hasToken && hasCertificate)
    {
      throw new FormatException("A request presents either a token or a client certificate, not both");
    }

    if (hasToken)
    {
      if (!AcceptsTokens)
      {
        throw new FormatException("Tokens are not accepted by this deployment");
      }

      // Read first: the token handler answers a token it cannot check, without the keys of the authority, as invalid,
      // while it says nothing of the caller. Throws when the metadata cannot be read, answered as unavailable.
      if (Metadata is { } metadata)
      {
        await metadata.GetConfigurationAsync(context.RequestAborted)
                      .ConfigureAwait(false);
      }

      var result = await context.AuthenticateAsync(JwtBearerDefaults.AuthenticationScheme)
                                .ConfigureAwait(false);

      if (!result.Succeeded)
      {
        throw new FormatException($"The token is invalid: {result.Failure?.Message ?? "it could not be validated"}");
      }

      return TokenIdentity.FromClaims(result.Principal,
                                      options_.SubjectClaim,
                                      options_.RolesClaim);
    }

    if (hasCertificate)
    {
      using var x509 = ParseCertificate(certificate);
      return CertificateIdentity.FromCertificate(x509);
    }

    return null;
  }

  private static X509Certificate2 ParseCertificate(string escapedPem)
  {
    try
    {
      return X509Certificate2.CreateFromPem(Uri.UnescapeDataString(escapedPem));
    }
    catch (Exception e) when (e is CryptographicException or ArgumentException or UriFormatException)
    {
      throw new FormatException("The forwarded client certificate is not URL-escaped PEM",
                                e);
    }
  }
}
