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
using System.Threading.Tasks;

using ArmoniK.Core.Authorization;
using ArmoniK.Core.Common.Injection.Options;

using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Http;

namespace ArmoniK.Core.Common.Enforcement;

/// <summary>
///   Derives the caller of a request from the credential presented: a token or a client certificate forwarded by the
///   ingress, never both
/// </summary>
public class CallerResolver
{
  private const string BearerPrefix = "Bearer ";

  private readonly IdentityOptions options_;

  /// <summary>
  ///   Build the resolver
  /// </summary>
  /// <param name="options">Options of the identification of callers</param>
  public CallerResolver(IdentityOptions options)
    => options_ = options;

  /// <summary>
  ///   Whether tokens are validated, and therefore accepted
  /// </summary>
  public bool AcceptsTokens
    => !string.IsNullOrWhiteSpace(options_.Authority);

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
