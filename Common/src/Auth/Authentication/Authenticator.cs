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

using System.ComponentModel;

using ArmoniK.Core.Common.Auth.Authorization;
using ArmoniK.Core.Common.Exceptions;

using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using System.Linq;
using System.Text.Encodings.Web;
using System.Threading;
using System.Threading.Tasks;

using JetBrains.Annotations;

namespace ArmoniK.Core.Common.Auth.Authentication;

[PublicAPI]
public class AuthenticatorOptions : AuthenticationSchemeOptions
{
  public const string SectionName = nameof(Authenticator);

  public string? CNHeader          { get; set; }
  public string? FingerprintHeader { get; set; }

  public string? ImpersonationHeader       { get; set; }
  public bool?   ImpersonationWithUsername { get; set; }

  public bool? RequireAuthentication { get; set; }
  public bool? RequireAuthorization  { get; set; }

  public void CopyFrom(AuthenticatorOptions other)
  {
    CNHeader                  = other.CNHeader;
    FingerprintHeader         = other.FingerprintHeader;
    ImpersonationHeader       = other.ImpersonationHeader;
    ImpersonationWithUsername = other.ImpersonationWithUsername;
    RequireAuthentication     = other.RequireAuthentication;
    RequireAuthorization      = other.RequireAuthorization;
  }

  public static readonly AuthenticatorOptions DefaultNoAuth = new()
                                                       {
                                                         RequireAuthentication = false,
                                                         RequireAuthorization  = false,
                                                       };

  public static readonly AuthenticatorOptions Default = new()
                                                 {
                                                   CNHeader = "X-Certificate-Client-CN",
                                                   FingerprintHeader = "X-Certificate-Client-Fingerprint",
                                                   ImpersonationHeader = "X-Client-Impersonate",
                                                   ImpersonationWithUsername = true,
                                                   RequireAuthentication = true,
                                                   RequireAuthorization = true,
                                                 };
}

public class Authenticator : AuthenticationHandler<AuthenticatorOptions>
{
  public const string SchemeName = "SubmitterAuthenticationScheme";

  private readonly ILogger<Authenticator> logger_;
  private readonly string                 cnHeader_;
  private readonly string                 fingerprintHeader_;
  private readonly string?                impersonationHeader_;
  private readonly IAuthenticationTable   authTable_;
  private readonly bool                   impersonationWithUsername_;
  private readonly bool                   requireAuthentication_;

  public Authenticator(IOptionsMonitor<AuthenticatorOptions> options,
                       ILoggerFactory                        loggerFactory,
                       UrlEncoder                            encoder,
                       ISystemClock                          clock,
                       IAuthenticationTable                  authTable)
    : base(options,
           loggerFactory,
           encoder,
           clock)
  {
    requireAuthentication_ = options.CurrentValue.RequireAuthentication ?? true;
    fingerprintHeader_ = options.CurrentValue.FingerprintHeader ?? (!requireAuthentication_
                                                                      ? ""
                                                                      : throw new
                                                                          ArmoniKException($"{AuthenticatorOptions.SectionName}.FingerprintHeader is not configured"));
    cnHeader_ = options.CurrentValue.CNHeader ?? (!requireAuthentication_
                                                    ? ""
                                                    : throw new ArmoniKException($"{AuthenticatorOptions.SectionName}.CNHeader is not configured"));
    impersonationHeader_       = options.CurrentValue.ImpersonationHeader;
    impersonationWithUsername_ = options.CurrentValue.ImpersonationWithUsername ?? false;

    authTable_ = authTable;
    logger_    = loggerFactory.CreateLogger<Authenticator>();
  }

  protected override async Task<AuthenticateResult> HandleAuthenticateAsync()
  {
    if (!requireAuthentication_)
    {
      return AuthenticateResult.Success(new AuthenticationTicket(new UserIdentity(new UserAuthenticationResult(),
                                                                                  SchemeName),
                                                                 SchemeName));
    }

    UserIdentity? identity;
    if (Request.Headers.TryGetValue(cnHeader_,
                                    out var cns) && Request.Headers.TryGetValue(fingerprintHeader_,
                                                                                out var fingerprints))
    {
      var cn          = cns.First();
      var fingerprint = fingerprints.First();

      identity = await GetIdentityFromCertificateAsync(cn,
                                            fingerprint).ConfigureAwait(false);

      
      if (identity == null)
      {
        return AuthenticateResult.Fail("Unrecognized user certificate");
      }
    }
    else
    {
      return AuthenticateResult.Fail("Missing Certificate Headers");
    }

    if (impersonationHeader_ != null
        && Request.Headers.TryGetValue(impersonationHeader_, out var imps)
        && !string.IsNullOrWhiteSpace(imps.First()))
    {
      if (identity.HasClaim(c => c.Type == Permissions.Impersonate.Claim.Type))
      {
        identity = await GetImpersonationatedIdentity(identity,
                                                      imps.First())
                     .ConfigureAwait(false);

        if (identity == null)
          return AuthenticateResult.Fail("User being impersonated doesn't exist");
        if (identity.Identity == null)
          return AuthenticateResult.Fail("Certificate doesn't allow to impersonate the specified user (insufficient roles)");
      }
      else
      {
        return AuthenticateResult.Fail("Given certificate cannot impersonate a user");
      }
    }

    var ticket = new AuthenticationTicket(identity,
                                          SchemeName);
    return AuthenticateResult.Success(ticket);
  }

  public async Task<UserIdentity?> GetIdentityFromCertificateAsync(string cn,
                                                              string fingerprint)
  {
    logger_.LogDebug("Authenticating request with CN {CN} and fingerprint {Fingerprint}",
                     cn,
                     fingerprint);
    var result = await authTable_.GetIdentityAsync(cn,
                                                   fingerprint,
                                                   new CancellationToken(false))
                                 .ConfigureAwait(false);

    if (result == null)
      return null;
    return new UserIdentity(result,
                            SchemeName);
  }

  public async Task<UserIdentity?> GetImpersonationatedIdentity(UserIdentity baseIdentity,
                                                                string       impersonnated)
  {
    // Get all roles that can be impersonated
    var impersonatableRoles = baseIdentity.Claims.Where(c => c.Type == Permissions.Impersonate.Claim.Type)
                                          .Select(c => c.Value);
    UserAuthenticationResult? result;
    // Impersonation can be done using the username or the userid
    if (impersonationWithUsername_)
    {
      result = await authTable_.GetIdentityFromNameAsync(impersonnated,
                                                         new CancellationToken(false))
                               .ConfigureAwait(false);
    }
    else
    {
      result = await authTable_.GetIdentityFromIdAsync(impersonnated,
                                                       new CancellationToken(false))
                               .ConfigureAwait(false);
    }

    // User being impersonated doesn't exist
    if (result == null)
      return null;

    // User exists and can be impersonnated according to the impersonation permissions of the base user
    if (result.Roles.All(str => impersonatableRoles.Contains(str)))
      return new UserIdentity(result,
                          SchemeName);

    // User exists but the base user doesn't have enough permissions to impersonate them
    return new UserIdentity(result);

  }
}
