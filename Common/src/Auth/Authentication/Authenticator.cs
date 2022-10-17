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

using System.Linq;
using System.Security.Authentication;
using System.Text.Encodings.Web;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Auth.Authorization;
using ArmoniK.Core.Common.Exceptions;

using JetBrains.Annotations;

using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ArmoniK.Core.Common.Auth.Authentication;

[PublicAPI]
public class AuthenticatorOptions : AuthenticationSchemeOptions
{
  public const string SectionName = nameof(Authenticator);

  /// <summary>
  ///   Default options used when no authentication is required
  /// </summary>
  public static readonly AuthenticatorOptions DefaultNoAuth = new()
                                                              {
                                                                RequireAuthentication = false,
                                                                RequireAuthorization  = false,
                                                              };

  /// <summary>
  ///   Default options used when authentication and authorization are required
  /// </summary>
  public static readonly AuthenticatorOptions DefaultAuth = new()
                                                            {
                                                              CNHeader                    = "X-Certificate-Client-CN",
                                                              FingerprintHeader           = "X-Certificate-Client-Fingerprint",
                                                              ImpersonationUsernameHeader = "X-Impersonate-Username",
                                                              ImpersonationIdHeader       = "X-Impersonate-Id",
                                                              RequireAuthentication       = true,
                                                              RequireAuthorization        = true,
                                                            };

  /// <summary>
  ///   Default options, will prevent launch as a fail-dead as it requires a proper configuration
  /// </summary>
  public static readonly AuthenticatorOptions Default = new();

  // ReSharper disable once InconsistentNaming
  public string CNHeader                    { get; set; } = "";
  public string FingerprintHeader           { get; set; } = "";
  public string ImpersonationUsernameHeader { get; set; } = "";

  public string ImpersonationIdHeader { get; set; } = "";

  public bool RequireAuthentication { get; set; } = true;
  public bool RequireAuthorization  { get; set; } = true;

  public void CopyFrom(AuthenticatorOptions other)
  {
    CNHeader                    = other.CNHeader;
    FingerprintHeader           = other.FingerprintHeader;
    ImpersonationIdHeader       = other.ImpersonationIdHeader;
    ImpersonationUsernameHeader = other.ImpersonationUsernameHeader;
    RequireAuthentication       = other.RequireAuthentication;
    RequireAuthorization        = other.RequireAuthorization;
  }
}

public class Authenticator : AuthenticationHandler<AuthenticatorOptions>
{
  public const string SchemeName = "SubmitterAuthenticationScheme";

  private static readonly UserIdentity DefaultUser = new(new UserAuthenticationResult(),
                                                         SchemeName);

  private readonly IAuthenticationTable   authTable_;
  private readonly AuthenticationCache    cache_;
  private readonly string                 cnHeader_;
  private readonly string                 fingerprintHeader_;
  private readonly string                 impersonationIdHeader_;
  private readonly string                 impersonationUsernameHeader_;
  private readonly ILogger<Authenticator> logger_;

  private readonly bool requireAuthentication_;

  public Authenticator(IOptionsMonitor<AuthenticatorOptions> options,
                       ILoggerFactory                        loggerFactory,
                       UrlEncoder                            encoder,
                       ISystemClock                          clock,
                       IAuthenticationTable                  authTable,
                       AuthenticationCache                   cache)
    : base(options,
           loggerFactory,
           encoder,
           clock)
  {
    requireAuthentication_ = options.CurrentValue.RequireAuthentication;
    fingerprintHeader_     = options.CurrentValue.FingerprintHeader;
    if (requireAuthentication_ && string.IsNullOrEmpty(fingerprintHeader_))
    {
      throw new ArmoniKException($"{AuthenticatorOptions.SectionName}.FingerprintHeader is not configured");
    }

    cnHeader_ = options.CurrentValue.CNHeader;
    if (requireAuthentication_ && string.IsNullOrEmpty(cnHeader_))
    {
      throw new ArmoniKException($"{AuthenticatorOptions.SectionName}.CNHeader is not configured");
    }

    impersonationUsernameHeader_ = options.CurrentValue.ImpersonationUsernameHeader;
    impersonationIdHeader_       = options.CurrentValue.ImpersonationIdHeader;

    authTable_ = authTable;
    cache_     = cache;
    logger_    = loggerFactory.CreateLogger<Authenticator>();
  }

  /// <summary>
  ///   Function called by the Authentication middleware to get the authentication ticket for the user
  /// </summary>
  /// <returns></returns>
  [UsedImplicitly]
  protected override async Task<AuthenticateResult> HandleAuthenticateAsync()
  {
    // Bypass if authentication is not required
    if (!requireAuthentication_)
    {
      return AuthenticateResult.Success(new AuthenticationTicket(DefaultUser,
                                                                 SchemeName));
    }

    var cn                    = TryGetHeader(cnHeader_);
    var fingerprint           = TryGetHeader(fingerprintHeader_);
    var impersonationUsername = TryGetHeader(impersonationUsernameHeader_);
    var impersonationId       = TryGetHeader(impersonationIdHeader_);

    var cacheKey = new AuthenticationCacheKey(Request.HttpContext.Connection.Id,
                                              cn,
                                              fingerprint,
                                              impersonationId,
                                              impersonationUsername);
    var keyHash = cacheKey.GetHashCode();

    var identity = cache_.Get(cacheKey);
    if (identity != null)
    {
      logger_.LogInformation($"Found authenticated user {identity.UserName} in cache. Authentication hashkey : {keyHash}.");
      return AuthenticateResult.Success(new AuthenticationTicket(identity,
                                                                 SchemeName));
    }

    if (!string.IsNullOrEmpty(cn) && !string.IsNullOrEmpty(fingerprint))
    {
      identity = await GetIdentityFromCertificateAsync(cn,
                                                       fingerprint)
                   .ConfigureAwait(false);


      if (identity == null)
      {
        return AuthenticateResult.Fail("Unrecognized user certificate");
      }
    }
    else
    {
      return AuthenticateResult.Fail("Missing Certificate Headers");
    }

    // Try to impersonate only if at least one of the impersonation headers is set
    if (impersonationId != null || impersonationUsername != null)
    {
      // Only users with the impersonate permission can impersonate
      if (identity.HasClaim(c => c.Type == Permissions.Impersonate.Claim.Type))
      {
        try
        {
          var prevIdentity = identity;
          identity = await GetImpersonatedIdentityAsync(identity,
                                                        impersonationId,
                                                        impersonationUsername)
                       .ConfigureAwait(false);
          logger_.LogInformation($"User with id {prevIdentity.UserId} and name {prevIdentity.UserName} impersonated the user with id {identity.UserId} and name {identity.UserName}. Authentication key : {keyHash}");
        }
        catch (AuthenticationException e)
        {
          return AuthenticateResult.Fail(e.Message);
        }
      }
      else
      {
        return AuthenticateResult.Fail("Given certificate cannot impersonate a user");
      }
    }

    cache_.Set(cacheKey,
               identity);
    // Authentication hasn't been rejected, create the ticket and authenticate the user
    var ticket = new AuthenticationTicket(identity,
                                          SchemeName);
    return AuthenticateResult.Success(ticket);
  }

  /// <summary>
  ///   Get the UserIdentity from the CN and Fingerprint of a certificate
  /// </summary>
  /// <param name="cn">Common name of the certificate</param>
  /// <param name="fingerprint">Fingerprint of the certificate</param>
  /// <param name="cancellationToken">Cancellation token</param>
  /// <returns>
  ///   A UserIdentity object which can be used in authentication, corresponding to the certificate. Null if it
  ///   doesn't correspond to any user.
  /// </returns>
  public async Task<UserIdentity?> GetIdentityFromCertificateAsync(string            cn,
                                                                   string            fingerprint,
                                                                   CancellationToken cancellationToken = default)
  {
    logger_.LogDebug("Authenticating request with CN {CN} and fingerprint {Fingerprint}",
                     cn,
                     fingerprint);
    var result = await authTable_.GetIdentityFromCertificateAsync(cn,
                                                                  fingerprint,
                                                                  cancellationToken)
                                 .ConfigureAwait(false);

    if (result == null)
    {
      return null;
    }

    return new UserIdentity(result,
                            SchemeName);
  }

  /// <summary>
  ///   Get the UserIdentity attempting to be impersonated by the user
  /// </summary>
  /// <param name="baseIdentity">UserIdentity trying to impersonate</param>
  /// <param name="impersonationId">Id of the user being impersonated</param>
  /// <param name="impersonationUsername">Username of the user being impersonated</param>
  /// <param name="cancellationToken">Cancellation token</param>
  /// <returns>The impersonated user's UserIdentity</returns>
  /// <exception cref="AuthenticationException">
  ///   Thrown when both id and username are missing,
  ///   the impersonated user doesn't exist,
  ///   or the impersonating user doesn't have the permissions to impersonate the specified user
  /// </exception>
  public async Task<UserIdentity> GetImpersonatedIdentityAsync(UserIdentity      baseIdentity,
                                                               string?           impersonationId,
                                                               string?           impersonationUsername,
                                                               CancellationToken cancellationToken = default)
  {
    UserAuthenticationResult? result = null;
    if (impersonationId != null || impersonationUsername != null)
    {
      result = await authTable_.GetIdentityFromUserAsync(impersonationId,
                                                         impersonationUsername,
                                                         cancellationToken)
                               .ConfigureAwait(false);
    }
    else
    {
      throw new AuthenticationException("Impersonation headers are missing");
    }

    if (result == null)
    {
      throw new AuthenticationException("User being impersonated does not exist");
    }


    // User exists and can be impersonated according to the impersonation permissions of the base user
    if (result.Roles.All(role => baseIdentity.HasClaim(Permissions.Impersonate.Claim.Type,
                                                       role)))
    {
      return new UserIdentity(result,
                              SchemeName);
    }


    // User exists but the base user doesn't have enough permissions to impersonate them
    throw new AuthenticationException("Certificate does not allow to impersonate the specified user (insufficient roles)");
  }

  /// <summary>
  ///   Tries to retrieve the given header from the request, return null if it doesn't exist or is empty
  /// </summary>
  /// <param name="headerName">Name of the header</param>
  /// <returns>Value of the header if found and not empty, null otherwise</returns>
  private string? TryGetHeader(string headerName)
  {
    if (!string.IsNullOrEmpty(headerName) && Request.Headers.TryGetValue(headerName,
                                                                                                                        out var values) &&
        !string.IsNullOrWhiteSpace(values.First()))
    {
      return values.First();
    }

    return null;
  }
}
