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

using ArmoniK.Core.Common.Auth.Authorization;
using ArmoniK.Core.Common.Exceptions;

using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using System.Linq;
using System.Text.Encodings.Web;
using System.Threading;
using System.Threading.Tasks;

namespace ArmoniK.Core.Common.Auth.Authentication
{
  public class AuthenticatorOptions : AuthenticationSchemeOptions
  {
    public const string SectionName = nameof(AuthenticatorOptions);

    public void CopyFrom(AuthenticatorOptions o)
    {
      CNHeader                  = o.CNHeader;
      FingerprintHeader         = o.FingerprintHeader;
      ImpersonationHeader       = o.ImpersonationHeader;
      ImpersonationWithUsername = o.ImpersonationWithUsername;
    }

    public string? CNHeader          { get; set; }
    public string? FingerprintHeader { get; set; }

    public string? ImpersonationHeader       { get; set; }
    public bool?   ImpersonationWithUsername { get; set; }
  }

  public class Authenticator : AuthenticationHandler<AuthenticatorOptions>
  {
    private readonly ILogger<Authenticator> logger_;
    private readonly string                 cnHeader_;
    private readonly string                 fingerprintHeader_;
    private readonly string?                impersonationHeader_;
    private readonly IAuthenticationTable  authTable_;
    private readonly bool                   impersonationWithUsername_;

    public Authenticator(IOptionsMonitor<AuthenticatorOptions> options,
                         ILoggerFactory                        loggerFactory,
                         UrlEncoder                            encoder,
                         ISystemClock                          clock,
                         IAuthenticationTable                 authTable)
      : base(options,
             loggerFactory,
             encoder,
             clock)
    {
      fingerprintHeader_ = options.CurrentValue.FingerprintHeader ??
                           throw new ArmoniKException($"{AuthenticatorOptions.SectionName}.FingerprintHeader is not configured");
      cnHeader_                  = options.CurrentValue.CNHeader ?? throw new ArmoniKException($"{AuthenticatorOptions.SectionName}.CNHeader is not configured");
      impersonationHeader_       = options.CurrentValue.ImpersonationHeader;
      impersonationWithUsername_ = options.CurrentValue.ImpersonationWithUsername ?? false;

      authTable_ = authTable;
      logger_     = loggerFactory.CreateLogger<Authenticator>();
    }

    protected override async Task<AuthenticateResult> HandleAuthenticateAsync()
    {
      UserIdentity? identity;
      if (Request.Headers.TryGetValue(cnHeader_,
                                      out var cns) && Request.Headers.TryGetValue(fingerprintHeader_,
                                                                                  out var fingerprints))
      {
        var cn          = cns.First();
        var fingerprint = fingerprints.First();
        logger_.LogDebug("Authenticating request with CN {CN} and fingerprint {Fingerprint}",
                         cn,
                         fingerprint);
        identity = await authTable_.GetIdentityAsync(cn,
                                                      fingerprint,
                                                      new CancellationToken(false))
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

      if (impersonationHeader_ != null && Request.Headers.TryGetValue(impersonationHeader_,
                                                                                                                   out var imps) &&
          !string.IsNullOrWhiteSpace(imps.First()))
      {
        if (identity.HasClaim(c => c.Type == Permissions.Impersonate.Claim.Type))
        {
          //Get all roles that can be impersonnated
          var impersonatableRoles = identity.Claims.Where(c => c.Type == Permissions.Impersonate.Claim.Type)
                                            .Select(c => c.Value);

          if (impersonationWithUsername_)
          {
            identity = await authTable_.GetIdentityFromNameAsync(imps.First(),
                                                                  new CancellationToken(false))
                                        .ConfigureAwait(false);
          }
          else
          {
            identity = await authTable_.GetIdentityFromIdAsync(imps.First(),
                                                                new CancellationToken(false))
                                        .ConfigureAwait(false);
          }

          if (identity == null)
          {
            return AuthenticateResult.Fail("User being impersonated doesn't exist");
          }

          if (!identity.Roles.All(str => impersonatableRoles.Contains(str)))
          {
            return AuthenticateResult.Fail("Certificate doesn't allow to impersonate the specified user (insufficient roles)");
          }
        }
        else
        {
          return AuthenticateResult.Fail("Given certificate cannot impersonate a user");
        }
      }

      var ticket = new AuthenticationTicket(identity,
                                            Scheme.Name);
      return AuthenticateResult.Success(ticket);
    }
  }
}
