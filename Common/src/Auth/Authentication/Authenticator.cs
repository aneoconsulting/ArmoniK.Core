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

using System.Security.Claims;
using System.Text.Encodings.Web;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ArmoniK.Core.Common.Auth.Authentication
{
  public class AuthenticatorOptions : AuthenticationSchemeOptions
  {

    public static string SectionName = nameof(AuthenticatorOptions);
    public AuthenticatorOptions()
    {
      CNHeader          = "X-Certificate-Client-CN";
      FingerprintHeader = "X-Certificate-Client-Fingerprint";
    }

    public void CopyFrom(AuthenticatorOptions o)
    {
      CNHeader = o.CNHeader;
      FingerprintHeader = o.FingerprintHeader;
    }

    public string                 CNHeader          { get; set; }
    public string                 FingerprintHeader { get; set; }
  }

  public class Authenticator : AuthenticationHandler<AuthenticatorOptions>
  {
    private readonly ILogger<Authenticator> logger_;
    private readonly string                 cnHeader_;
    private readonly string                 fingerprintHeader_;
    private readonly IAuthenticationSource  authSource_;

    public Authenticator(IOptionsMonitor<AuthenticatorOptions> options,
                         ILoggerFactory                        loggerFactory,
                         UrlEncoder                            encoder,
                         ISystemClock                          clock,
                         IAuthenticationSource authSource)
      : base(options,
             loggerFactory,
             encoder,
             clock)
    {
      fingerprintHeader_ = options.CurrentValue.FingerprintHeader;
      cnHeader_          = options.CurrentValue.CNHeader;
      authSource_        = authSource;
      logger_            = loggerFactory.CreateLogger<Authenticator>();
    }

    protected override async Task<AuthenticateResult> HandleAuthenticateAsync()
    {
      var cn = Request.Headers[cnHeader_]
                      .ToString();
      var fingerprint = Request.Headers[fingerprintHeader_]
                               .ToString();
      logger_.LogDebug("Authenticating request with CN {CN} and fingerprint {Fingerprint}",
                       cn,
                       fingerprint);

      var identity = await authSource_.GetIdentityAsync(cn, fingerprint, new CancellationToken(false))
                                .ConfigureAwait(false);
      if (identity == null)
      {
        return AuthenticateResult.Fail("Unrecognized fingerprint");
      }

      var ticket = new AuthenticationTicket(new ClaimsPrincipal(identity),
                                            Scheme.Name);
      return AuthenticateResult.Success(ticket);
    }
  }
}
