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
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Security.Principal;
using System.Text;
using System.Text.Encodings.Web;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Exceptions;

using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Identity;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ArmoniK.Core.Common.Auth
{
  internal class AuthenticatorOptions : AuthenticationSchemeOptions
  {
    public AuthenticatorOptions()
    {
      CNHeader          = "X-Certificate-Client-CN";
      FingerprintHeader = "X-Certificate-Client-Fingerprint";
    }

    public string                 CNHeader          { get; set; }
    public string                 FingerprintHeader { get; set; }
    public IAuthenticationSource? AuthSource        { get; set; }
  }

  internal class Authenticator : AuthenticationHandler<AuthenticatorOptions>
  {
    private readonly ILogger<Authenticator> logger_;
    private readonly string                 cnHeader_;
    private readonly string                 fingerprintHeader_;
    private readonly IAuthenticationSource  authSource_;

    public Authenticator(IOptionsMonitor<AuthenticatorOptions> options,
                         ILoggerFactory                        loggerFactory,
                         UrlEncoder                            encoder,
                         ISystemClock                          clock)
      : base(options,
             loggerFactory,
             encoder,
             clock)
    {
      fingerprintHeader_ = options.CurrentValue.FingerprintHeader;
      cnHeader_          = options.CurrentValue.CNHeader;
      authSource_        = options.CurrentValue.AuthSource ?? throw new ArmoniKException("AuthSource isn't specified");
      logger_            = loggerFactory.CreateLogger<Authenticator>();
    }

    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
      var cn = Request.Headers[cnHeader_]
                      .ToString();
      var fingerprint = Request.Headers[fingerprintHeader_]
                               .ToString();
      logger_.LogDebug("Authenticating request with CN {CN} and fingerprint {Fingerprint}",
                       cn,
                       fingerprint);

      var identity = authSource_.GetIdentity(cn,
                                             fingerprint);
      if (identity == null)
      {
        return Task.FromResult(AuthenticateResult.Fail("Unrecognized fingerprint"));
      }

      var ticket = new AuthenticationTicket(new ClaimsPrincipal(identity),
                                            Scheme.Name);
      return Task.FromResult(AuthenticateResult.Success(ticket));
    }
  }
}
