using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Security.Principal;
using System.Text;
using System.Text.Encodings.Web;
using System.Threading.Tasks;

using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Identity;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ArmoniK.Core.Common.Auth
{
  internal class AuthenticatorOptions : AuthenticationSchemeOptions
  {
    public string CNHeader { get; set; }
    public string FingerprintHeader { get; set; }
    public IAuthenticationSource AuthSource { get; set; }
  }
  internal class Authenticator : AuthenticationHandler<AuthenticatorOptions>
  {
    private readonly ILogger<Authenticator> _logger;
    private readonly string _cnHeader;
    private readonly string _fingerprintHeader;
    private IAuthenticationSource _authSource;
    public Authenticator(IOptionsMonitor<AuthenticatorOptions> options,
             ILoggerFactory    logger,
             UrlEncoder        encoder,
             ISystemClock      clock)
      : base(options,
             logger,
             encoder,
             clock)
    {
      _fingerprintHeader = options.CurrentValue.FingerprintHeader;
      _cnHeader = options.CurrentValue.CNHeader;
      _authSource = options.CurrentValue.AuthSource;
    }

    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
      var cn = Request.Headers[_cnHeader].ToString();
      if (_authSource.ValidateCN(cn))
      {

        var fingerprint = Request.Headers[_fingerprintHeader].ToString();
        IIdentity identity = _authSource.GetIdentity(cn,
                                                     fingerprint);
        
      }

      return AuthenticateAsync();

    }
  }
}
