using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using JetBrains.Annotations;

using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Auth
{
  public static class ServiceCollectionExt
  {
    [PublicAPI]
    public static IServiceCollection AddClientSubmitterAuthentication(this IServiceCollection services,
                                                             ConfigurationManager    configuration,
                                                             ILogger                 logger)
    {
      services.AddAuthentication().AddScheme<AuthenticatorOptions, Authenticator>("SubmitterAuthenticationScheme",
                                                                 o => {});
      return services;
    }

    [PublicAPI]
    public static IServiceCollection AddClientSubmitterAuthorization(this IServiceCollection services,
                                                             ConfigurationManager    configuration,
                                                             ILogger                 logger)
    {

      return services;
    }
  }
}
