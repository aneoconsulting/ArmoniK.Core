// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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

using System.IO;
using System.Linq;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Utils;

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using StackExchange.Redis;

namespace ArmoniK.Core.Adapters.Redis;

public static class ServiceCollectionExt
{
  [PublicAPI]
  public static IServiceCollection AddRedis(this IServiceCollection serviceCollection,
                                            ConfigurationManager    configuration,
                                            ILogger                 logger)
  {
    var components = configuration.GetSection(Components.SettingSection);

    if (components["ObjectStorage"] == "ArmoniK.Adapters.Redis.ObjectStorage")
    {
      // ReSharper disable once InlineOutVariableDeclaration
      Options.Redis redisOptions;
      serviceCollection.AddOption(configuration,
                                  Options.Redis.SettingSection,
                                  out redisOptions);

      using var _ = logger.BeginNamedScope("Redis configuration",
                                           ("EndpointUrl", redisOptions.EndpointUrl));

      if (!string.IsNullOrEmpty(redisOptions.CredentialsPath))
      {
        configuration.AddJsonFile(redisOptions.CredentialsPath,
                                  false,
                                  false);

        serviceCollection.AddOption(configuration,
                                    Options.Redis.SettingSection,
                                    out redisOptions);

        logger.LogTrace("Loaded Redis credentials from file {path}",
                        redisOptions.CredentialsPath);
      }

      if (!string.IsNullOrEmpty(redisOptions.CaPath))
      {
        var authority = new X509Certificate2(redisOptions.CaPath);

        // Configure the SSL settings (https://stackexchange.github.io/StackExchange.Redis/Configuration.html)
        var config = new ConfigurationOptions
                     {
                       ClientName           = redisOptions.ClientName,
                       ReconnectRetryPolicy = new ExponentialRetry(10),
                       Ssl                  = redisOptions.Ssl,
                       AbortOnConnectFail   = true,
                       SslHost              = redisOptions.SslHost,
                       Password             = redisOptions.Password,
                       User                 = redisOptions.User,
                       SslProtocols         = SslProtocols.Tls12,
                     };
        config.CertificateValidation += (sender,
                                         certificate,
                                         chain,
                                         sslPolicyErrors) =>
                                        {
                                          if (sslPolicyErrors == SslPolicyErrors.None)
                                          {
                                            return true;
                                          }

                                          if ((sslPolicyErrors & ~SslPolicyErrors.RemoteCertificateChainErrors) != 0)
                                          {
                                            logger.LogError("SSL validation failed: {SslPolicyErrors}",
                                                            sslPolicyErrors);
                                            return false;
                                          }

                                          // If there is any error other than untrusted root or partial chain, fail the validation
                                          if (chain!.ChainStatus.Any(status => status.Status is not X509ChainStatusFlags.UntrustedRoot and
                                                                                                not X509ChainStatusFlags.PartialChain))
                                          {
                                            return false;
                                          }

                                          // Disable some extensive checks that would fail on the authority that is not in store
                                          chain.ChainPolicy.RevocationMode    = X509RevocationMode.NoCheck;
                                          chain.ChainPolicy.VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority;

                                          // Add unknown authority to the store
                                          chain.ChainPolicy.ExtraStore.Add(authority);

                                          // Check if the chain is valid for the actual server certificate (ie: trusted)
                                          if (!chain.Build(new X509Certificate2(certificate!)))
                                          {
                                            logger.LogError("SSL chain validation failed.");
                                            return false;
                                          }

                                          // Check that the chain root is actually the specified authority (caCert)
                                          var isTrusted = chain.ChainElements.Any(x => x.Certificate.Thumbprint == authority.Thumbprint);

                                          if (!isTrusted)
                                          {
                                            logger.LogError("Certificate chain root does not match the specified CA authority.");
                                          }

                                          return isTrusted;
                                        };
        config.EndPoints.Add(redisOptions.EndpointUrl);

        if (redisOptions.Timeout > 0)
        {
          config.ConnectTimeout = redisOptions.Timeout;
        }

        logger.LogDebug("setup connection to Redis at {EndpointUrl} with user {user}",
                        redisOptions.EndpointUrl,
                        redisOptions.User);

        serviceCollection.AddSingleton<IDatabaseAsync>(_ => ConnectionMultiplexer.Connect(config,
                                                                                          TextWriter.Null)
                                                                                 .GetDatabase());
        serviceCollection.AddSingletonWithHealthCheck<IObjectStorage, ObjectStorage>(nameof(IObjectStorage));
      }
      else
      {
        logger.LogTrace("No CA path provided");
      }
    }

    return serviceCollection;
  }
}
