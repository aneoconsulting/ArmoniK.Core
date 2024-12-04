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

using System.Linq;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

using ArmoniK.Core.Base;
using ArmoniK.Core.Utils;

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using RabbitMQ.Client;

namespace ArmoniK.Core.Adapters.Amqp;

/// <summary>
///   Class for building Amqp object and Queue interfaces through Dependency Injection
/// </summary>
[PublicAPI]
public class QueueBuilder : IDependencyInjectionBuildable
{
  /// <inheritdoc />
  [PublicAPI]
  public void Build(IServiceCollection   serviceCollection,
                    ConfigurationManager configuration,
                    ILogger              logger)
  {
    logger.LogInformation("Configure Amqp client");


    serviceCollection.AddOption(configuration,
                                QueueCommon.Amqp.SettingSection,
                                out QueueCommon.Amqp amqpOptions);

    if (!string.IsNullOrEmpty(amqpOptions.CredentialsPath))
    {
      configuration.AddJsonFile(amqpOptions.CredentialsPath,
                                false,
                                false);
      logger.LogTrace("Loaded amqp credentials from file {path}",
                      amqpOptions.CredentialsPath);

      serviceCollection.AddOption(configuration,
                                  QueueCommon.Amqp.SettingSection,
                                  out amqpOptions);
    }
    else
    {
      logger.LogTrace("No credential path provided");
    }

    if (!string.IsNullOrEmpty(amqpOptions.CaPath))
    {
      var authority = new X509Certificate2(amqpOptions.CaPath);

      // Configure the SSL settings
      var sslOption = new SslOption
                      {
                        Enabled                = true,
                        ServerName             = amqpOptions.Host,
                        Certs                  = new X509Certificate2Collection(),
                        AcceptablePolicyErrors = SslPolicyErrors.RemoteCertificateChainErrors,
                        CertificateValidationCallback = (sender,
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
                                                            logger.LogError("SSL validation failed: {errors}",
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
                                                        },
                      };

      // Apply the SSL settings to your RabbitMQ connection factory
      var factory = new ConnectionFactory
                    {
                      HostName = amqpOptions.Host,
                      UserName = amqpOptions.User,
                      Password = amqpOptions.Password,
                      Ssl      = sslOption,
                    };

      serviceCollection.AddSingleton(factory);
    }
    else
    {
      logger.LogTrace("No CA path provided");
    }

    serviceCollection.AddSingletonWithHealthCheck<IConnectionAmqp, ConnectionAmqp>(nameof(IConnectionAmqp));
    serviceCollection.AddSingleton<IPushQueueStorage, PushQueueStorage>();
    serviceCollection.AddSingleton<IPullQueueStorage, PullQueueStorage>();

    logger.LogInformation("Amqp configuration complete");
  }
}
