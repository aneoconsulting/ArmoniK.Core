// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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
using System.Security.Cryptography.X509Certificates;

using ArmoniK.Core.Adapters.QueueCommon;
using ArmoniK.Core.Base;
using ArmoniK.Core.Utils;

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using RabbitMQ.Client;

namespace ArmoniK.Core.Adapters.RabbitMQ;

/// <summary>
///   Class for building RabbitMQ object and Queue interfaces through Dependency Injection
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
    logger.LogInformation("Configure RabbitMQ client");


    serviceCollection.AddOption(configuration,
                                Amqp.SettingSection,
                                out Amqp amqpOptions);


    if (!string.IsNullOrEmpty(amqpOptions.CredentialsPath))
    {
      configuration.AddJsonFile(amqpOptions.CredentialsPath,
                                false,
                                false);
      logger.LogTrace("Loaded amqp credentials from file {path}",
                      amqpOptions.CredentialsPath);

      serviceCollection.AddOption(configuration,
                                  Amqp.SettingSection,
                                  out amqpOptions);
    }
    else
    {
      logger.LogTrace("No credential path provided");
    }

    SslOption? sslOption = null;
    if (!string.IsNullOrEmpty(amqpOptions.CaPath))
    {
      if (!File.Exists(amqpOptions.CaPath))
      {
        logger.LogError("CA file {path} does not exist",
                        amqpOptions.CaPath);
        throw new FileNotFoundException("Root certificate file not found",
                                        amqpOptions.CaPath);
      }

      var authority = new X509Certificate2(amqpOptions.CaPath);
      logger.LogInformation("Loaded CA certificate from file {path}",
                            amqpOptions.CaPath);

      sslOption = new SslOption
                  {
                    Enabled    = true,
                    ServerName = amqpOptions.Host,
                    CertificateValidationCallback = (sender,
                                                     certificate,
                                                     chain,
                                                     sslPolicyErrors) =>
                                                    {
                                                      if (certificate == null || chain == null)
                                                      {
                                                        logger.LogWarning("Certificate or certificate chain is null");
                                                        return false;
                                                      }

                                                      return CertificateValidator.ValidateServerCertificate(sender,
                                                                                                            certificate,
                                                                                                            chain,
                                                                                                            sslPolicyErrors,
                                                                                                            authority,
                                                                                                            logger);
                                                    }
                  };
      logger.LogDebug("Server certificate validation callback set");
    }
    else
    {
      logger.LogWarning("No CA certificate provided");
    }

    serviceCollection.AddSingleton<IConnectionRabbit>(sp => new ConnectionRabbit(amqpOptions,
                                                                                 sp.GetRequiredService<ILogger<ConnectionRabbit>>(),
                                                                                 sslOption));
    serviceCollection.AddSingleton<IPushQueueStorage, PushQueueStorage>();
    serviceCollection.AddSingleton<IPullQueueStorage, PullQueueStorage>();

    logger.LogInformation("RabbitMQ configuration complete");
  }
}
