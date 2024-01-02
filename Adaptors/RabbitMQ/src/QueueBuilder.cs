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

using System;
using System.Security.Cryptography.X509Certificates;

using ArmoniK.Core.Adapters.QueueCommon;
using ArmoniK.Core.Base;
using ArmoniK.Core.Utils;

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

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

    if (!string.IsNullOrEmpty(amqpOptions.CaPath))
    {
      var localTrustStore       = new X509Store(StoreName.Root);
      var certificateCollection = new X509Certificate2Collection();
      try
      {
        certificateCollection.ImportFromPemFile(amqpOptions.CaPath);
        localTrustStore.Open(OpenFlags.ReadWrite);
        localTrustStore.AddRange(certificateCollection);
        logger.LogTrace("Imported AMQP certificate from file {path}",
                        amqpOptions.CaPath);
      }
      catch (Exception ex)
      {
        logger.LogError("Root certificate import failed: {error}",
                        ex.Message);
        throw;
      }
      finally
      {
        localTrustStore.Close();
      }
    }

    serviceCollection.AddSingletonWithHealthCheck<IConnectionRabbit, ConnectionRabbit>(nameof(IConnectionRabbit));
    serviceCollection.AddSingleton<IPushQueueStorage, PushQueueStorage>();
    serviceCollection.AddSingleton<IPullQueueStorage, PullQueueStorage>();

    logger.LogInformation("RabbitMQ configuration complete");
  }
}
