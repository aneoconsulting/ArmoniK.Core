// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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

using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Common.Injection;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Storage;

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Amqp;

public static class ServiceCollectionExt
{
  [PublicAPI]
  public static IServiceCollection AddAmqp(this IServiceCollection serviceCollection,
                                           ConfigurationManager    configuration,
                                           ILogger                 logger)
  {
    logger.LogInformation("Configure Amqp client");

    var components = configuration.GetSection(Components.SettingSection);

    if (components["QueueStorage"] == "ArmoniK.Adapters.Amqp.QueueStorage")
    {
      Common.Injection.Options.Amqp amqpOptions;
      serviceCollection.AddOption(configuration,
                                  Common.Injection.Options.Amqp.SettingSection,
                                  out amqpOptions);
      using var _ = logger.BeginNamedScope("AMQP configuration",
                                           ("host", amqpOptions.Host),
                                           ("port", amqpOptions.Port));

      if (!string.IsNullOrEmpty(amqpOptions.CredentialsPath))
      {
        configuration.AddJsonFile(amqpOptions.CredentialsPath,
                                  false,
                                  false);
        logger.LogTrace("Loaded amqp credentials from file {path}",
                        amqpOptions.CredentialsPath);

        serviceCollection.AddOption(configuration,
                                    Common.Injection.Options.Amqp.SettingSection,
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

      serviceCollection.AddSingletonWithHealthCheck<IConnectionAmqp, ConnectionAmqp>(nameof(IConnectionAmqp));
      serviceCollection.AddSingleton<IPushQueueStorage, PushQueueStorage>();
      serviceCollection.AddSingleton<IPullQueueStorage, PullQueueStorage>();

      logger.LogInformation("Amqp configuration complete");
    }
    else
    {
      logger.LogInformation("Nothing to configure");
    }

    return serviceCollection;
  }
}
