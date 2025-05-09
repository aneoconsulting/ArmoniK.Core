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

using ArmoniK.Core.Base;
using ArmoniK.Core.Utils;

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

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

    serviceCollection.AddSingletonWithHealthCheck<IConnectionAmqp, ConnectionAmqp>(nameof(IConnectionAmqp));
    serviceCollection.AddSingleton<IPushQueueStorage, PushQueueStorage>();
    serviceCollection.AddSingleton<IPullQueueStorage, PullQueueStorage>();

    logger.LogInformation("Amqp configuration complete");
  }
}
