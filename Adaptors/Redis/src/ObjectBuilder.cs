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
using System.Net.Security;

using ArmoniK.Core.Base;
using ArmoniK.Core.Utils;

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using StackExchange.Redis;

namespace ArmoniK.Core.Adapters.Redis;

/// <summary>
///   Class for building Redis instance and Object interfaces through Dependency Injection
/// </summary>
[PublicAPI]
public class ObjectBuilder : IDependencyInjectionBuildable
{
  /// <inheritdoc />
  [PublicAPI]
  public void Build(IServiceCollection   serviceCollection,
                    ConfigurationManager configuration,
                    ILogger              logger)
  {
    // ReSharper disable once InlineOutVariableDeclaration
    Options.Redis redisOptions;
    serviceCollection.AddOption(configuration,
                                Options.Redis.SettingSection,
                                out redisOptions);

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

    RemoteCertificateValidationCallback? validationCallback = null;
    var config = new ConfigurationOptions
                 {
                   ClientName           = redisOptions.ClientName,
                   ReconnectRetryPolicy = new ExponentialRetry(10),
                   Ssl                  = redisOptions.Ssl,
                   AbortOnConnectFail   = true,
                   SslHost              = redisOptions.SslHost,
                   Password             = redisOptions.Password,
                   User                 = redisOptions.User,
                 };

    if (redisOptions.Ssl && !string.IsNullOrEmpty(redisOptions.CaPath))
    {
      validationCallback = CertificateValidator.CreateCallback(redisOptions.CaPath,
                                                               logger);
    }

    config.CertificateValidation += validationCallback;
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
}
