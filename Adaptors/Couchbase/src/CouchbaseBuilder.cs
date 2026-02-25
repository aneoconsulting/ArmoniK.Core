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
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using ArmoniK.Core.Base;
using ArmoniK.Core.Utils;

using Couchbase;
using Couchbase.Core.Retry;
using Couchbase.Extensions.DependencyInjection;

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using System;

namespace ArmoniK.Core.Adapters.Couchbase
{
  public class CouchbaseBuilder : IDependencyInjectionBuildable
  {
    /// <inheritdoc />
    [PublicAPI]
    public void Build(IServiceCollection serviceCollection,
                      ConfigurationManager configuration,
                      ILogger logger)
    {
      Options.CouchbaseSettings couchbaseOptions;
      Options.CouchbaseStorage couchbaseStorageOptions;

      serviceCollection.AddOption(configuration,
                                             Options.CouchbaseSettings.SettingSection,
                                             out couchbaseOptions);

      serviceCollection.AddOption(configuration,
                                  Options.CouchbaseStorage.SettingSection,
                                  out couchbaseStorageOptions);

      logger.LogDebug("Couchbase connection settings: ConnectionString={ConnectionString}, IsTls={IsTls}, BootstrapTimeout={BootstrapTimeout}, KvTimeout={KvTimeout}",
              couchbaseOptions.ConnectionString,
              couchbaseOptions.IsTls,
              couchbaseOptions.BootstrapTimeout,
              couchbaseOptions.KvTimeout);

      try
      {
        // Configure Couchbase using AddCouchbase with proper options configuration
        // This registers IClusterProvider which should be used to get ICluster instances
        serviceCollection.AddCouchbase(options =>
        {
          options.ConnectionString = couchbaseOptions.ConnectionString;
          options.UserName = couchbaseOptions.Login;
          options.Password = couchbaseOptions.Password;

          // Extended timeouts for operations
          options.KvTimeout = couchbaseOptions.KvTimeout;
          options.QueryTimeout = couchbaseOptions.QueryTimeout;
          options.ManagementTimeout = couchbaseOptions.ManagementTimeout;

          options.NumKvConnections = couchbaseOptions.NumKvConnections;
          options.EnableTcpKeepAlives = couchbaseOptions.EnableTcpKeepAlive;
          options.TcpKeepAliveTime = couchbaseOptions.TcpKeepAliveTime;
          options.TcpKeepAliveInterval = couchbaseOptions.TcpKeepAliveInterval;

          // Connection pool settings
          options.MaxKvConnections = couchbaseOptions.MaxKvConnections;

          // Retry policy settings
          options.EnableOperationDurationTracing = couchbaseOptions.EnableOperationDurationTracing;

          // Configure custom retry strategy
          options.RetryStrategy = new RetryCustom();

          if (couchbaseOptions.IsTls)
          {
            options.EnableTls = true;
          }
        });
        serviceCollection.AddSingleton<IPostConfigureOptions<ClusterOptions>, CouchbaseLoggingConfigurator>();
        serviceCollection.AddSingletonWithHealthCheck<IObjectStorage, CouchbaseStorage>(nameof(IObjectStorage));
      }
      catch (Exception ex)
      {
        logger.LogError(ex, "Failed to configure Couchbase. TLS={IsTls}, ConnectionString={ConnectionString}, User={User}",
                        couchbaseOptions.IsTls,
                        couchbaseOptions.ConnectionString,
                        couchbaseOptions.Login);
        throw;
      }
    }
  }

  /// <summary>
  /// Custom retry strategy for Couchbase operations.
  /// Retries up to 3 times with a 1 second delay between attempts.
  /// </summary>
  internal class RetryCustom : IRetryStrategy
  {
    /// <inheritdoc />
    public RetryAction RetryAfter(IRequest request, RetryReason reason)
    {
      return request.Attempts < 3
        ? RetryAction.Duration(TimeSpan.FromSeconds(1))
        : RetryAction.Duration(null);
    }
  }
  /// <summary>
  /// Configures Couchbase SDK logging using ILoggerFactory from DI.
  /// Uses PostConfigure to ensure it runs after the main AddCouchbase configuration.
  /// </summary>
  internal class CouchbaseLoggingConfigurator : IPostConfigureOptions<ClusterOptions>
  {
    private readonly ILoggerFactory loggerFactory_;
    private readonly ILogger logger_;

    public CouchbaseLoggingConfigurator(ILoggerFactory loggerFactory, ILogger<CouchbaseLoggingConfigurator> logger)
    {
      loggerFactory_ = loggerFactory;
      logger_ = logger;
    }

    public void PostConfigure(string? name, ClusterOptions options)
    {
      // Enable Couchbase SDK logging with the application's ILoggerFactory
      options.WithLogging(loggerFactory_);
      logger_.LogDebug("Couchbase SDK logging configured with ILoggerFactory from DI (PostConfigure)");

    }
  }
}
