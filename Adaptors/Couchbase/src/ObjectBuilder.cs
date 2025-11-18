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

using System;

using ArmoniK.Core.Base;
using ArmoniK.Core.Utils;

using Couchbase;

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Couchbase
{
  public class ObjectBuilder : IDependencyInjectionBuildable
  {
    /// <inheritdoc />
    [PublicAPI]
    public void Build(IServiceCollection serviceCollection,
                      ConfigurationManager configuration,
                      Microsoft.Extensions.Logging.ILogger logger)
    {
      Options.Couchbase couchbaseOptions;

      serviceCollection.AddOption(configuration,
                                  Options.Couchbase.SettingSection,
                                  out couchbaseOptions);

      var clusterOptions = new ClusterOptions
      {
        UserName = couchbaseOptions.Login,
        Password = couchbaseOptions.Password,

        // Extended timeouts for operations
        KvTimeout = couchbaseOptions.KvTimeout,
        QueryTimeout = couchbaseOptions.QueryTimeout,
        ManagementTimeout = couchbaseOptions.ManagementTimeout,

        NumKvConnections = couchbaseOptions.NumKvConnections,
        EnableTcpKeepAlives = couchbaseOptions.EnableTcpKeepAlive,
        TcpKeepAliveTime = couchbaseOptions.TcpKeepAliveTime,
        TcpKeepAliveInterval = couchbaseOptions.TcpKeepAliveInterval,

        // Connection pool settings
        MaxKvConnections = couchbaseOptions.MaxKvConnections,

        // Retry policy settings
        EnableOperationDurationTracing = couchbaseOptions.EnableOperationDurationTracing,
      };

      if (couchbaseOptions.IsTls)
      {
        clusterOptions.EnableTls = true;
      }

      clusterOptions.WithConnectionString(couchbaseOptions.ConnectionString)
                          .WithCredentials(couchbaseOptions.Login, couchbaseOptions.Password);

      logger.LogDebug("Setup connection to Couchbase at {ConnectionString} with user {User}",
                      couchbaseOptions.ConnectionString,
                      couchbaseOptions.Login);

      try
      {
        serviceCollection.AddSingleton<ICluster>(_ => 
        {
          return Cluster.ConnectAsync(couchbaseOptions.ConnectionString, clusterOptions)
                       .GetAwaiter()
                       .GetResult();
        });

        serviceCollection.AddSingletonWithHealthCheck<IObjectStorage, ObjectStorage>(nameof(IObjectStorage));
      }
      catch (Exception ex)
      {
        logger.LogError(ex, "Cluster.ConnectAsync failed. TLS={IsTls}, ConnectionString={ConnectionString}, User={User}",
                        couchbaseOptions.IsTls, 
                        couchbaseOptions.ConnectionString, 
                        couchbaseOptions.Login);
        throw;
      }
    }
  }
}
