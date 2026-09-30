// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using System.Threading;

using ArmoniK.Core.Base;
using ArmoniK.Core.Utils;

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Broker;

/// <summary>
///   Registers the ArmoniK Broker queue in the dependency injection container.
/// </summary>
[PublicAPI]
public class QueueBuilder : IDependencyInjectionBuildable
{
  /// <summary>
  ///   Period of the HTTP/2 PING frames keeping idle connections alive through firewalls and NAT.
  /// </summary>
  private static readonly TimeSpan KeepAlivePingPeriod = TimeSpan.FromSeconds(30);

  /// <summary>
  ///   Timeout of a TCP connection establishment.
  /// </summary>
  private static readonly TimeSpan ConnectTimeout = TimeSpan.FromSeconds(10);

  /// <inheritdoc />
  public void Build(IServiceCollection   serviceCollection,
                    ConfigurationManager configuration,
                    ILogger              logger)
  {
    var options = configuration.GetRequiredValue<Broker>(Broker.SettingSection);
    if (string.IsNullOrEmpty(options.Endpoint))
    {
      throw new InvalidOperationException($"{Broker.SettingSection}:{nameof(Broker.Endpoint)} is required");
    }

    serviceCollection.AddSingleton(options);
    // Request logging is removed: the Pollster long polls in a loop, which would log several lines per second at Information.
    serviceCollection.AddHttpClient(BrokerClient.HttpClientName,
                                    client =>
                                    {
                                      client.BaseAddress = new Uri(options.Endpoint.TrimEnd('/') + "/");
                                      client.Timeout     = Timeout.InfiniteTimeSpan;
                                    })
                     .ConfigurePrimaryHttpMessageHandler(sp => CreateHandler(options,
                                                                             sp.GetRequiredService<ILogger<BrokerClient>>()))
                     .AddHttpMessageHandler<RetryHandler>()
                     .RemoveAllLoggers();
    serviceCollection.AddTransient<RetryHandler>();
    serviceCollection.AddSingleton<BrokerClient>();
    serviceCollection.AddSingleton<IPullQueueStorage, PullQueueStorage>();
    serviceCollection.AddSingleton<IPushQueueStorage, PushQueueStorage>();
  }

  private static SocketsHttpHandler CreateHandler(Broker  options,
                                                  ILogger logger)
  {
    var handler = new SocketsHttpHandler
                  {
                    EnableMultipleHttp2Connections = true,
                    KeepAlivePingDelay             = KeepAlivePingPeriod,
                    KeepAlivePingTimeout           = KeepAlivePingPeriod,
                    KeepAlivePingPolicy            = HttpKeepAlivePingPolicy.Always,
                    ConnectTimeout                 = ConnectTimeout,
                  };

    if (!string.IsNullOrEmpty(options.ClientCertificateFile))
    {
      handler.SslOptions.ClientCertificates = new X509CertificateCollection
                                              {
                                                X509Certificate2.CreateFromPemFile(options.ClientCertificateFile,
                                                                                   string.IsNullOrEmpty(options.ClientKeyFile)
                                                                                     ? null
                                                                                     : options.ClientKeyFile),
                                              };
    }

    if (!string.IsNullOrEmpty(options.CaFile))
    {
      handler.SslOptions.RemoteCertificateValidationCallback = CertificateValidator.CreateCallback(options.CaFile,
                                                                                                  options.AllowHostMismatch,
                                                                                                  logger);
    }

    return handler;
  }
}
