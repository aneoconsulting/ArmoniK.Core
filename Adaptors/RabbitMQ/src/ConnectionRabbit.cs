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
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.QueueCommon;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Utils;

using JetBrains.Annotations;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using RabbitMQ.Client;

namespace ArmoniK.Core.Adapters.RabbitMQ;

[UsedImplicitly]
public class ConnectionRabbit : IConnectionRabbit
{
  private readonly AsyncLazy                 connectionTask_;
  private readonly ILogger<ConnectionRabbit> logger_;

  private readonly Amqp options_;

  private bool isInitialized_;

  public ConnectionRabbit(Amqp                      options,
                          ILogger<ConnectionRabbit> logger)
  {
    logger_         = logger;
    options_        = options;
    connectionTask_ = new AsyncLazy(() => InitTask(this));
  }

  private IConnection? Connection { get; set; }

  public IModel? Channel { get; private set; }

  public async Task Init(CancellationToken cancellationToken = default)
    => await connectionTask_;

  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => tag switch
       {
         HealthCheckTag.Startup or HealthCheckTag.Readiness => Task.FromResult(isInitialized_
                                                                                 ? HealthCheckResult.Healthy()
                                                                                 : HealthCheckResult.Unhealthy($"{nameof(ConnectionRabbit)} is not yet initialized.")),
         HealthCheckTag.Liveness => Task.FromResult(isInitialized_ && Connection is not null && Connection.IsOpen && Channel is not null && Channel.IsOpen
                                                      ? HealthCheckResult.Healthy()
                                                      : HealthCheckResult.Unhealthy($"{nameof(ConnectionRabbit)} not initialized or connection dropped.")),
         _ => throw new ArgumentOutOfRangeException(nameof(tag),
                                                    tag,
                                                    null),
       };

  public void Dispose()
  {
    if (isInitialized_)
    {
      Channel!.Close();
      Connection!.Close();

      Channel.Dispose();
      Connection.Dispose();
    }

    GC.SuppressFinalize(this);
  }

  private async Task InitTask(ConnectionRabbit  conn,
                              CancellationToken cancellationToken = default)
  {
    var factory = new ConnectionFactory
                  {
                    UserName               = conn.options_.User,
                    Password               = conn.options_.Password,
                    HostName               = conn.options_.Host,
                    Port                   = conn.options_.Port,
                    DispatchConsumersAsync = true,
                  };


    if (options_.Scheme.Equals("AMQPS"))
    {
      factory.Ssl.Enabled    = true;
      factory.Ssl.ServerName = conn.options_.Host;
      factory.Ssl.CertificateValidationCallback = delegate(object           _,
                                                           X509Certificate? _,
                                                           X509Chain?       _,
                                                           SslPolicyErrors  errors)
                                                  {
                                                    switch (errors)
                                                    {
                                                      case SslPolicyErrors.RemoteCertificateNameMismatch when conn.options_.AllowHostMismatch:
                                                      case SslPolicyErrors.None:
                                                        return true;
                                                      default:
                                                        logger_.LogError("SSL error : {error}",
                                                                         errors);
                                                        return false;
                                                    }
                                                  };
    }

    var retry = 0;
    for (; retry < conn.options_.MaxRetries; retry++)
    {
      try
      {
        conn.Connection = factory.CreateConnection();
        conn.Connection.ConnectionShutdown += (_,
                                               ea) => OnShutDown(ea,
                                                                 "Connection",
                                                                 logger_);

        Channel = conn.Connection.CreateModel();
        Channel.ModelShutdown += (_,
                                  ea) => OnShutDown(ea,
                                                    "Channel",
                                                    logger_);
        break;
      }
      catch (Exception ex)
      {
        logger_.LogInformation(ex,
                               "Retrying to create connection");
        await Task.Delay(1000 * retry,
                         cancellationToken)
                  .ConfigureAwait(false);
      }
    }

    if (retry == conn.options_.MaxRetries)
    {
      throw new TimeoutException($"{nameof(conn.options_.MaxRetries)} reached");
    }

    conn.isInitialized_ = true;
  }

  private static void OnShutDown(ShutdownEventArgs ea,
                                 string            model,
                                 ILogger           logger)
  {
    if (ea.Cause is null)
    {
      logger.LogInformation("RabbitMQ {model} closed with no error",
                            model);
    }
    else
    {
      logger.LogWarning("RabbitMQ {model} closed with error: {error}",
                        model,
                        ea.Cause);
    }
  }
}
