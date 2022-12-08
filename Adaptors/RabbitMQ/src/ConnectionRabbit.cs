// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
//   D. Brasseur       <dbrasseur@aneo.fr>
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

using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Utils;

using JetBrains.Annotations;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using RabbitMQ.Client;

using System;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;

namespace ArmoniK.Core.Adapters.RabbitMQ;

[UsedImplicitly]
public class ConnectionRabbit : IConnectionRabbit
{
  private readonly AsyncLazy connectionTask_;
  private readonly ILogger<ConnectionRabbit> logger_;

  private readonly Amqp options_;

  private bool isInitialized_;

  public ConnectionRabbit(Amqp options,
                          ILogger<ConnectionRabbit> logger)
  {
    logger_ = logger;
    options_ = options;
    connectionTask_ = new AsyncLazy(() => InitTask(this));
  }

  public IModel? Channel { get; private set; }

  public IConnection? Connection { get; private set; }

  public async Task Init(CancellationToken cancellationToken = default)
  => await connectionTask_;

  public async Task InitTask(ConnectionRabbit conn, CancellationToken cancellationToken = default)
  {
    var factory = new ConnectionFactory
    {
      UserName = conn.options_.User,
      Password = conn.options_.Password,
      HostName = conn.options_.Host,
      Port = conn.options_.Port,
      DispatchConsumersAsync = true,
    };


    if (options_.Scheme.Equals("AMQPS"))
    {
      factory.Ssl.Enabled = true;
      factory.Ssl.ServerName = conn.options_.Host;
      factory.Ssl.CertificateValidationCallback = delegate (object _,
                                                           X509Certificate? _,
                                                           X509Chain? _,
                                                           SslPolicyErrors errors)
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
        conn.Connection.ConnectionShutdown += (obj,
                                           ea) => OnShutDown(obj,
                                                             ea,
                                                             "Connection",
                                                             logger_);

        Channel = conn.Connection.CreateModel();
        Channel.ModelShutdown += (obj,
                                  ea) => OnShutDown(obj,
                                                    ea,
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

  public Task<HealthCheckResult> Check(HealthCheckTag tag)
  {
    if (!isInitialized_)
    {
      return Task.FromResult(tag != HealthCheckTag.Liveness
                               ? HealthCheckResult.Degraded($"{nameof(ConnectionRabbit)} is not yet initialized.")
                               : HealthCheckResult.Unhealthy($"{nameof(ConnectionRabbit)} is not yet initialized."));
    }

    if (Connection is null || !Connection.IsOpen || Channel is null || Channel.IsClosed)
    {
      return Task.FromResult(HealthCheckResult.Unhealthy("Rabbit connection dropped."));
    }

    return Task.FromResult(HealthCheckResult.Healthy());
  }

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

  private static void OnShutDown(object? obj,
                                 ShutdownEventArgs ea,
                                 string model,
                                 ILogger logger)
  {
    if (ea.Cause is null)
    {
      logger.LogInformation($"RabbitMQ {model} closed with no error");
    }
    else
    {
      logger.LogWarning($"RabbitMQ {model} closed with error: {0}",
                        ea.Cause);
    }
  }
}
