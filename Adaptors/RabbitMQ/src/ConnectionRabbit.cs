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
  private readonly ExecutionSingleizer<IModel> connectionSingleizer_ = new();
  private readonly ConnectionFactory           factory_;
  private readonly ILogger<ConnectionRabbit>   logger_;
  private readonly Amqp                        options_;
  private          IModel?                     model_;

  public ConnectionRabbit(Amqp                      options,
                          ILogger<ConnectionRabbit> logger,
                          SslOption?                sslOption)
  {
    logger_  = logger;
    options_ = options;
    factory_ = new ConnectionFactory
               {
                 UserName               = options.User,
                 Password               = options.Password,
                 HostName               = options.Host,
                 Port                   = options.Port,
                 DispatchConsumersAsync = true,
                 Ssl                    = sslOption
               };
  }

  public Task Init(CancellationToken cancellationToken = default)
    => GetConnectionAsync(cancellationToken);

  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => tag switch
       {
         HealthCheckTag.Startup or HealthCheckTag.Readiness => Task.FromResult(model_ is not null
                                                                                 ? HealthCheckResult.Healthy()
                                                                                 : HealthCheckResult.Unhealthy($"{nameof(ConnectionRabbit)} is not yet initialized.")),
         HealthCheckTag.Liveness => Task.FromResult(model_ is not null && model_.IsOpen
                                                      ? HealthCheckResult.Healthy()
                                                      : HealthCheckResult.Unhealthy($"{nameof(ConnectionRabbit)} not initialized or connection dropped.")),
         _ => throw new ArgumentOutOfRangeException(nameof(tag),
                                                    tag,
                                                    null),
       };

  public void Dispose()
  {
    model_?.Close();
    model_?.Dispose();

    connectionSingleizer_.Dispose();

    GC.SuppressFinalize(this);
  }


  public async Task<IModel> GetConnectionAsync(CancellationToken cancellationToken = default)
  {
    if (model_ is not null && !model_.IsClosed)
    {
      return model_;
    }

    return await connectionSingleizer_.Call(async token =>
                                            {
                                              // this is needed to resolve TOCTOU problem
                                              if (model_ is not null && !model_.IsClosed)
                                              {
                                                return model_;
                                              }

                                              var conn = await CreateConnection(options_,
                                                                                factory_,
                                                                                logger_,
                                                                                token)
                                                           .ConfigureAwait(false);
                                              model_ = conn;
                                              return conn;
                                            },
                                            cancellationToken)
                                      .ConfigureAwait(false);
  }

  private static async Task<IModel> CreateConnection(Amqp              options,
                                                     ConnectionFactory factory,
                                                     ILogger           logger,
                                                     CancellationToken cancellationToken = default)
  {
    if (options.Scheme.Equals("AMQPS"))
    {
      factory.Ssl.Enabled    = true;
      factory.Ssl.ServerName = options.Host;
      factory.Ssl.CertificateValidationCallback = delegate(object           _,
                                                           X509Certificate? _,
                                                           X509Chain?       _,
                                                           SslPolicyErrors  errors)
                                                  {
                                                    switch (errors)
                                                    {
                                                      case SslPolicyErrors.RemoteCertificateNameMismatch when options.AllowHostMismatch:
                                                      case SslPolicyErrors.None:
                                                        return true;
                                                      default:
                                                        logger.LogError("SSL error : {error}",
                                                                        errors);
                                                        return false;
                                                    }
                                                  };
    }

    var retry = 0;
    for (; retry < options.MaxRetries; retry++)
    {
      try
      {
        var connection = factory.CreateConnection();
        connection.ConnectionShutdown += (_,
                                          ea) => OnShutDown(ea,
                                                            "Connection",
                                                            logger);

        var model = connection.CreateModel();
        model.ModelShutdown += (_,
                                ea) => OnShutDown(ea,
                                                  "Channel",
                                                  logger);
        return model;
      }
      catch (Exception ex)
      {
        logger.LogInformation(ex,
                              "Retrying to create connection");
        await Task.Delay(1000 * retry,
                         cancellationToken)
                  .ConfigureAwait(false);
      }
    }

    throw new TimeoutException($"{nameof(options.MaxRetries)} reached");
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
