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
using System.Threading;
using System.Threading.Tasks;

using Amqp;
using Amqp.Framing;

using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Utils;
using ArmoniK.Utils;

using JetBrains.Annotations;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Amqp;

[UsedImplicitly]
public class ConnectionAmqp : IConnectionAmqp
{
  private readonly ExecutionSingleizer<Connection> connectionSingleizer_ = new();
  private readonly ILogger<ConnectionAmqp>         logger_;
  private readonly QueueCommon.Amqp                options_;
  private          Connection?                     connection_;

  public ConnectionAmqp(QueueCommon.Amqp        options,
                        ILogger<ConnectionAmqp> logger)
  {
    options_ = options;
    logger_  = logger;
  }

  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => tag switch
       {
         HealthCheckTag.Startup or HealthCheckTag.Readiness => Task.FromResult(connection_ is not null
                                                                                 ? HealthCheckResult.Healthy()
                                                                                 : HealthCheckResult.Unhealthy($"{nameof(ConnectionAmqp)} is not yet initialized.")),
         HealthCheckTag.Liveness => Task.FromResult(connection_ is not null && connection_.ConnectionState == ConnectionState.Opened
                                                      ? HealthCheckResult.Healthy()
                                                      : HealthCheckResult.Unhealthy($"{nameof(ConnectionAmqp)} not initialized or connection dropped.")),
         _ => throw new ArgumentOutOfRangeException(nameof(tag),
                                                    tag,
                                                    null),
       };

  public Task Init(CancellationToken cancellationToken = default)
    => GetConnectionAsync(cancellationToken);

  public async Task<Connection> GetConnectionAsync(CancellationToken cancellationToken = default)
  {
    if (connection_ is not null && !connection_.IsClosed)
    {
      return connection_;
    }

    return await connectionSingleizer_.Call(async token =>
                                            {
                                              // this is needed to resolve TOCTOU problem
                                              if (connection_ is not null && !connection_.IsClosed)
                                              {
                                                return connection_;
                                              }

                                              var conn = await CreateConnection(options_,
                                                                                logger_,
                                                                                token)
                                                           .ConfigureAwait(false);
                                              connection_ = conn;
                                              return conn;
                                            },
                                            cancellationToken)
                                      .ConfigureAwait(false);
  }

  private static async Task<Connection> CreateConnection(QueueCommon.Amqp  options,
                                                         ILogger           logger,
                                                         CancellationToken cancellationToken = default)
  {
    var address = new Address(options.Host,
                              options.Port,
                              options.User,
                              options.Password,
                              scheme: options.Scheme);

    var connectionFactory = new ConnectionFactory();
    if (options.Scheme.Equals("AMQPS"))
    {
      if (options.Ssl && !string.IsNullOrEmpty(options.CaPath))
      {
        connectionFactory.SSL.RemoteCertificateValidationCallback = CertificateValidator.CreateCallback(options.CaPath,
                                                                                                        options.AllowInsecureTls,
                                                                                                        logger);
      }
      else if (!options.Ssl)
      {
        logger.LogWarning("SSL is disabled for ActiveMQ but the scheme is {scheme}",
                          options.Scheme);
      }
      else
      {
        logger.LogDebug("No CA path provided for ActiveMQ");
      }
    }
    else
    {
      logger.LogWarning("SSL is disabled for ActiveMQ: {options.Ssl}",
                        options.Ssl);
      logger.LogWarning("Scheme is {scheme}",
                        options.Scheme);
    }

    var retry = 0;
    for (; retry < options.MaxRetries; retry++)
    {
      try
      {
        var connection = await connectionFactory.CreateAsync(address)
                                                .ConfigureAwait(false);
        connection.AddClosedCallback((_,
                                      e) => OnCloseConnection(e,
                                                              logger));

        return connection;
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

  private static void OnCloseConnection(Error?  error,
                                        ILogger logger)
  {
    if (error is null)
    {
      logger.LogInformation("AMQP Connection closed with no error");
    }
    else
    {
      logger.LogWarning("AMQP Connection closed with error: {error}",
                        error.ToString());
    }
  }
}
