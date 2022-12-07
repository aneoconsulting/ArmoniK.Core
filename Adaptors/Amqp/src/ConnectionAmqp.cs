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

using Amqp;
using Amqp.Framing;

using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Utils;

using JetBrains.Annotations;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Amqp;

[UsedImplicitly]
public class ConnectionAmqp : IConnectionAmqp
{
  private readonly AsyncLazy                     connectionTask_;
  private readonly ILogger<ConnectionAmqp>       logger_;
  private readonly Common.Injection.Options.Amqp options_;
  private          bool                          isInitialized_;

  public ConnectionAmqp(Common.Injection.Options.Amqp options,
                        ILogger<ConnectionAmqp>       logger)
  {
    options_        = options;
    logger_         = logger;
    connectionTask_ = new AsyncLazy(() => InitTask(this));
  }

  public Connection? Connection { get; private set; }

  public Task<HealthCheckResult> Check(HealthCheckTag tag)
  {
    if (!isInitialized_)
    {
      return Task.FromResult(tag != HealthCheckTag.Liveness
                               ? HealthCheckResult.Degraded($"{nameof(ConnectionAmqp)} is not yet initialized.")
                               : HealthCheckResult.Unhealthy($"{nameof(ConnectionAmqp)} is not yet initialized."));
    }

    if (Connection is null || Connection.ConnectionState != ConnectionState.Opened)
    {
      return Task.FromResult(HealthCheckResult.Unhealthy("Amqp connection dropped."));
    }

    return Task.FromResult(HealthCheckResult.Healthy());
  }

  public async Task Init(CancellationToken cancellationToken = default)
    => await connectionTask_;

  private static async Task InitTask(ConnectionAmqp    conn,
                                     CancellationToken cancellationToken = default)
  {
    conn.logger_.LogInformation("Get address for session");
    var address = new Address(conn.options_.Host,
                              conn.options_.Port,
                              conn.options_.User,
                              conn.options_.Password,
                              scheme: conn.options_.Scheme);

    var connectionFactory = new ConnectionFactory();
    if (conn.options_.Scheme.Equals("AMQPS"))
    {
      connectionFactory.SSL.RemoteCertificateValidationCallback = delegate(object           _,
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
                                                                        conn.logger_.LogError("SSL error : {error}",
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
        conn.Connection = await connectionFactory.CreateAsync(address)
                                                 .ConfigureAwait(false);
        conn.Connection.AddClosedCallback((x,
                                           e) => OnCloseConnection(x,
                                                                   e,
                                                                   conn.logger_));
        break;
      }
      catch (Exception ex)
      {
        conn.logger_.LogInformation(ex,
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

  private static void OnCloseConnection(IAmqpObject sender,
                                        Error?      error,
                                        ILogger     logger)
  {
    if (error == null)
    {
      logger.LogInformation("AMQP Connection closed with no error");
    }
    else
    {
      logger.LogWarning("AMQP Connection closed with error: {0}",
                        error.ToString());
    }
  }
}
