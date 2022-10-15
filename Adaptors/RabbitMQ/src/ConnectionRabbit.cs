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

using System;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Injection.Options;

using JetBrains.Annotations;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using RabbitMQ.Client;

using TimeoutException = ArmoniK.Core.Common.Exceptions.TimeoutException;

namespace ArmoniK.Core.Adapters.RabbitMQ;

[UsedImplicitly]
public class ConnectionRabbit : IConnectionRabbit
{
  private readonly ILogger<ConnectionRabbit> logger_;

  private readonly Amqp options_;

  private IConnection? connection_;
  private bool         isInitialized_;

  public ConnectionRabbit(Amqp                      options,
                          ILogger<ConnectionRabbit> logger)
  {
    logger_  = logger;
    options_ = options;
  }

  public IModel? Channel { get; private set; }

  public async Task Init(CancellationToken cancellationToken)
  {
    var factory = new ConnectionFactory
                  {
                    UserName               = options_.User,
                    Password               = options_.Password,
                    HostName               = options_.Host,
                    Port                   = options_.Port,
                    DispatchConsumersAsync = true,
                  };

    var retry = 0;
    for (; retry < options_.MaxRetries; retry++)
    {
      try
      {
        connection_ = factory.CreateConnection();
        Channel     = connection_.CreateModel();
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

    if (retry == options_.MaxRetries)
    {
      throw new TimeoutException($"{nameof(options_.MaxRetries)} reached");
    }

    isInitialized_ = Channel is not null && Channel.IsOpen;
  }

  public Task<HealthCheckResult> Check(HealthCheckTag tag)
  {
    if (!isInitialized_)
    {
      return Task.FromResult(tag != HealthCheckTag.Liveness
                               ? HealthCheckResult.Degraded($"{nameof(ConnectionRabbit)} is not yet initialized.")
                               : HealthCheckResult.Unhealthy($"{nameof(ConnectionRabbit)} is not yet initialized."));
    }

    if (connection_ is null || !connection_.IsOpen)
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
      connection_!.Close();

      Channel.Dispose();
      connection_.Dispose();
    }

    GC.SuppressFinalize(this);
  }
}
