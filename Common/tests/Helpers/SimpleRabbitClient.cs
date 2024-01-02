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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.RabbitMQ;
using ArmoniK.Core.Base.DataStructures;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using RabbitMQ.Client;

namespace ArmoniK.Core.Common.Tests.Helpers;

public class SimpleRabbitClient : IConnectionRabbit
{
  private readonly ILoggerFactory loggerFactory_;
  private          IConnection?   connection_;

  public SimpleRabbitClient()
  {
    loggerFactory_ = new LoggerFactory();
    loggerFactory_.AddProvider(new ConsoleForwardingLoggerProvider());
  }

  public IModel? Channel { get; private set; }

  public void Dispose()
  {
    if (connection_ is not null && connection_.IsOpen)
    {
      connection_.Close();
    }

    loggerFactory_.Dispose();
    GC.SuppressFinalize(this);
  }

  public Task Init(CancellationToken cancellation)
  {
    var connectionFactory = new ConnectionFactory
                            {
                              Uri                    = new Uri("amqp://guest:guest@localhost:5672"),
                              DispatchConsumersAsync = true,
                            };

    connection_ = connectionFactory.CreateConnection();
    Channel     = connection_!.CreateModel();

    return Task.CompletedTask;
  }

  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(connection_ is not null && connection_.IsOpen && Channel is not null && Channel.IsOpen
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy());
}
