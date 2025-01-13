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

using ArmoniK.Core.Adapters.Amqp;
using ArmoniK.Core.Base.DataStructures;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Tests.Helpers;

public class SimpleAmqpClient : IConnectionAmqp, IAsyncDisposable
{
  private readonly Address           address_;
  private readonly ConnectionFactory connectionFactory_;
  private readonly ILoggerFactory    loggerFactory_;

  private Connection? connection_;
  private bool        isInitialized_;

  public SimpleAmqpClient()
  {
    loggerFactory_ = new LoggerFactory();
    loggerFactory_.AddProvider(new ConsoleForwardingLoggerProvider());

    address_ = new Address("amqp://guest:guest@localhost:5672");

    connectionFactory_ = new ConnectionFactory();
  }

  public async ValueTask DisposeAsync()
  {
    if (connection_ is not null && connection_.ConnectionState == ConnectionState.Opened)
    {
      await connection_.CloseAsync()
                       .ConfigureAwait(false);
    }

    loggerFactory_.Dispose();
    GC.SuppressFinalize(this);
  }

  public async Task Init(CancellationToken cancellation)
  {
    connection_ = await connectionFactory_.CreateAsync(address_)
                                          .ConfigureAwait(false);
    isInitialized_ = true;
  }

  public Task<Connection> GetConnectionAsync(CancellationToken cancellationToken = default)
    => Task.FromResult(connection_!);

  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(isInitialized_
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy());
}
