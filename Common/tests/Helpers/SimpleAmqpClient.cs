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
using System.Threading;
using System.Threading.Tasks;

using Amqp;

using ArmoniK.Core.Adapters.Amqp;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Tests.Helpers;

public class SimpleAmqpClient : IConnectionAmqp, IAsyncDisposable
{
  private readonly Address           address_;
  private readonly ConnectionFactory connectionFactory_;
  private readonly ILoggerFactory    loggerFactory_;

  public SimpleAmqpClient()
  {
    loggerFactory_ = new LoggerFactory();
    loggerFactory_.AddProvider(new ConsoleForwardingLoggerProvider());

    address_ = new Address("amqp://guest:guest@localhost:5672");

    connectionFactory_ = new ConnectionFactory();
  }

  public async ValueTask DisposeAsync()
  {
    if (Connection is not null && Connection.ConnectionState == ConnectionState.Opened)
    {
      await Connection.CloseAsync()
                      .ConfigureAwait(false);
    }

    loggerFactory_.Dispose();
    GC.SuppressFinalize(this);
  }

  public Connection? Connection { get; private set; }

  public async Task Init(CancellationToken cancellation)
    => Connection = await connectionFactory_.CreateAsync(address_)
                                            .ConfigureAwait(false);

  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(HealthCheckResult.Healthy());
}