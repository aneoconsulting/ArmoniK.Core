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
using System.Threading.Tasks;

using Amqp;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Tests.Helpers;

public class SimpleAmqpClientHelper : IAsyncDisposable
{
  private readonly Connection     connection_;
  private readonly ILoggerFactory loggerFactory_;

  public SimpleAmqpClientHelper()
  {
    loggerFactory_ = new LoggerFactory();
    loggerFactory_.AddProvider(new ConsoleForwardingLoggerProvider());

    var address = new Address("amqp://guest:guest@localhost:5672");

    connection_ = new Connection(address);
    Session     = new Session(connection_);
  }

  public Session Session { get; }

  public async ValueTask DisposeAsync()
  {
    await Session.CloseAsync()
                 .ConfigureAwait(false);
    await connection_.CloseAsync()
                     .ConfigureAwait(false);
    GC.SuppressFinalize(this);
  }
}
