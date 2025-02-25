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

using ArmoniK.Core.Control.IntentLog.Protocol.Client;
using ArmoniK.Core.Control.IntentLog.Protocol.Server;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Control.IntentLog;

internal class ServerHandler : IServerHandler
{
  public Task OpenAsync(Connection        connection,
                        int               intentId,
                        byte[]            payload,
                        CancellationToken cancellationToken = default)
  {
    Console.WriteLine($"Opening intent: {intentId} with payload: {payload}");
    return Task.CompletedTask;
  }

  public Task AmendAsync(Connection        connection,
                         int               intentId,
                         byte[]            payload,
                         CancellationToken cancellationToken = default)
  {
    Console.WriteLine($"Amending intent: {intentId} with payload: {payload}");
    return Task.CompletedTask;
  }

  public Task CloseAsync(Connection        connection,
                         int               intentId,
                         byte[]            payload,
                         CancellationToken cancellationToken = default)
  {
    Console.WriteLine($"Closing intent: {intentId} with payload: {payload}");
    return Task.CompletedTask;
  }

  public Task AbortAsync(Connection        connection,
                         int               intentId,
                         byte[]            payload,
                         CancellationToken cancellationToken = default)
  {
    Console.WriteLine($"Aborting intent: {intentId} with payload: {payload}");
    return Task.CompletedTask;
  }

  public Task TimeoutAsync(Connection        connection,
                           int               intentId,
                           byte[]            payload,
                           CancellationToken cancellationToken = default)
  {
    Console.WriteLine($"Timeout intent: {intentId} with payload: {payload}");
    return Task.CompletedTask;
  }

  public Task ResetAsync(Connection        connection,
                         int               intentId,
                         byte[]            payload,
                         CancellationToken cancellationToken = default)
  {
    Console.WriteLine($"Reset intent: {intentId} with payload: {payload}");
    return Task.CompletedTask;
  }
}

internal class Program
{
  private static void Usage()
  {
    Console.Error.WriteLine("Usage: dotnet run [client|server]");
    Environment.Exit(1);
  }

  private static async Task RunClient(ILoggerFactory loggerFactory)
  {
    Console.WriteLine("Starting client...");
    await using var client = await Client.ConnectAsync("localhost",
                                                       1337,
                                                       logger: loggerFactory.CreateLogger<Client>());

    Console.WriteLine("Client connected.");

    await using var intent = await client.OpenAsync("pouet"u8.ToArray())
                                         .ConfigureAwait(false);
    intent.AbortOnDispose("Abort"u8.ToArray());
    Console.WriteLine("Opened intent");

    await intent.AmendAsync("plop"u8.ToArray())
                .ConfigureAwait(false);
    Console.WriteLine("Amended intent");
  }

  private static async Task RunServer(ILoggerFactory loggerFactory)
  {
    await using var server = new Server(new ServerHandler(),
                                        new Server.Options
                                        {
                                          Endpoint = "localhost",
                                          Port     = 1337,
                                        },
                                        loggerFactory.CreateLogger<Server>());
    Console.WriteLine("Listening on port 1337");
    await Task.Delay(Timeout.Infinite)
              .ConfigureAwait(false);
  }

  private static async Task Main(string[] args)
  {
    if (args.Length == 0)
    {
      Usage();
    }

    var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole()
                                                               .SetMinimumLevel(LogLevel.Debug));

    switch (args[0])
    {
      case "client":
        await RunClient(loggerFactory)
          .ConfigureAwait(false);
        break;
      case "server":
        await RunServer(loggerFactory)
          .ConfigureAwait(false);
        break;
    }
  }
}
