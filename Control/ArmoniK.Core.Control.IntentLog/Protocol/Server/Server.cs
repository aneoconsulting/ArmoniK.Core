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
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Utils;

using JetBrains.Annotations;

namespace ArmoniK.Core.Control.IntentLog.Protocol.Server;

[PublicAPI]
public class Server<T> : IDisposable, IAsyncDisposable
  where T : class
{
  private Task acceptLoop_;

  private CancellationTokenSource cts_;

  [PublicAPI]
  public Server(IServerHandler<T> handler,
                Options?          options           = null,
                CancellationToken cancellationToken = default)
  {
    cts_ = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

    var ipHostInfo = Dns.Resolve(options?.Endpoint ?? Dns.GetHostName());
    var ipAddress  = ipHostInfo.AddressList[0];
    var localEndPoint = new IPEndPoint(ipAddress,
                                       options?.Port ?? 1337);

    var socket = new Socket(SocketType.Stream,
                            ProtocolType.Tcp);

    socket.Bind(localEndPoint);
    socket.Listen(100);

    acceptLoop_ = Task.Run(AcceptLoop);
    return;

    async Task AcceptLoop()
    {
      using var listeningSocket = socket;
      using var cts             = cts_;

      // ReSharper disable once VariableHidesOuterVariable
      var cancellationToken = cts_.Token;

      var connections = new ConcurrentDictionary<IntPtr, Connection<T>>();

      while (!cancellationToken.IsCancellationRequested)
      {
        try
        {
          NetworkStream stream;
          var accepted = await socket.AcceptAsync(cancellationToken)
                                     .ConfigureAwait(false);
          var key = accepted.Handle;

          try
          {
            stream = new NetworkStream(accepted,
                                       true);
          }
          catch
          {
            accepted.Dispose();
            throw;
          }

          Connection<T> connection;
          try
          {
            connection = new Connection<T>(handler,
                                           stream,
                                           () => connections.TryRemove(key,
                                                                       out _),
                                           cancellationToken);
          }
          catch
          {
            await stream.DisposeAsync()
                        .ConfigureAwait(false);
            throw;
          }

          connections.TryAdd(key,
                             connection);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
          // Empty on purpose
        }
        catch
        {
          // TODO: log
          await cts_.CancelAsync()
                    .ConfigureAwait(false);
          break;
        }
      }

      foreach (var (_, connection) in connections)
      {
        try
        {
          await connection.DisposeAsync()
                          .ConfigureAwait(false);
        }
        catch
        {
          // TODO: log
        }
      }
    }
  }

  public async ValueTask DisposeAsync()
  {
    try
    {
      await cts_.CancelAsync()
                .ConfigureAwait(false);
    }
    catch (ObjectDisposedException)
    {
      // Empty on purpose
    }

    try
    {
      await acceptLoop_.ConfigureAwait(false);
    }
    catch
    {
      // TODO: log
    }

    cts_.Dispose();
  }

  public void Dispose()
    => DisposeAsync()
      .WaitSync();

  [PublicAPI]
  public class Options
  {
    public string? Endpoint;
    public int?    Port;
  }
}
