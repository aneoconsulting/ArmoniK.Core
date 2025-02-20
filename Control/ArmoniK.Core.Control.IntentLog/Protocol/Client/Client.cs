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
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Core.Control.IntentLog.Protocol.Messages;
using ArmoniK.Utils;

using JetBrains.Annotations;

using Action = ArmoniK.Core.Control.IntentLog.Protocol.Messages.Action;

namespace ArmoniK.Core.Control.IntentLog.Protocol.Client;

[PublicAPI]
public class Client<T> : IDisposable, IAsyncDisposable
  where T : class
{
  private readonly CancellationTokenSource                               cts_;
  private readonly Task                                                  eventLoop_;
  private readonly Channel<(Request<T>, TaskCompletionSource<Response>)> requests_;
  private          int                                                   nextId_;

  [PublicAPI]
  public Client(Stream            stream,
                CancellationToken cancellationToken = default)
  {
    cts_       = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    requests_  = Channel.CreateBounded<(Request<T>, TaskCompletionSource<Response>)>(1);
    eventLoop_ = Task.Run(EventLoop);

    return;

    async Task EventLoop()
    {
      using var       cts = cts_;
      await using var str = stream;

      // ReSharper disable once VariableHidesOuterVariable
      var cancellationToken = cts_.Token;

      var nextRequest  = NextRequest();
      var nextResponse = NextResponse();

      var mapping = new Dictionary<int, Queue<TaskCompletionSource<Response>>>();

      while (!cancellationToken.IsCancellationRequested)
      {
        try
        {
          var task = await Task.WhenAny(nextRequest,
                                        nextResponse)
                               .ConfigureAwait(false);
          nextRequest = NextRequest();

          if (ReferenceEquals(task,
                              nextRequest))
          {
            var (request, responseChannel) = await nextRequest.ConfigureAwait(false);

            try
            {
              await request.SendAsync(str,
                                      cancellationToken)
                           .ConfigureAwait(false);

              if (!mapping.TryGetValue(request.IntentId,
                                       out var queue))
              {
                queue                     = new Queue<TaskCompletionSource<Response>>();
                mapping[request.IntentId] = queue;
              }

              queue.Enqueue(responseChannel);
            }
            catch (Exception ex)
            {
              responseChannel.TrySetException(ex);
            }
          }
          else
          {
            var response = await nextResponse.ConfigureAwait(false);
            nextResponse = NextResponse();

            if (mapping.TryGetValue(response.IntentId,
                                    out var queue) && queue.TryDequeue(out var tcs))
            {
              tcs.TrySetResult(response);
            }
            // TODO: else: log
          }
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
    }

    Task<Response> NextResponse()
      => Response.ReceiveAsync(stream,
                               cts_.Token);

    Task<(Request<T>, TaskCompletionSource<Response>)> NextRequest()
      => requests_.Reader.ReadAsync(cts_.Token)
                  .AsTask();
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
      await eventLoop_.ConfigureAwait(false);
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
  public async Task<Intent<T>> OpenAsync(T?                obj,
                                         CancellationToken cancellationToken = default)
  {
    var id = Interlocked.Add(ref nextId_,
                             1);
    await Call(new Request<T>

               {
                 IntentId = id,
                 Action   = Action.Open,
                 Payload  = obj,
               },
               cancellationToken)
      .ConfigureAwait(false);

    return new Intent<T>(this,
                         id);
  }

  internal async Task Call(Request<T>        request,
                           CancellationToken cancellationToken = default)
  {
    var tcs = new TaskCompletionSource<Response>();
    await requests_.Writer.WriteAsync((request, tcs),
                                      cancellationToken)
                   .ConfigureAwait(false);

    await using var reg = cancellationToken.Register(() => tcs.TrySetCanceled(cancellationToken));

    var response = await tcs.Task.ConfigureAwait(false);

    switch (response.Error)
    {
      case null:
        break;
      case Exception e:
        throw e;
      default:
        throw new Exception($"Unknown response: {response}");
    }
  }

  [PublicAPI]
  public static async Task<Client<T>> ConnectAsync(string            host,
                                                   int               port,
                                                   Options?          options           = null,
                                                   CancellationToken cancellationToken = default)
  {
    _ = options;

    var socket = new Socket(SocketType.Stream,
                            ProtocolType.Tcp);

    NetworkStream stream;

    try
    {
      await socket.ConnectAsync(host,
                                port,
                                cancellationToken)
                  .ConfigureAwait(false);

      stream = new NetworkStream(socket,
                                 true);
    }
    catch
    {
      socket.Dispose();
      throw;
    }

    return new Client<T>(stream,
                         cancellationToken);
  }

  public class Options
  {
  }
}
