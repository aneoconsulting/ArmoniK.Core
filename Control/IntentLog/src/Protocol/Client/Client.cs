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

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace ArmoniK.Core.Control.IntentLog.Protocol.Client;

[PublicAPI]
public class Client : IDisposable, IAsyncDisposable
{
  private readonly CancellationTokenSource                            cts_;
  private readonly Task                                               eventLoop_;
  private readonly ILogger                                            logger_;
  private readonly Channel<(Request, TaskCompletionSource<Response>)> requests_;

  [PublicAPI]
  public Client(Stream            stream,
                ILogger<Client>?  logger            = null,
                CancellationToken cancellationToken = default)
  {
    cts_      = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    requests_ = Channel.CreateBounded<(Request, TaskCompletionSource<Response>)>(1);
    logger_   = logger ?? NullLogger<Client>.Instance;
    eventLoop_ = Task.Factory.StartNew(EventLoop,
                                       TaskCreationOptions.LongRunning)
                     .Unwrap();

    return;

    async Task EventLoop()
    {
      using var       cts = cts_;
      await using var str = stream;

      // ReSharper disable once VariableHidesOuterVariable
      var cancellationToken = cts_.Token;

      var nextRequest  = NextRequest();
      var nextResponse = NextResponse();

      var        mapping   = new Dictionary<Guid, Queue<TaskCompletionSource<Response>>>();
      Exception? exception = null;

      while (!cancellationToken.IsCancellationRequested)
      {
        try
        {
          var task = await Task.WhenAny(nextRequest,
                                        nextResponse)
                               .ConfigureAwait(false);

          if (ReferenceEquals(task,
                              nextRequest))
          {
            var (request, responseChannel) = await nextRequest.ConfigureAwait(false);
            nextRequest                    = NextRequest();

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

            logger_.LogDebug("Client received response intent {IntentId}:{IntentStatus} -> {@IntentError}",
                             response.IntentId,
                             response.Type,
                             response.Payload);

            nextResponse = NextResponse();

            // ReSharper disable once SwitchStatementHandlesSomeKnownEnumValuesWithDefault
            switch (response.Type)
            {
              case ResponseType.Ping:
                await new Request
                  {
                    IntentId = response.IntentId,
                    Type     = RequestType.Pong,
                    Payload  = response.Payload,
                  }.SendAsync(str,
                              cancellationToken)
                   .ConfigureAwait(false);
                break;
              case ResponseType.Pong:
                break;
              default:

                if (mapping.TryGetValue(response.IntentId,
                                        out var queue) && queue.TryDequeue(out var tcs))
                {
                  tcs.TrySetResult(response);
                }
                else
                {
                  logger_.LogError("Client error: Received incorrect intent ID from server: {IntentId}",
                                   response.IntentId);
                }

                break;
            }
          }
        }
        catch (Exception ex)
        {
          exception = ex;

          if (!cancellationToken.IsCancellationRequested && ex is not EndOfStreamException and not IOException
                                                                                                   {
                                                                                                     InnerException: SocketException
                                                                                                                     {
                                                                                                                       SocketErrorCode: SocketError.ConnectionReset or
                                                                                                                                        SocketError.NetworkReset or
                                                                                                                                        SocketError.HostDown or
                                                                                                                                        SocketError.TimedOut,
                                                                                                                     },
                                                                                                   })
          {
            logger_.LogError(ex,
                             "Client error");

            await cts_.CancelAsync()
                      .ConfigureAwait(false);
          }

          break;
        }
      }

      foreach (var (_, queue) in mapping)
      {
        foreach (var tcs in queue)
        {
          tcs.TrySetException(exception ?? new OperationCanceledException(cancellationToken));
        }
      }
    }

    Task<Response> NextResponse()
      => Response.ReceiveAsync(stream,
                               cts_.Token);

    Task<(Request, TaskCompletionSource<Response>)> NextRequest()
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
    catch (Exception ex)
    {
      logger_.LogError(ex,
                       "Client error while disposing");
    }

    cts_.Dispose();
  }


  public void Dispose()
    => DisposeAsync()
      .WaitSync();

  [PublicAPI]
  public async Task<Intent> OpenAsync(byte[]            payload,
                                      CancellationToken cancellationToken = default)
  {
    var id = new Guid();
    await Call(new Request

               {
                 IntentId = id,
                 Type     = RequestType.Open,
                 Payload  = payload,
               },
               cancellationToken)
      .ConfigureAwait(false);

    return new Intent(this,
                      logger_,
                      id);
  }

  internal async Task Call(Request           request,
                           CancellationToken cancellationToken = default)
  {
    logger_.LogDebug("Calling intent: {IntentId}:{IntentAction}:{@IntentPayload}",
                     request.IntentId,
                     request.Type,
                     request.Payload);
    var tcs = new TaskCompletionSource<Response>();
    await requests_.Writer.WriteAsync((request, tcs),
                                      cancellationToken)
                   .ConfigureAwait(false);

    await using var reg = cancellationToken.Register(() => tcs.TrySetCanceled(cancellationToken));

    var response = await tcs.Task.ConfigureAwait(false);

    logger_.LogDebug("Called intent: {IntentId}:{IntentAction}:{@IntentPayload} -> {@IntentError}",
                     request.IntentId,
                     request.Type,
                     request.Payload,
                     response.Payload);

    switch (response.Payload)
    {
      case null:
        break;
      default:
        throw new Exception($"Unknown response: {response.Payload}");
    }
  }

  [PublicAPI]
  public static async Task<Client> ConnectAsync(string            host,
                                                int               port,
                                                Options?          options           = null,
                                                ILogger<Client>?  logger            = null,
                                                CancellationToken cancellationToken = default)
  {
    _ = options;

    var socket = new Socket(SocketType.Stream,
                            ProtocolType.Tcp);
    socket.SetSocketOption(SocketOptionLevel.Socket,
                           SocketOptionName.ReuseAddress,
                           true);


    logger?.LogInformation("Client created {@Endpoint}",
                           socket.LocalEndPoint);

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

    return new Client(stream,
                      logger,
                      cancellationToken);
  }

  public class Options
  {
  }
}
