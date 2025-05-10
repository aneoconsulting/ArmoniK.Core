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

      try
      {
        while (!cancellationToken.IsCancellationRequested)
        {
          logger_.LogTrace("Waiting for a new event");
          var task = nextRequest.IsCompleted
                       ? nextRequest
                       : await Task.WhenAny(nextRequest,
                                            nextResponse)
                                   .ConfigureAwait(false);
          logger_.LogTrace("Received an event");

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
      }
      catch (Exception ex)
      {
        exception = ex;

        if (cancellationToken.IsCancellationRequested)
        {
          logger_.LogTrace("Client stopping: cancellation requested");
        }
        else if (ex is EndOfStreamException and not IOException
                                                    {
                                                      InnerException: SocketException
                                                                      {
                                                                        SocketErrorCode: SocketError.ConnectionReset or SocketError.NetworkReset or
                                                                                         SocketError.HostDown or SocketError.TimedOut,
                                                                      },
                                                    })
        {
          logger_.LogTrace(ex,
                           "Client stopping: server closed the connection");
        }
        else
        {
          logger_.LogError(ex,
                           "Client error");
        }
      }

      logger_.LogTrace("Client stopping: cancel all on-flight tasks");
      exception ??= new OperationCanceledException(cancellationToken);
      requests_.Writer.TryComplete(exception);
      await cts_.CancelAsync()
                .ConfigureAwait(false);

      // Cancel callers whose requests have already been sent
      foreach (var (id, queue) in mapping)
      {
        logger_.LogTrace("Cancel {NCallers} callers intent {IntentId}",
                         queue.Count,
                         id);
        foreach (var tcs in queue)
        {
          tcs.TrySetException(exception);
        }
      }

      // Cancel caller whose request has already been removed from channel, but not yet processed
      try
      {
        var (request, tcs) = await nextRequest.ConfigureAwait(false);
        logger_.LogTrace("Cancel caller intent {IntentId}",
                         request.IntentId);
        tcs.TrySetException(exception);
      }
      catch (Exception ex) when (ex is ChannelClosedException or OperationCanceledException)
      {
        // Empty on purpose
      }

      // Cancel callers whose requests are still pending within the queue
      while (requests_.Reader.TryRead(out var r))
      {
        var (request, tcs) = r;
        logger_.LogTrace("Cancel caller intent {IntentId}",
                         request.IntentId);
        tcs.TrySetException(exception);
      }

      logger_.LogTrace("Client stopped");
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
    var id = Guid.NewGuid();
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
    var tcs = new TaskCompletionSource<Response>(TaskCreationOptions.RunContinuationsAsynchronously);

    try
    {
      await requests_.Writer.WriteAsync((request, tcs),
                                        cancellationToken)
                     .ConfigureAwait(false);
    }
    catch (ChannelClosedException ex)
    {
      (ex.InnerException ?? ex).RethrowWithStacktrace();
    }

    logger_.LogTrace("Request sent for processing");

    await using var reg = cancellationToken.Register(() => tcs.TrySetCanceled(cancellationToken));

    var response = await tcs.Task.ConfigureAwait(false);

    logger_.LogDebug("Called intent: {IntentId}:{IntentAction}:{@IntentPayload} -> {@IntentError}",
                     request.IntentId,
                     request.Type,
                     request.Payload,
                     response.Payload);

    if (response.Type is ResponseType.Error)
    {
      throw new ServerError($"Server error for intent {request.IntentId}:{request.Type}",
                            response.Payload);
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

    if (OperatingSystem.IsWindows())
    {
      socket.SetSocketOption(SocketOptionLevel.Socket,
                             SocketOptionName.ReuseAddress,
                             true);
    }

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

public class Client<TRequest, TResponse> : IDisposable, IAsyncDisposable
  where TResponse : Exception
{
  private readonly Func<byte[], TResponse> decoder_;
  private readonly Func<TRequest, byte[]>  encoder_;
  private readonly Client                  inner_;

  [PublicAPI]
  public Client(Client                  inner,
                Func<TRequest, byte[]>  encoder,
                Func<byte[], TResponse> decoder)
  {
    inner_   = inner;
    encoder_ = encoder;
    decoder_ = decoder;
  }

  [PublicAPI]
  public Client(Stream                  stream,
                Func<TRequest, byte[]>  encoder,
                Func<byte[], TResponse> decoder,
                ILogger<Client>?        logger            = null,
                CancellationToken       cancellationToken = default)
  {
    inner_ = new Client(stream,
                        logger,
                        cancellationToken);
    encoder_ = encoder;
    decoder_ = decoder;
  }

  public ValueTask DisposeAsync()
    => inner_.DisposeAsync();

  public void Dispose()
    => inner_.Dispose();

  [PublicAPI]
  public async Task<Intent<TRequest, TResponse>> OpenAsync(TRequest          request,
                                                           CancellationToken cancellationToken = default)
  {
    var payload = encoder_(request);
    try
    {
      var intent = await inner_.OpenAsync(payload,
                                          cancellationToken)
                               .ConfigureAwait(false);
      return new Intent<TRequest, TResponse>(intent,
                                             encoder_,
                                             decoder_);
    }
    catch (ServerError error)
    {
      throw decoder_(error.Payload);
    }
  }

  [PublicAPI]
  public static async Task<Client<TRequest, TResponse>> ConnectAsync(string                  host,
                                                                     int                     port,
                                                                     Func<TRequest, byte[]>  encoder,
                                                                     Func<byte[], TResponse> decoder,
                                                                     Client.Options?         options           = null,
                                                                     ILogger<Client>?        logger            = null,
                                                                     CancellationToken       cancellationToken = default)
  {
    try
    {
      var client = await Client.ConnectAsync(host,
                                             port,
                                             options,
                                             logger,
                                             cancellationToken)
                               .ConfigureAwait(false);
      return new Client<TRequest, TResponse>(client,
                                             encoder,
                                             decoder);
    }
    catch (ServerError error)
    {
      throw decoder(error.Payload);
    }
  }
}
