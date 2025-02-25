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

using Action = System.Action;

namespace ArmoniK.Core.Control.IntentLog.Protocol.Server;

[PublicAPI]
public class Connection : IDisposable, IAsyncDisposable
{
  private readonly ILogger                 logger_;
  private          CancellationTokenSource cts_;
  private          Task                    eventLoop_;

  [PublicAPI]
  public Connection(IServerHandler    handler,
                    Stream            stream,
                    Action            onClose,
                    ILogger?          logger,
                    CancellationToken cancellationToken)
  {
    logger_ = logger ?? NullLogger.Instance;
    cts_    = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    var responseChannel = Channel.CreateBounded<(Response, bool)>(1);

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

      logger_.LogDebug("Connection {ConnectionId} opened",
                       Id);

      var nextRequest  = NextRequest();
      var nextResponse = NextResponse();

      var mapping = new Dictionary<int, Intent>();

      while (!cancellationToken.IsCancellationRequested)
      {
        try
        {
          var readyTask = await Task.WhenAny(nextRequest,
                                             nextResponse)
                                    .ConfigureAwait(false);

          if (ReferenceEquals(readyTask,
                              nextRequest))
          {
            var request = await nextRequest.ConfigureAwait(false);

            logger_.LogDebug("Connection {ConnectionId} received request {IntentId}:{IntentAction}:{@IntentPayload}",
                             Id,
                             request.IntentId,
                             request.Type,
                             request.Payload);

            nextRequest = NextRequest();

            // ReSharper disable once SwitchStatementHandlesSomeKnownEnumValuesWithDefault
            switch (request.Type)
            {
              case Request.RequestType.Ping:
                await new Response
                  {
                    IntentId = request.IntentId,
                    Type     = Response.ResponseType.Pong,
                    Payload  = request.Payload,
                  }.SendAsync(str,
                              cancellationToken)
                   .ConfigureAwait(false);
                break;
              case Request.RequestType.Pong:
                break;
              default:
                if (!mapping.TryGetValue(request.IntentId,
                                         out var intent))
                {
                  intent = new Intent(this,
                                      handler,
                                      logger_,
                                      responseChannel.Writer,
                                      cancellationToken);
                  mapping[request.IntentId] = intent;
                }

                await intent.RequestAsync(request,
                                          cancellationToken)
                            .ConfigureAwait(false);
                break;
            }
          }
          else
          {
            var (response, final) = await nextResponse.ConfigureAwait(false);


            logger_.LogDebug("Connection {ConnectionId} send response intent {IntentId}:{IntentStatus} -> {@IntentError}",
                             Id,
                             response.IntentId,
                             response.Type,
                             response.Payload);
            nextResponse = NextResponse();

            if (mapping.TryGetValue(response.IntentId,
                                    out var intent) && final && intent.NbRequests == 0)
            {
              mapping.Remove(response.IntentId);
            }

            await response.SendAsync(str,
                                     cancellationToken)
                          .ConfigureAwait(false);
          }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
          // Connection closing has been requested through cancellation
          break;
        }
        catch (EndOfStreamException)
        {
          // Connection has been closed by client
          break;
        }
        catch (IOException ioException) when (ioException.InnerException is SocketException
                                                                            {
                                                                              SocketErrorCode: SocketError.ConnectionReset or SocketError.NetworkReset or
                                                                                               SocketError.HostDown or SocketError.TimedOut,
                                                                            })
        {
          // Connection has been closed by client
          break;
        }
        catch (Exception ex)
        {
          logger_.LogError(ex,
                           "Connection {ConnectionId} error",
                           Id);
          await cts_.CancelAsync()
                    .ConfigureAwait(false);
        }
      }

      foreach (var (id, intent) in mapping)
      {
        try
        {
          await handler.ResetAsync(this,
                                   id,
                                   Array.Empty<byte>(),
                                   cancellationToken)
                       .ConfigureAwait(false);
          await intent.DisposeAsync()
                      .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
          logger_.LogError(ex,
                           "Connection {ConnectionId} error while resetting intent {IntentId}",
                           Id,
                           id);
        }
      }

      onClose();

      logger_.LogDebug("Connection {ConnectionId} closed",
                       Id);
    }

    Task<Request> NextRequest()
      => Request.ReceiveAsync(stream,
                              cts_.Token);

    Task<(Response, bool)> NextResponse()
      => responseChannel.Reader.ReadAsync(cts_.Token)
                        .AsTask();
  }

  public long Id
    => GetHashCode();

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
                       "Connection {ConnectionId} error while disposing",
                       Id);
    }

    cts_.Dispose();
  }


  public void Dispose()
    => DisposeAsync()
      .WaitSync();
}
