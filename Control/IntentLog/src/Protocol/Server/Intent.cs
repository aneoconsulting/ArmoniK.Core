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
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Core.Control.IntentLog.Protocol.Messages;
using ArmoniK.Utils;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Control.IntentLog.Protocol.Server;

[PublicAPI]
public class Intent : IDisposable, IAsyncDisposable
{
  private readonly CancellationTokenSource cts_;
  private readonly Task                    eventLoop_;
  private readonly ILogger                 logger_;
  private readonly Channel<Request>        requests_;

  internal Intent(Connection                      connection,
                  Guid                            intentId,
                  IServerHandler                  handler,
                  ILogger                         logger,
                  ChannelWriter<(Response, bool)> responses,
                  CancellationToken               cancellationToken)
  {
    Connection = connection;
    Id         = intentId;
    cts_       = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    logger_    = logger;
    requests_  = Channel.CreateUnbounded<Request>();

    eventLoop_ = Task.Factory.StartNew(EventLoop,
                                       TaskCreationOptions.LongRunning)
                     .Unwrap();

    return;

    async Task EventLoop()
    {
      using var cts = cts_;

      // ReSharper disable once VariableHidesOuterVariable
      var cancellationToken = cts_.Token;

      while (!cancellationToken.IsCancellationRequested)
      {
        try
        {
          var request = await requests_.Reader.ReadAsync(cancellationToken)
                                       .ConfigureAwait(false);
          Exception? exception = null;
          try
          {
            Func<Intent, byte[], CancellationToken, Task> f = request.Type switch
                                                              {
                                                                RequestType.Open    => handler.OpenAsync,
                                                                RequestType.Amend   => handler.AmendAsync,
                                                                RequestType.Close   => handler.CloseAsync,
                                                                RequestType.Abort   => handler.AbortAsync,
                                                                RequestType.Timeout => handler.TimeoutAsync,
                                                                RequestType.Reset   => handler.ResetAsync,
                                                                _                   => throw new InvalidOperationException(),
                                                              };
            await f(this,
                    request.Payload,
                    cancellationToken)
              .ConfigureAwait(false);
          }
          catch (Exception ex)
          {
            exception = ex;
          }

          await responses.WriteAsync((new Response
                                      {
                                        IntentId = request.IntentId,
                                        Type = exception is null
                                                 ? ResponseType.Success
                                                 : ResponseType.Error,
                                        Payload = exception is ServerError serverError
                                                    ? serverError.Payload
                                                    : Encoding.UTF8.GetBytes(exception?.Message ?? string.Empty),
                                      }, request.Type.IsFinal()),
                                     cancellationToken)
                         .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
          break;
        }
      }
    }
  }

  internal int NbRequests
    => requests_.Reader.Count;

  [PublicAPI]
  public Connection Connection { get; }

  [PublicAPI]
  public Guid Id { get; }


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
                       "Connection error while disposing");
    }

    cts_.Dispose();
  }


  public void Dispose()
    => DisposeAsync()
      .WaitSync();

  internal ValueTask RequestAsync(Request           request,
                                  CancellationToken cancellationToken = default)
    => requests_.Writer.WriteAsync(request,
                                   cancellationToken);
}
