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

using ArmoniK.Core.Control.IntentLog.Protocol.Messages;
using ArmoniK.Utils;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Control.IntentLog.Protocol.Client;

[PublicAPI]
public class Intent : IDisposable, IAsyncDisposable
{
  private readonly Guid        id_;
  private readonly ILogger     logger_;
  private          Client?     client_;
  private          byte[]      diposeRequestPayload_;
  private          RequestType disposeRequestType_;

  internal Intent(Client? client,
                  ILogger logger,
                  Guid    id)
  {
    client_               = client;
    logger_               = logger;
    id_                   = id;
    disposeRequestType_   = RequestType.Close;
    diposeRequestPayload_ = Array.Empty<byte>();
  }

  public async ValueTask DisposeAsync()
  {
    await ReleaseUnmanagedResourcesAsync(Interlocked.Exchange(ref client_,
                                                              null),
                                         logger_,
                                         id_,
                                         disposeRequestType_,
                                         diposeRequestPayload_)
      .ConfigureAwait(false);
    GC.SuppressFinalize(this);
  }

  public void Dispose()
    => DisposeAsync()
      .WaitSync();

  [PublicAPI]
  public void CloseOnDispose(byte[]? payload = null)
  {
    disposeRequestType_   = RequestType.Close;
    diposeRequestPayload_ = payload ?? Array.Empty<byte>();
  }

  [PublicAPI]
  public void AbortOnDispose(byte[]? payload = null)
  {
    disposeRequestType_   = RequestType.Abort;
    diposeRequestPayload_ = payload ?? Array.Empty<byte>();
  }

  [PublicAPI]
  public void ResetOnDispose(byte[]? payload = null)
  {
    disposeRequestType_   = RequestType.Reset;
    diposeRequestPayload_ = payload ?? Array.Empty<byte>();
  }

  [PublicAPI]
  public void TimeoutOnDispose(byte[]? payload = null)
  {
    disposeRequestType_   = RequestType.Timeout;
    diposeRequestPayload_ = payload ?? Array.Empty<byte>();
  }

  private static async Task ReleaseUnmanagedResourcesAsync(Client?     client,
                                                           ILogger     logger,
                                                           Guid        id,
                                                           RequestType requestType,
                                                           byte[]      payload)
  {
    if (client is null)
    {
      return;
    }

    try
    {
      await client.Call(new Request
                        {
                          IntentId = id,
                          Type     = requestType,
                          Payload  = payload,
                        })
                  .ConfigureAwait(false);
    }
    catch (Exception ex)
    {
      logger.LogError(ex,
                      "Error while releasing intent {IntentId}",
                      id);
    }
  }

  ~Intent()
    => _ = Task.Run(() => ReleaseUnmanagedResourcesAsync(Interlocked.Exchange(ref client_,
                                                                              null),
                                                         logger_,
                                                         id_,
                                                         RequestType.Reset,
                                                         Array.Empty<byte>()));

  internal async Task CallAsync(RequestType       requestType,
                                byte[]            payload,
                                CancellationToken cancellationToken)
  {
    ObjectDisposedException.ThrowIf(client_ is null,
                                    this);

    await client_.Call(new Request
                       {
                         IntentId = id_,
                         Type     = requestType,
                         Payload  = payload,
                       },
                       cancellationToken)
                 .ConfigureAwait(false);

    if (requestType.IsFinal())
    {
      client_ = null;
      GC.SuppressFinalize(this);
    }
  }

  [PublicAPI]
  public Task AmendAsync(byte[]            payload,
                         CancellationToken cancellationToken = default)
    => CallAsync(RequestType.Amend,
                 payload,
                 cancellationToken);

  [PublicAPI]
  public Task AbortAsync(byte[]            payload,
                         CancellationToken cancellationToken = default)
    => CallAsync(RequestType.Abort,
                 payload,
                 cancellationToken);

  [PublicAPI]
  public Task CloseAsync(byte[]            payload,
                         CancellationToken cancellationToken = default)
    => CallAsync(RequestType.Close,
                 payload,
                 cancellationToken);
}

[PublicAPI]
public class Intent<TRequest, TResponse> : IDisposable, IAsyncDisposable
  where TResponse : Exception
{
  private readonly Func<byte[], TResponse> decoder_;
  private readonly Func<TRequest, byte[]>  encoder_;
  private readonly Intent                  inner_;

  internal Intent(Intent                  intent,
                  Func<TRequest, byte[]>  encoder,
                  Func<byte[], TResponse> decoder)
  {
    inner_   = intent;
    encoder_ = encoder;
    decoder_ = decoder;
  }

  public ValueTask DisposeAsync()
    => inner_.DisposeAsync();

  public void Dispose()
    => inner_.Dispose();

  [PublicAPI]
  public void CloseOnDispose(TRequest request)
  {
    var payload = encoder_(request);
    inner_.CloseOnDispose(payload);
  }

  [PublicAPI]
  public void AbortOnDispose(TRequest request)
  {
    var payload = encoder_(request);
    inner_.AbortOnDispose(payload);
  }

  [PublicAPI]
  public void ResetOnDispose(TRequest request)
  {
    var payload = encoder_(request);
    inner_.ResetOnDispose(payload);
  }

  [PublicAPI]
  public void TimeoutOnDispose(TRequest request)
  {
    var payload = encoder_(request);
    inner_.TimeoutOnDispose(payload);
  }

  private async Task CallAsync(RequestType       requestType,
                               TRequest          request,
                               CancellationToken cancellationToken)
  {
    var payload = encoder_(request);

    try
    {
      await inner_.CallAsync(requestType,
                             payload,
                             cancellationToken)
                  .ConfigureAwait(false);
    }
    catch (ServerError error)
    {
      throw decoder_(error.Payload);
    }
  }

  [PublicAPI]
  public Task AmendAsync(TRequest          request,
                         CancellationToken cancellationToken = default)
    => CallAsync(RequestType.Amend,
                 request,
                 cancellationToken);

  [PublicAPI]
  public Task AbortAsync(TRequest          request,
                         CancellationToken cancellationToken = default)
    => CallAsync(RequestType.Abort,
                 request,
                 cancellationToken);

  [PublicAPI]
  public Task CloseAsync(TRequest          request,
                         CancellationToken cancellationToken = default)
    => CallAsync(RequestType.Close,
                 request,
                 cancellationToken);
}
