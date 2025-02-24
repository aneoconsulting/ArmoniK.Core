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
public class Intent<T> : IDisposable, IAsyncDisposable
  where T : class
{
  private readonly int                    id_;
  private readonly ILogger                logger_;
  private          Client<T>?             client_;
  private          T?                     diposeRequestPayload_;
  private          Request<T>.RequestType disposeRequestType_;

  internal Intent(Client<T>? client,
                  ILogger    logger,
                  int        id)
  {
    client_               = client;
    logger_               = logger;
    id_                   = id;
    disposeRequestType_   = Request<T>.RequestType.Close;
    diposeRequestPayload_ = null;
  }

  public async ValueTask DisposeAsync()
  {
    await ReleaseUnmanagedResourcesAsync(Interlocked.Exchange(ref client_,
                                                              null),
                                         logger_,
                                         id_,
                                         Request<T>.RequestType.Reset,
                                         null)
      .ConfigureAwait(false);
    GC.SuppressFinalize(this);
  }

  public void Dispose()
    => DisposeAsync()
      .WaitSync();

  public void CloseOnDispose(T? payload = null)
  {
    disposeRequestType_   = Request<T>.RequestType.Close;
    diposeRequestPayload_ = payload;
  }

  public void AbortOnDispose(T? payload = null)
  {
    disposeRequestType_   = Request<T>.RequestType.Abort;
    diposeRequestPayload_ = payload;
  }

  public void ResetOnDispose(T? payload = null)
  {
    disposeRequestType_   = Request<T>.RequestType.Reset;
    diposeRequestPayload_ = payload;
  }

  public void TimeoutOnDispose(T? payload = null)
  {
    disposeRequestType_   = Request<T>.RequestType.Timeout;
    diposeRequestPayload_ = payload;
  }

  private static async Task ReleaseUnmanagedResourcesAsync(Client<T>?             client,
                                                           ILogger                logger,
                                                           int                    id,
                                                           Request<T>.RequestType requestType,
                                                           T?                     payload)
  {
    if (client is null)
    {
      return;
    }

    try
    {
      await client.Call(new Request<T>
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
                                                         Request<T>.RequestType.Reset,
                                                         null));

  private async Task CallAsync(Request<T>.RequestType requestType,
                               T?                     obj,
                               CancellationToken      cancellationToken)
  {
    ObjectDisposedException.ThrowIf(client_ is null,
                                    this);

    await client_.Call(new Request<T>
                       {
                         IntentId = id_,
                         Type     = requestType,
                         Payload  = obj,
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
  public Task AmendAsync(T?                obj,
                         CancellationToken cancellationToken = default)
    => CallAsync(Request<T>.RequestType.Amend,
                 obj,
                 cancellationToken);

  [PublicAPI]
  public Task AbortAsync(T?                obj,
                         CancellationToken cancellationToken = default)
    => CallAsync(Request<T>.RequestType.Abort,
                 obj,
                 cancellationToken);

  [PublicAPI]
  public Task CloseAsync(T?                obj,
                         CancellationToken cancellationToken = default)
    => CallAsync(Request<T>.RequestType.Close,
                 obj,
                 cancellationToken);
}
