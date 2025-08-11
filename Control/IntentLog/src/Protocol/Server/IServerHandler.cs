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

using JetBrains.Annotations;

using MessagePack;

namespace ArmoniK.Core.Control.IntentLog.Protocol.Server;

[PublicAPI]
public interface IServerHandler
{
  public Task OpenAsync(Intent            intent,
                        byte[]            payload,
                        CancellationToken cancellationToken = default);

  public Task AmendAsync(Intent            intent,
                         byte[]            payload,
                         CancellationToken cancellationToken = default);

  public Task CloseAsync(Intent            intent,
                         byte[]            payload,
                         CancellationToken cancellationToken = default);

  public Task AbortAsync(Intent            intent,
                         byte[]            payload,
                         CancellationToken cancellationToken = default);

  public Task TimeoutAsync(Intent            intent,
                           byte[]            payload,
                           CancellationToken cancellationToken = default);

  public Task ResetAsync(Intent            intent,
                         byte[]            payload,
                         CancellationToken cancellationToken = default);
}

[PublicAPI]
public interface IServerHandler<in TRequest>
{
  public Task OpenAsync(Intent            intent,
                        TRequest          request,
                        CancellationToken cancellationToken = default);

  public Task AmendAsync(Intent            intent,
                         TRequest          request,
                         CancellationToken cancellationToken = default);

  public Task CloseAsync(Intent            intent,
                         TRequest          request,
                         CancellationToken cancellationToken = default);

  public Task AbortAsync(Intent            intent,
                         TRequest          request,
                         CancellationToken cancellationToken = default);

  public Task TimeoutAsync(Intent            intent,
                           TRequest          request,
                           CancellationToken cancellationToken = default);

  public Task ResetAsync(Intent            intent,
                         TRequest          request,
                         CancellationToken cancellationToken = default);
}

[PublicAPI]
public class ServerHandler<TRequest, TResponse> : IServerHandler
  where TResponse : Exception
{
  private readonly Func<byte[], TRequest>   decoder_;
  private readonly Func<TResponse, byte[]>  encoder_;
  private readonly IServerHandler<TRequest> inner_;

  public ServerHandler(IServerHandler<TRequest> inner)
    : this(inner,
           response => MessagePackSerializer.Serialize(response),
           payload => MessagePackSerializer.Deserialize<TRequest>(payload))
  {
  }

  public ServerHandler(IServerHandler<TRequest> inner,
                       Func<TResponse, byte[]>  encoder,
                       Func<byte[], TRequest>   decoder)
  {
    inner_   = inner;
    encoder_ = encoder;
    decoder_ = decoder;
  }

  public async Task OpenAsync(Intent            intent,
                              byte[]            payload,
                              CancellationToken cancellationToken = default)
  {
    try
    {
      await inner_.OpenAsync(intent,
                             decoder_(payload),
                             cancellationToken);
    }
    catch (TResponse response)
    {
      throw new ServerError(response.Message,
                            encoder_(response));
    }
  }

  public async Task AmendAsync(Intent            intent,
                               byte[]            payload,
                               CancellationToken cancellationToken = default)
  {
    try
    {
      await inner_.AmendAsync(intent,
                              decoder_(payload),
                              cancellationToken);
    }
    catch (TResponse response)
    {
      throw new ServerError(response.Message,
                            encoder_(response));
    }
  }

  public async Task CloseAsync(Intent            intent,
                               byte[]            payload,
                               CancellationToken cancellationToken = default)
  {
    try
    {
      await inner_.CloseAsync(intent,
                              decoder_(payload),
                              cancellationToken);
    }
    catch (TResponse response)
    {
      throw new ServerError(response.Message,
                            encoder_(response));
    }
  }

  public async Task AbortAsync(Intent            intent,
                               byte[]            payload,
                               CancellationToken cancellationToken = default)
  {
    try
    {
      await inner_.AbortAsync(intent,
                              decoder_(payload),
                              cancellationToken);
    }
    catch (TResponse response)
    {
      throw new ServerError(response.Message,
                            encoder_(response));
    }
  }

  public async Task TimeoutAsync(Intent            intent,
                                 byte[]            payload,
                                 CancellationToken cancellationToken = default)
  {
    try
    {
      await inner_.TimeoutAsync(intent,
                                decoder_(payload),
                                cancellationToken);
    }
    catch (TResponse response)
    {
      throw new ServerError(response.Message,
                            encoder_(response));
    }
  }

  public async Task ResetAsync(Intent            intent,
                               byte[]            payload,
                               CancellationToken cancellationToken = default)
  {
    try
    {
      await inner_.ResetAsync(intent,
                              decoder_(payload),
                              cancellationToken);
    }
    catch (TResponse response)
    {
      throw new ServerError(response.Message,
                            encoder_(response));
    }
  }
}
