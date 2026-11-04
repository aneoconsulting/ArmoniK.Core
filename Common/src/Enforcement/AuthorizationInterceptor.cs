// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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

using Grpc.AspNetCore.Server;
using Grpc.Core;
using Grpc.Core.Interceptors;

using Microsoft.AspNetCore.Http;

namespace ArmoniK.Core.Common.Enforcement;

/// <summary>
///   Decides every call of the control plane before its handler runs. Only registered when authorization is enabled
/// </summary>
public class AuthorizationInterceptor : Interceptor
{
  private readonly AuthorizationEnforcer enforcer_;

  /// <summary>
  ///   Build the interceptor
  /// </summary>
  /// <param name="enforcer">Decides requests</param>
  public AuthorizationInterceptor(AuthorizationEnforcer enforcer)
    => enforcer_ = enforcer;

  /// <inheritdoc />
  public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(TRequest                               request,
                                                                                ServerCallContext                      context,
                                                                                UnaryServerMethod<TRequest, TResponse> continuation)
  {
    await AuthorizeAsync(request,
                         context)
      .ConfigureAwait(false);

    return await continuation(request,
                              context)
             .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public override async Task ServerStreamingServerHandler<TRequest, TResponse>(TRequest                                         request,
                                                                               IServerStreamWriter<TResponse>                   responseStream,
                                                                               ServerCallContext                                context,
                                                                               ServerStreamingServerMethod<TRequest, TResponse> continuation)
  {
    await AuthorizeAsync(request,
                         context)
      .ConfigureAwait(false);

    await continuation(request,
                       responseStream,
                       context)
      .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public override async Task<TResponse> ClientStreamingServerHandler<TRequest, TResponse>(IAsyncStreamReader<TRequest>                     requestStream,
                                                                                          ServerCallContext                                context,
                                                                                          ClientStreamingServerMethod<TRequest, TResponse> continuation)
    => await continuation(await DecidingStreamAsync(requestStream,
                                                    context)
                            .ConfigureAwait(false),
                          context)
         .ConfigureAwait(false);

  /// <inheritdoc />
  public override async Task DuplexStreamingServerHandler<TRequest, TResponse>(IAsyncStreamReader<TRequest>                     requestStream,
                                                                               IServerStreamWriter<TResponse>                   responseStream,
                                                                               ServerCallContext                                context,
                                                                               DuplexStreamingServerMethod<TRequest, TResponse> continuation)
    => await continuation(await DecidingStreamAsync(requestStream,
                                                    context)
                            .ConfigureAwait(false),
                          responseStream,
                          context)
         .ConfigureAwait(false);

  private async Task<IAsyncStreamReader<TRequest>> DecidingStreamAsync<TRequest>(IAsyncStreamReader<TRequest> requestStream,
                                                                                 ServerCallContext            context)
  {
    var call = Rule(context);

    if (call is null)
    {
      return requestStream;
    }

    var (service, method, rule) = call.Value;

    // What a session rule decides on is in the first message
    if (rule.Kind == RuleKind.Session)
    {
      return new DecidingReader<TRequest>(requestStream,
                                          first => enforcer_.AuthorizeAsync(service,
                                                                            method,
                                                                            rule,
                                                                            first!,
                                                                            context),
                                          () => enforcer_.RefuseEmptyStream(service,
                                                                            method,
                                                                            rule,
                                                                            context));
    }

    await enforcer_.AuthorizeAsync(service,
                                   method,
                                   rule,
                                   new object(),
                                   context)
                   .ConfigureAwait(false);
    return requestStream;
  }

  private async Task AuthorizeAsync(object            request,
                                    ServerCallContext context)
  {
    var call = Rule(context);

    if (call is null)
    {
      return;
    }

    var (service, method, rule) = call.Value;

    await enforcer_.AuthorizeAsync(service,
                                   method,
                                   rule,
                                   request,
                                   context)
                   .ConfigureAwait(false);
  }

  /// <summary>
  ///   The rule of the call, null for a service served without authorization, such as reflection
  /// </summary>
  private static (string Service, string Method, MethodRule Rule)? Rule(ServerCallContext context)
  {
    var metadata = context.GetHttpContext()
                          .GetEndpoint()
                          ?.Metadata.GetMetadata<GrpcMethodMetadata>() ?? throw new RpcException(new Status(StatusCode.Internal,
                                                                                                            "The call carries no gRPC endpoint metadata, so it cannot be authorized"));

    var service = metadata.ServiceType.Name;
    var method  = metadata.Method.Name;
    var rule = MethodAuthorization.For(service,
                                       method);

    return rule is null
             ? null
             : (service, method, rule);
  }

  /// <summary>
  ///   Reads a request stream, deciding on its first message before handing it to the handler
  /// </summary>
  private sealed class DecidingReader<T> : IAsyncStreamReader<T>
  {
    private readonly Func<T, Task>         decide_;
    private readonly Func<Exception>       empty_;
    private readonly IAsyncStreamReader<T> inner_;
    private          Task?                 decision_;

    public DecidingReader(IAsyncStreamReader<T> inner,
                          Func<T, Task>         decide,
                          Func<Exception>       empty)
    {
      inner_  = inner;
      decide_ = decide;
      empty_  = empty;
    }

    public T Current
      => inner_.Current;

    public async Task<bool> MoveNext(CancellationToken cancellationToken)
    {
      // A refusal is kept, so that a handler reading on after it gets it again instead of the next messages
      if (decision_ is not null)
      {
        await decision_.ConfigureAwait(false);
        return await inner_.MoveNext(cancellationToken)
                           .ConfigureAwait(false);
      }

      var moved = await inner_.MoveNext(cancellationToken)
                              .ConfigureAwait(false);
      // A stream without message names nothing to decide on: it is refused, so that no handler runs undecided
      decision_ = moved
                    ? decide_(inner_.Current)
                    : Task.FromException(empty_());
      await decision_.ConfigureAwait(false);
      return moved;
    }
  }
}
