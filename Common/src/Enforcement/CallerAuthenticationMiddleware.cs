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
using System.Globalization;
using System.Threading.Tasks;

using Grpc.AspNetCore.Server;
using Grpc.Core;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;

namespace ArmoniK.Core.Common.Enforcement;

/// <summary>
///   Authenticates the caller of a gRPC request from its headers, before the request is read, and refuses the request
///   when the caller cannot be authenticated
/// </summary>
/// <remarks>
///   gRPC reads and deserializes a request before calling interceptors, so <see cref="AuthorizationInterceptor" /> alone
///   would read the request of any caller, credentials or not. The caller authenticated here is kept for the
///   interceptor, which decides the request.
/// </remarks>
public sealed class CallerAuthenticationMiddleware
{
  private readonly RequestDelegate next_;

  /// <summary>
  ///   Build the middleware
  /// </summary>
  /// <param name="next">The rest of the pipeline</param>
  public CallerAuthenticationMiddleware(RequestDelegate next)
    => next_ = next;

  /// <summary>
  ///   Authenticate the caller of a gRPC request, then pass the request on, or refuse it
  /// </summary>
  /// <param name="http">HTTP context of the request</param>
  /// <param name="enforcer">Authenticates the caller</param>
  /// <returns>Task representing the asynchronous execution of the method</returns>
  public async Task InvokeAsync(HttpContext           http,
                                AuthorizationEnforcer enforcer)
  {
    var metadata = http.GetEndpoint()
                       ?.Metadata.GetMetadata<GrpcMethodMetadata>();
    var rule = metadata is null
                 ? null
                 : MethodAuthorization.For(metadata.ServiceType.Name,
                                           metadata.Method.Name);

    // Not a gRPC method, or one served without authorization
    if (rule is null)
    {
      await next_(http)
        .ConfigureAwait(false);
      return;
    }

    try
    {
      http.Items[AuthorizationEnforcer.CallerKey] = await enforcer.AuthenticateAsync(metadata!.ServiceType.Name,
                                                                                     metadata.Method.Name,
                                                                                     rule,
                                                                                     http)
                                                                  .ConfigureAwait(false);
    }
    catch (RpcException e)
    {
      Refuse(http.Response,
             e.Status);
      return;
    }

    await next_(http)
      .ConfigureAwait(false);
  }

  /// <summary>
  ///   Answer a gRPC status without message, the status in the headers ("trailers-only" response of the gRPC protocol)
  /// </summary>
  private static void Refuse(HttpResponse response,
                             Status       status)
  {
    response.StatusCode              = StatusCodes.Status200OK;
    response.ContentType             = "application/grpc";
    response.Headers["grpc-status"]  = ((int)status.StatusCode).ToString(CultureInfo.InvariantCulture);
    // Percent-encoded as the protocol requires, of UTF-8 bytes: encoding more than required is allowed
    response.Headers["grpc-message"] = Uri.EscapeDataString(status.Detail);
  }
}

/// <summary>
///   Extension to add <see cref="CallerAuthenticationMiddleware" /> to a pipeline
/// </summary>
public static class CallerAuthenticationMiddlewareExt
{
  /// <summary>
  ///   Authenticate the callers of gRPC requests before the requests are read; to be added after routing, and only when
  ///   authorization is enabled
  /// </summary>
  /// <param name="app">The pipeline</param>
  /// <returns>The pipeline</returns>
  public static IApplicationBuilder UseCallerAuthentication(this IApplicationBuilder app)
    => app.UseMiddleware<CallerAuthenticationMiddleware>();
}
