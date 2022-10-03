// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
//   D. Brasseur       <dbrasseur@aneo.fr>
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

using ArmoniK.Core.Common.Injection.Options;

using Grpc.Core;
using Grpc.Core.Interceptors;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.gRPC;

/// <summary>
///   Interceptor that counts all the exceptions thrown by the gRPC services
///   It is then marked Unhealthy if number of errors is higher than a threshold
/// </summary>
public class ExceptionInterceptor : Interceptor, IHealthCheckProvider
{
  private readonly ILogger logger_;
  private readonly int     maxAllowedErrors_;
  private          int     nbErrors_;

  /// <summary>
  ///   Construct the interceptor
  /// </summary>
  /// <param name="submitterOptions">Map containing the submitter options, and especially `Options.Submitter.MaxErrorAllowed`</param>
  /// <param name="logger">Logger to be used by the interceptor</param>
  public ExceptionInterceptor(Submitter                     submitterOptions,
                              ILogger<ExceptionInterceptor> logger)
  {
    maxAllowedErrors_ = submitterOptions.MaxErrorAllowed;
    nbErrors_         = 0;
    logger_           = logger;
    logger_.LogDebug("Interceptor created with {maxAllowedErrors_} maximum errors",
                     maxAllowedErrors_);
  }

  /// <inheritdoc/>
  public Task<HealthCheckResult> Check(HealthCheckTag _)
  {
    logger_.LogDebug("Interceptor HealthCheck: errors {nbErrors}/{maxAllowedErrors}",
                     nbErrors_,
                     maxAllowedErrors_);
    return Task.FromResult(nbErrors_ <= maxAllowedErrors_
                             ? HealthCheckResult.Healthy()
                             : HealthCheckResult.Unhealthy("Too many errors recorded"));
  }

  /// <inheritdoc/>
  public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(TRequest                               request,
                                                                                ServerCallContext                      context,
                                                                                UnaryServerMethod<TRequest, TResponse> continuation)
  {
    try
    {
      return await continuation(request,
                                context)
               .ConfigureAwait(false);
    }
    catch (Exception e)
    {
      await HandleException(e)
        .ConfigureAwait(false);
      throw;
    }
  }

  /// <inheritdoc/>
  public override async Task<TResponse> ClientStreamingServerHandler<TRequest, TResponse>(IAsyncStreamReader<TRequest>                     requestStream,
                                                                                          ServerCallContext                                context,
                                                                                          ClientStreamingServerMethod<TRequest, TResponse> continuation)
  {
    try
    {
      return await base.ClientStreamingServerHandler(requestStream,
                                                     context,
                                                     continuation)
                       .ConfigureAwait(false);
    }
    catch (Exception e)
    {
      await HandleException(e)
        .ConfigureAwait(false);
      throw;
    }
  }

  /// <inheritdoc/>
  public override async Task ServerStreamingServerHandler<TRequest, TResponse>(TRequest                                         request,
                                                                               IServerStreamWriter<TResponse>                   responseStream,
                                                                               ServerCallContext                                context,
                                                                               ServerStreamingServerMethod<TRequest, TResponse> continuation)
  {
    try
    {
      await base.ServerStreamingServerHandler(request,
                                              responseStream,
                                              context,
                                              continuation)
                .ConfigureAwait(false);
    }
    catch (Exception e)
    {
      await HandleException(e)
        .ConfigureAwait(false);
      throw;
    }
  }

  /// <inheritdoc/>
  public override async Task DuplexStreamingServerHandler<TRequest, TResponse>(IAsyncStreamReader<TRequest>                     requestStream,
                                                                               IServerStreamWriter<TResponse>                   responseStream,
                                                                               ServerCallContext                                context,
                                                                               DuplexStreamingServerMethod<TRequest, TResponse> continuation)
  {
    try
    {
      await base.DuplexStreamingServerHandler(requestStream,
                                              responseStream,
                                              context,
                                              continuation)
                .ConfigureAwait(false);
    }
    catch (Exception e)
    {
      await HandleException(e)
        .ConfigureAwait(false);
      throw;
    }
  }

  private ValueTask HandleException(Exception e)
  {
    _ = e;

    // Interlocked is required here because the interceptor can be shared across threads
    Interlocked.Increment(ref nbErrors_);
    return ValueTask.CompletedTask;
  }
}
