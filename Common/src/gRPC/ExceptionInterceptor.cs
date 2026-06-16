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
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Base.Exceptions;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Utils;

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
  private readonly ExceptionManager exceptionManager_;
  private readonly ILogger          logger_;

  /// <summary>
  ///   Construct the interceptor
  /// </summary>
  /// <param name="exceptionManager">Component that record errors and success</param>
  /// <param name="logger">Logger to be used by the interceptor</param>
  public ExceptionInterceptor(ExceptionManager              exceptionManager,
                              ILogger<ExceptionInterceptor> logger)
  {
    exceptionManager_ = exceptionManager;
    logger_           = logger;
  }

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult((tag, exceptionManager_.Failed) switch
                       {
                         // If there is too many errors, the pod is marked as not ready to avoid Kubernetes sending new requests to the controller.
                         // Liveness Unhealthy is not needed as the pod is killing itself, without the intervention of Kubernetes.
                         (HealthCheckTag.Readiness, true) => HealthCheckResult.Unhealthy("Too many errors recorded, application is shutting down"),
                         _                                => HealthCheckResult.Healthy(),
                       });

  /// <inheritdoc />
  public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(TRequest                               request,
                                                                                ServerCallContext                      context,
                                                                                UnaryServerMethod<TRequest, TResponse> continuation)
  {
    TResponse response;
    try
    {
      response = await continuation(request,
                                    context)
                   .ConfigureAwait(false);
    }
    catch (Exception e)
    {
      await HandleException(e,
                            context)
        .ConfigureAwait(false);
      throw;
    }

    await HandleSuccess(context)
      .ConfigureAwait(false);

    return response;
  }

  /// <inheritdoc />
  public override async Task<TResponse> ClientStreamingServerHandler<TRequest, TResponse>(IAsyncStreamReader<TRequest>                     requestStream,
                                                                                          ServerCallContext                                context,
                                                                                          ClientStreamingServerMethod<TRequest, TResponse> continuation)
  {
    TResponse response;
    try
    {
      response = await base.ClientStreamingServerHandler(requestStream,
                                                         context,
                                                         continuation)
                           .ConfigureAwait(false);
    }
    catch (Exception e)
    {
      await HandleException(e,
                            context)
        .ConfigureAwait(false);
      throw;
    }

    await HandleSuccess(context)
      .ConfigureAwait(false);

    return response;
  }

  /// <inheritdoc />
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
      await HandleException(e,
                            context)
        .ConfigureAwait(false);
      throw;
    }

    await HandleSuccess(context)
      .ConfigureAwait(false);
  }

  /// <inheritdoc />
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
      await HandleException(e,
                            context)
        .ConfigureAwait(false);
      throw;
    }

    await HandleSuccess(context)
      .ConfigureAwait(false);
  }

  private ValueTask HandleException(Exception         e,
                                    ServerCallContext context)
  {
    switch (e)
    {
      case OperationCanceledException:
        if (context.CancellationToken.IsCancellationRequested)
        {
          logger_.LogWarning(e,
                             "Client has cancelled the request");
        }
        else
        {
          exceptionManager_.RecordError(logger_,
                                        e,
                                        "Request has been canceled by internal means");
        }

        break;
      case ObjectDataNotFoundException:
      case PartitionNotFoundException:
      case ResultNotFoundException:
      case SessionNotFoundException:
      case TaskNotFoundException:
        logger_.LogWarning(e,
                           e.Message);
        return ValueTask.FromException(new RpcException(new Status(StatusCode.NotFound,
                                                                   e.Message)));
      case InvalidSessionTransitionException:
      case ResultInvalidStatusException:
      case SubmissionClosedException:
        logger_.LogWarning(e,
                           e.Message);
        return ValueTask.FromException(new RpcException(new Status(StatusCode.FailedPrecondition,
                                                                   e.Message)));
      case ArmoniKException:
        exceptionManager_.RecordError(logger_,
                                      e,
                                      e.Message);
        return ValueTask.FromException(new RpcException(new Status(StatusCode.Internal,
                                                                   e.Message)));

      case RpcException rpcException:

        switch (rpcException.Status.StatusCode)
        {
          case StatusCode.OK:
          case StatusCode.Cancelled:
          case StatusCode.InvalidArgument:
          case StatusCode.NotFound:
          case StatusCode.AlreadyExists:
          case StatusCode.PermissionDenied:
          case StatusCode.Unauthenticated:
          case StatusCode.FailedPrecondition:
          case StatusCode.OutOfRange:
            logger_.LogError(e,
                             "An exception has been thrown during handling of request due to client error");
            break;
          case StatusCode.Unknown:
          case StatusCode.DeadlineExceeded:
          case StatusCode.ResourceExhausted:
          case StatusCode.Aborted:
          case StatusCode.Unimplemented:
          case StatusCode.Internal:
          case StatusCode.Unavailable:
          case StatusCode.DataLoss:
          default:
            exceptionManager_.RecordError(logger_,
                                          e,
                                          "An exception has been thrown during handling of request");
            break;
        }

        break;
      default:
        exceptionManager_.RecordError(logger_,
                                      e,
                                      "An unknown exception has been thrown during handling of request");

        throw new RpcException(new Status(StatusCode.Unknown,
                                          "Unknown Exception, see application logs"));
    }

    return ValueTask.FromException(e);
  }

  private ValueTask HandleSuccess(ServerCallContext context)
  {
    _ = context;

    exceptionManager_.RecordSuccess(logger_);

    return ValueTask.CompletedTask;
  }
}
