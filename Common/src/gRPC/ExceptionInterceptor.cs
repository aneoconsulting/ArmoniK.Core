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

using TaskCanceledException = ArmoniK.Core.Common.Exceptions.TaskCanceledException;

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
      throw HandleException(e,
                            context);
    }

    HandleSuccess(context);

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
      throw HandleException(e,
                            context);
    }

    HandleSuccess(context);

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
      throw HandleException(e,
                            context);
    }

    HandleSuccess(context);
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
      throw HandleException(e,
                            context);
    }

    HandleSuccess(context);
  }

  /// <summary>
  ///   Handle the exception that was thrown by the RPC
  /// </summary>
  /// <param name="e">Exception thrown by the RPC</param>
  /// <param name="context">Context of the RPC</param>
  /// <returns>RpcException that is returned to the client</returns>
  private RpcException HandleException(Exception         e,
                                       ServerCallContext context)
    => e switch
       {
         OperationCanceledException when context.CancellationToken.IsCancellationRequested => ProcessException(ExceptionAction.Warning,
                                                                                                               e,
                                                                                                               context,
                                                                                                               StatusCode.Cancelled,
                                                                                                               "Cancelled by client"),
         OperationCanceledException => ProcessException(ExceptionAction.Record,
                                                        e,
                                                        context,
                                                        StatusCode.Cancelled,
                                                        "Canceled by internal means"),
         ObjectDataNotFoundException => ProcessException(ExceptionAction.Warning,
                                                         e,
                                                         context,
                                                         StatusCode.NotFound,
                                                         "Result data was not found"),
         PartitionNotFoundException => ProcessException(ExceptionAction.Warning,
                                                        e,
                                                        context,
                                                        StatusCode.NotFound,
                                                        "Partition was not found"),
         ResultNotFoundException => ProcessException(ExceptionAction.Warning,
                                                     e,
                                                     context,
                                                     StatusCode.NotFound,
                                                     "Result was not found"),
         SessionNotFoundException => ProcessException(ExceptionAction.Warning,
                                                      e,
                                                      context,
                                                      StatusCode.NotFound,
                                                      "Session was not found"),
         TaskNotFoundException => ProcessException(ExceptionAction.Warning,
                                                   e,
                                                   context,
                                                   StatusCode.NotFound,
                                                   "Task was not found"),
         InvalidSessionTransitionException => ProcessException(ExceptionAction.Warning,
                                                               e,
                                                               context,
                                                               StatusCode.FailedPrecondition,
                                                               "The session transition is invalid"),
         ResultInvalidStatusException => ProcessException(ExceptionAction.Warning,
                                                          e,
                                                          context,
                                                          StatusCode.FailedPrecondition,
                                                          "Result status is invalid"),
         SubmissionClosedException => ProcessException(ExceptionAction.Warning,
                                                       e,
                                                       context,
                                                       StatusCode.FailedPrecondition,
                                                       "Submission for the session is closed"),
         TaskAlreadyInFinalStateException => ProcessException(ExceptionAction.Warning,
                                                              e,
                                                              context,
                                                              StatusCode.FailedPrecondition,
                                                              "Task is already in a final state"),
         TaskCanceledException => ProcessException(ExceptionAction.Warning,
                                                   e,
                                                   context,
                                                   StatusCode.FailedPrecondition,
                                                   "Task is canceled"),
         TaskPausedException => ProcessException(ExceptionAction.Warning,
                                                 e,
                                                 context,
                                                 StatusCode.FailedPrecondition,
                                                 "Task is paused"),
         ArmoniKException => ProcessException(ExceptionAction.Warning,
                                              e,
                                              context,
                                              StatusCode.Internal,
                                              "Internal error, see ArmoniK logs",
                                              true),
         RpcException
         {
           StatusCode: StatusCode.OK or StatusCode.Cancelled or StatusCode.InvalidArgument or StatusCode.NotFound or StatusCode.AlreadyExists or
                       StatusCode.PermissionDenied or StatusCode.Unauthenticated or StatusCode.FailedPrecondition or StatusCode.OutOfRange,
         } r => ProcessException(ExceptionAction.Warning,
                                 e,
                                 context,
                                 r.StatusCode,
                                 r.Status.Detail,
                                 true),
         RpcException r => ProcessException(ExceptionAction.Record,
                                            e,
                                            context,
                                            r.StatusCode,
                                            r.Status.Detail,
                                            true),
         _ => ProcessException(ExceptionAction.Record,
                               e,
                               context,
                               StatusCode.Unknown,
                               "Unknown exception, see ArmoniK logs",
                               true),
       };

  /// <summary>
  ///   Records the success of an RPC
  /// </summary>
  /// <param name="context">Context of the RPC</param>
  private void HandleSuccess(ServerCallContext context)
  {
    _ = context;

    exceptionManager_.RecordSuccess(logger_);
  }

  /// <summary>
  ///   Converts an exception into a <see cref="RpcException" />
  /// </summary>
  /// <param name="action">Action to do: either log warning, log error, or record error</param>
  /// <param name="e">Exception that was thrown by the RPC</param>
  /// <param name="context">Context of the RPC</param>
  /// <param name="statusCode">Response status code</param>
  /// <param name="details">Details for the status</param>
  /// <param name="skipMessageInRpc">
  ///   If true, the message of the exception is not added to the details in the response to the
  ///   client
  /// </param>
  /// <returns>The <see cref="RpcException" /> sent to the client</returns>
  private RpcException ProcessException(ExceptionAction   action,
                                        Exception         e,
                                        ServerCallContext context,
                                        StatusCode        statusCode,
                                        string            details,
                                        bool              skipMessageInRpc = false)
  {
    switch (action)
    {
      case ExceptionAction.Warning:
        logger_.LogWarning(e,
                           "Error while processing the request {RequestPath} with status {ErrorStatus}: {ErrorDetails}: {ErrorMessage}",
                           context.Method,
                           statusCode,
                           details,
                           e.Message);
        break;
      case ExceptionAction.Error:
        logger_.LogError(e,
                         "Error while processing the request {RequestPath} with status {ErrorStatus}: {ErrorDetails}: {ErrorMessage}",
                         context.Method,
                         statusCode,
                         details,
                         e.Message);
        break;
      case ExceptionAction.Record:
      default:
        exceptionManager_.RecordError(logger_,
                                      e,
                                      "Error while processing the request {RequestPath} with status {ErrorStatus}: {ErrorDetails}: {ErrorMessage}",
                                      context.Method,
                                      statusCode,
                                      details,
                                      e.Message);
        break;
    }

    if (!skipMessageInRpc && !string.IsNullOrEmpty(details) && details != e.Message)
    {
      details = $"{details}: {e.Message}";
    }

    return new RpcException(new Status(statusCode,
                                       details),
                            e.Message);
  }

  /// <summary>
  ///   Action to perform on the exception
  /// </summary>
  private enum ExceptionAction
  {
    /// <summary>Log with warning level</summary>
    Warning,

    /// <summary>Log with error level</summary>
    Error,

    /// <summary>Record the exception (and log with error level)</summary>
    Record,
  }
}
