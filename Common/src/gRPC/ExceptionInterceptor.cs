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

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Base.Exceptions;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Utils;
using ArmoniK.Utils;

using Grpc.Core;
using Grpc.Core.Interceptors;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using TaskCanceledException = ArmoniK.Core.Common.Exceptions.TaskCanceledException;

namespace ArmoniK.Core.Common.gRPC;

/// <summary>
///   Interceptor that maps exceptions thrown by the gRPC services to <see cref="RpcException" /> and records them on the
///   <see cref="ExceptionManager" />. It is marked Unhealthy (readiness) once the number of errors exceeds the threshold.
///   It also participates in graceful shutdown: it tracks in-flight requests, rejects new ones with
///   <see cref="StatusCode.Unavailable" /> while the control plane is shutting down, and signals the
///   <see cref="ExceptionManager" /> to stop the application once all in-flight requests have drained.
/// </summary>
public class ExceptionInterceptor : Interceptor, IHealthCheckProvider
{
  private readonly ExceptionManager exceptionManager_;
  private readonly ILogger          logger_;
  private          int              requestCounter_;

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
    requestCounter_   = 0;

    exceptionManager.Register();
    exceptionManager.EarlyCancellationToken.Register(StopRequest);
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
    StartRequest();
    await using var stop = new Deferrer(StopRequest);

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
    StartRequest();
    await using var stop = new Deferrer(StopRequest);

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
    StartRequest();
    await using var stop = new Deferrer(StopRequest);

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
    StartRequest();
    await using var stop = new Deferrer(StopRequest);

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
  ///   Check if the application is shutting down and increment the request counter.
  /// </summary>
  /// <exception cref="RpcException">If application is shutting down, throw a RPC unavailable</exception>
  private void StartRequest()
  {
    var previousCounter = requestCounter_;
    int counter;

    // CAS loop: atomically check the shutdown state and increment the in-flight counter.
    // Keeping the check and the increment in a single attempt is what makes the gate race-free:
    // a request can never slip past a shutdown that started between the check and the increment.
    do
    {
      counter = previousCounter;

      // A negative counter means StopRequest has already driven the counter below zero (shutdown
      // is in progress); the token check covers the window before that has happened yet. Either way,
      // refuse the request without incrementing so we do not revive a draining/stopped control plane.
      if (counter < 0 || exceptionManager_.EarlyCancellationToken.IsCancellationRequested)
      {
        throw new RpcException(new Status(StatusCode.Unavailable,
                                          "Control plane is shutting down"));
      }

      // CompareExchange returns the value that was in the field before the call:
      //   - equal to counter  => our swap won, the counter was incremented, exit the loop.
      //   - different         => another thread moved the counter; retry against the fresh value
      //                          so the shutdown check above is re-evaluated before we increment.
      previousCounter = Interlocked.CompareExchange(ref requestCounter_,
                                                    counter + 1,
                                                    counter);
    } while (counter != previousCounter);
  }

  /// <summary>
  ///   Decrement the counter and notifies the application that all requests have been processed if it is shutting down and
  ///   there are no remaining RPC
  /// </summary>
  private void StopRequest()
  {
    var counter = Interlocked.Decrement(ref requestCounter_);

    if (counter < 0)
    {
      exceptionManager_.UnregisterAndStop(logger_,
                                          "Cancellation is requested and all requests have been fully processed");
    }
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
