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

using ArmoniK.Api.Common.Channel.Utils;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Worker;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Base.Exceptions;
using ArmoniK.Core.Common.gRPC.Convertors;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Storage;

using Grpc.Core;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using Output = ArmoniK.Core.Common.Storage.Output;

namespace ArmoniK.Core.Common.Stream.Worker;

/// <summary>
///   Handles the interactions with the worker.
/// </summary>
public class WorkerStreamHandler : IWorkerStreamHandler
{
  private readonly GrpcChannelProvider                     channelProvider_;
  private readonly ILogger<WorkerStreamHandler>            logger_;
  private readonly InitWorker                              optionsInitWorker_;
  private          bool                                    isInitialized_;
  private          int                                     retryCheck_;
  private          Api.gRPC.V1.Worker.Worker.WorkerClient? workerClient_;

  /// <summary>
  ///   Initializes a new instance of the <see cref="WorkerStreamHandler" /> class.
  /// </summary>
  /// <param name="channelProvider">The gRPC channel provider.</param>
  /// <param name="optionsInitWorker">The initialization options for the worker.</param>
  /// <param name="logger">The logger instance.</param>
  public WorkerStreamHandler(GrpcChannelProvider          channelProvider,
                             InitWorker                   optionsInitWorker,
                             ILogger<WorkerStreamHandler> logger)
  {
    channelProvider_   = channelProvider;
    optionsInitWorker_ = optionsInitWorker;
    logger_            = logger;
  }

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (isInitialized_)
    {
      return;
    }

    for (var retry = 1; retry < optionsInitWorker_.WorkerCheckRetries; ++retry)
    {
      try
      {
        var channel = channelProvider_.Get();
        workerClient_ = new Api.gRPC.V1.Worker.Worker.WorkerClient(channel);

        var check = await CheckWorker(cancellationToken)
                      .ConfigureAwait(false);

        if (!check)
        {
          throw new ArmoniKException("Worker Health Check was not successful");
        }

        isInitialized_ = true;
        return;
      }
      catch (Exception ex)
      {
        logger_.LogError(ex,
                         "Failed to create worker channel, retry in {seconds}",
                         optionsInitWorker_.WorkerCheckDelay * retry);
        await Task.Delay(optionsInitWorker_.WorkerCheckDelay * retry,
                         cancellationToken)
                  .ConfigureAwait(false);
      }
    }

    var e = new ArmoniKException("Could not get grpc channel");
    logger_.LogError(e,
                     "Could not get grpc channel");
    throw e;
  }

  /// <inheritdoc />
  public async Task<HealthCheckResult> Check(HealthCheckTag tag)
  {
    try
    {
      if (!isInitialized_)
      {
        return HealthCheckResult.Unhealthy("Worker not yet initialized");
      }

      retryCheck_++;
      var check = await CheckWorker(CancellationToken.None)
                    .ConfigureAwait(false);

      if (!check)
      {
        return HealthCheckResult.Unhealthy("Health check on worker was not successful (too many retries)");
      }

      retryCheck_ = 0;
      return HealthCheckResult.Healthy();
    }
    catch (Exception ex)
    {
      return HealthCheckResult.Unhealthy("Health check on worker was not successful with exception",
                                         ex);
    }
  }

  /// <inheritdoc />
  public void Dispose()
    => GC.SuppressFinalize(this);

  /// <inheritdoc />
  public async Task<Output> StartTaskProcessing(TaskData          taskData,
                                                string            token,
                                                string            dataFolder,
                                                CancellationToken cancellationToken)
  {
    if (workerClient_ is null)
    {
      throw new ArmoniKException("Worker client should be initialized");
    }

    try
    {
      return (await workerClient_.ProcessAsync(new ProcessRequest
                                               {
                                                 CommunicationToken = token,
                                                 Configuration = new Configuration
                                                                 {
                                                                   DataChunkMaxSize = PayloadConfiguration.MaxChunkSize,
                                                                 },
                                                 DataDependencies =
                                                 {
                                                   taskData.DataDependencies,
                                                 },
                                                 DataFolder = dataFolder,
                                                 ExpectedOutputKeys =
                                                 {
                                                   taskData.ExpectedOutputIds,
                                                 },
                                                 PayloadId   = taskData.PayloadId,
                                                 SessionId   = taskData.SessionId,
                                                 TaskId      = taskData.TaskId,
                                                 TaskOptions = taskData.Options.ToGrpcTaskOptions(),
                                               },
                                               deadline: DateTime.UtcNow + taskData.Options.MaxDuration,
                                               cancellationToken: cancellationToken)
                                 .ConfigureAwait(false)).Output.ToInternalOutput();
    }
    catch (RpcException e) when (e.StatusCode is StatusCode.DeadlineExceeded)
    {
      return new Output(OutputStatus.Timeout,
                        "Deadline Exceeded");
    }
  }

  /// <summary>
  ///   Checks the health of the worker.
  /// </summary>
  /// <param name="cancellationToken">The cancellation token.</param>
  /// <returns>
  ///   A task that represents the asynchronous operation. The task result contains a boolean indicating the health
  ///   status of the worker.
  /// </returns>
  private Task<bool> CheckWorker(CancellationToken cancellationToken)
  {
    try
    {
      if (workerClient_ is null)
      {
        return Task.FromResult(false);
      }

      var reply = workerClient_.HealthCheck(new Empty(),
                                            cancellationToken: cancellationToken);
      if (reply.Status != HealthCheckReply.Types.ServingStatus.Serving)
      {
        return Task.FromResult(false);
      }

      logger_.LogDebug("Channel was initialized");
      return Task.FromResult(true);
    }
    catch (RpcException ex) when (ex.StatusCode == StatusCode.Unimplemented)
    {
      logger_.LogDebug("Channel was initialized but Worker health check is not implemented");
      return Task.FromResult(true);
    }
  }
}
