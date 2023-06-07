// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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
using ArmoniK.Core.Base;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;

using Grpc.Core;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Stream.Worker;

public class WorkerStreamHandler : IWorkerStreamHandler
{
  private readonly GrpcChannelProvider                                     channelProvider_;
  private readonly ILogger<WorkerStreamHandler>                            logger_;
  private readonly InitWorker                                              optionsInitWorker_;
  private          bool                                                    isInitialized_;
  private          int                                                     retryCheck_;
  private          AsyncClientStreamingCall<ProcessRequest, ProcessReply>? stream_;
  private          Api.gRPC.V1.Worker.Worker.WorkerClient?                 workerClient_;

  public WorkerStreamHandler(GrpcChannelProvider          channelProvider,
                             InitWorker                   optionsInitWorker,
                             ILogger<WorkerStreamHandler> logger)
  {
    channelProvider_   = channelProvider;
    optionsInitWorker_ = optionsInitWorker;
    logger_            = logger;
  }

  public void StartTaskProcessing(TaskData          taskData,
                                  CancellationToken cancellationToken)
  {
    if (workerClient_ == null)
    {
      throw new ArmoniKException("Worker client should be initialized");
    }

    stream_ = workerClient_.Process(deadline: DateTime.UtcNow + taskData.Options.MaxDuration,
                                    cancellationToken: cancellationToken);

    if (stream_ is null)
    {
      throw new ArmoniKException($"Failed to recuperate Stream for {taskData.TaskId}");
    }

    Pipe = new GrpcAsyncPipe<ProcessReply, ProcessRequest>(stream_.ResponseAsync,
                                                           stream_.RequestStream);
  }

  public IAsyncPipe<ProcessReply, ProcessRequest>? Pipe { get; private set; }


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
        logger_.LogDebug(ex,
                         "Channel was not created, retry in {seconds}",
                         optionsInitWorker_.WorkerCheckDelay * retry);
        await Task.Delay(optionsInitWorker_.WorkerCheckDelay * retry,
                         cancellationToken)
                  .ConfigureAwait(false);
      }
    }

    var e = new ArmoniKException("Could not get grpc channel");
    logger_.LogError(e,
                     string.Empty);
    throw e;
  }

  public async Task<HealthCheckResult> Check(HealthCheckTag tag)
  {
    try
    {
      if (!isInitialized_)
      {
        return tag == HealthCheckTag.Liveness
                 ? HealthCheckResult.Unhealthy("Worker not yet initialized")
                 : HealthCheckResult.Degraded("Worker not yet initialized");
      }

      retryCheck_++;
      var check = await CheckWorker(CancellationToken.None)
                    .ConfigureAwait(false);

      if (!check)
      {
        return retryCheck_ > optionsInitWorker_.WorkerCheckRetries
                 ? HealthCheckResult.Unhealthy("Health check on worker was not successful (too many retries)")
                 : HealthCheckResult.Degraded("Health check on worker was not successful (too many retries)");
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

  public void Dispose()
  {
    stream_?.Dispose();
    GC.SuppressFinalize(this);
  }

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
