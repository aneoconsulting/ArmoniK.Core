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
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;

using Grpc.Core;

using Microsoft.Extensions.Logging;

using ComputeRequest = ArmoniK.Api.gRPC.V1.ProcessRequest.Types.ComputeRequest;
using WorkerClient = ArmoniK.Api.gRPC.V1.Worker.WorkerClient;

namespace ArmoniK.Core.Common.Stream.Worker;

public class WorkerStreamHandler : IWorkerStreamHandler
{
  private readonly GrpcChannelProvider          channelProvider_;
  private readonly InitWorker                   optionsInitWorker_;
  private readonly ILogger<WorkerStreamHandler> logger_;
  private          WorkerClient?                workerClient_;
  private          bool                         isInitialized_;

  public WorkerStreamHandler(GrpcChannelProvider          channelProvider,
                             InitWorker                   optionsInitWorker,
                             ILogger<WorkerStreamHandler> logger)
  {
    channelProvider_   = channelProvider;
    optionsInitWorker_ = optionsInitWorker;
    logger_            = logger;
  }

  public Queue<ComputeRequest> WorkerReturn()
  {
    return new Queue<ComputeRequest>();
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

    Pipe = new GrpcAsyncPipe<ProcessReply, ProcessRequest>(stream_.ResponseStream,
                                                           stream_.RequestStream);
  }

  private AsyncDuplexStreamingCall<ProcessRequest, ProcessReply>? stream_;

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
        workerClient_ = new WorkerClient(channel);
        var reply = workerClient_.HealthCheck(new Empty(),
                                              cancellationToken: cancellationToken);
        if (reply.Status != HealthCheckReply.Types.ServingStatus.Serving)
        {
          throw new ArmoniKException("Worker Health Check was not successful");
        }

        isInitialized_ = true;
        logger_.LogInformation("Channel was initialized");
        return;
      }
      catch (RpcException ex) when (ex.StatusCode == StatusCode.Unimplemented)
      {
        isInitialized_ = true;
        logger_.LogInformation("Channel was initialized");
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

  public ValueTask<bool> Check(HealthCheckTag tag)
    => ValueTask.FromResult(isInitialized_);

  public void Dispose()
  {
    stream_?.Dispose();
    GC.SuppressFinalize(this);
  }
}