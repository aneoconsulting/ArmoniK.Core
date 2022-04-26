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
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Storage;

using Grpc.Core;

using Microsoft.Extensions.Logging;

using ComputeRequest = ArmoniK.Api.gRPC.V1.ProcessRequest.Types.ComputeRequest;
using WorkerClient = ArmoniK.Api.gRPC.V1.Worker.WorkerClient;

namespace ArmoniK.Core.Common.Stream.Worker;

public class WorkerStreamHandler : IWorkerStreamHandler
{
  private readonly WorkerClient                 workerClient_;
  private readonly ILogger<WorkerStreamHandler> logger_;
  private          bool                         isInitialized_;

  public WorkerStreamHandler(GrpcChannelProvider          channelProvider,
                             ILogger<WorkerStreamHandler> logger)
  {
    logger_       = logger;
    workerClient_ = BuildWorkerClient(channelProvider, logger).Result;
  }

  private static async Task<WorkerClient> BuildWorkerClient(GrpcChannelProvider channelProvider,
                                                            ILogger             logger)
  {
    using var   _ = logger.LogFunction();
    ChannelBase channel;
    try
    {
      channel = await channelProvider.GetAsync()
                                     .ConfigureAwait(false);
    }
    catch (Exception e)
    {
      logger.LogError(e,
                      "Could not create grpc channel");
      throw;
    }

    return new WorkerClient(channel);
  }

  public Queue<ComputeRequest> WorkerReturn() 
  { 
    return new Queue<ComputeRequest>();
  }

  public IAsyncStreamReader<ProcessReply> WorkerResponseStream(TaskData          taskData,
                                                            CancellationToken cancellationToken)
  {
    logger_.LogInformation("Query response stream");
    return workerClient_.Process(deadline: DateTime.UtcNow + taskData.Options.MaxDuration,
                                 cancellationToken: cancellationToken)
                        .ResponseStream;
  }

  public IClientStreamWriter<ProcessRequest> WorkerRequestStream(TaskData          taskData,
                                                          CancellationToken cancellationToken)
  {
    logger_.LogInformation("Query request stream");
    return workerClient_.Process(deadline: DateTime.UtcNow + taskData.Options.MaxDuration,
                                 cancellationToken: cancellationToken).RequestStream;
  }

  public Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      isInitialized_ = true;
    }

    return Task.CompletedTask;
  }

  public ValueTask<bool> Check(HealthCheckTag tag)
    => ValueTask.FromResult(isInitialized_);
}