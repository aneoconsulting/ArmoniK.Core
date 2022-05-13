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
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;

using Grpc.Core;

using ComputeRequest = ArmoniK.Api.gRPC.V1.ProcessRequest.Types.ComputeRequest;
using WorkerClient = ArmoniK.Api.gRPC.V1.Worker.WorkerClient;

namespace ArmoniK.Core.Common.Stream.Worker;

public class WorkerStreamHandler : IWorkerStreamHandler
{
  private readonly WorkerClient                                            workerClient_;
  private          bool                                                    isInitialized_;

  public WorkerStreamHandler(GrpcChannelProvider channelProvider)
  {
    ChannelBase channel;
    try
    {
      channel = channelProvider.Get();
    }
    catch
    {
      throw new ArmoniKException("Could not get grpc channel");
    }

    workerClient_ = new WorkerClient(channel);
  }

  public Queue<ComputeRequest> WorkerReturn() 
  { 
    return new Queue<ComputeRequest>();
  }

  public void StartTaskProcessing(TaskData taskData, CancellationToken cancellationToken)
  {
    Stream = workerClient_.Process(deadline: DateTime.UtcNow + taskData.Options.MaxDuration,
                                   cancellationToken: cancellationToken);
    if (Stream is null)
    {
      throw new ArmoniKException($"Failed to recuperate Stream for {taskData.TaskId}");
    }

    Pipe = new GrpcAsyncPipe<ProcessReply, ProcessRequest>(Stream.ResponseStream,
                                                           Stream.RequestStream);
  }

  public AsyncDuplexStreamingCall<ProcessRequest, ProcessReply>? Stream { get; private set; }

  public IAsyncPipe<ProcessReply, ProcessRequest>? Pipe { get; private set; }

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

  public void Dispose()
  {
    Stream?.Dispose();
    GC.SuppressFinalize(this);
  }
}