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
using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.Storage;

using Grpc.Core;

using ComputeRequest = ArmoniK.Api.gRPC.V1.ProcessRequest.Types.ComputeRequest;
using WorkerClient = ArmoniK.Api.gRPC.V1.Worker.WorkerClient;

namespace ArmoniK.Core.Common.Stream.Worker;

public class WorkerStreamHandler : IWorkerStreamHandler
{
  private readonly WorkerClient   workerClient_;
  private readonly DataPrefetcher dataPrefetcher_;
  private          bool           isInitialized_;

  public WorkerStreamHandler(IGrpcChannelProvider channelProvider, DataPrefetcher dataPrefetcher)
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

  public async Task<Queue<ComputeRequest>> StartTaskPrefetching(TaskData          taskData,
                                                                CancellationToken cancellationToken)
  { 
    return await dataPrefetcher_.PrefetchDataAsync(taskData,
                                                   cancellationToken).ConfigureAwait(false);
  }

  public void StartTaskProcessing(TaskData taskData, CancellationToken cancellationToken)
  {
    Stream = workerClient_.Process(deadline: DateTime.UtcNow + taskData.Options.MaxDuration,
                                   cancellationToken: cancellationToken);
    WorkerRequestStream = Stream is not null ?
                            Stream.RequestStream
                            : throw new ArmoniKException($"Failed to recuperate Stream for {taskData.TaskId}");
    WorkerResponseStream = Stream is not null ?
                             Stream.ResponseStream :
                             throw new ArmoniKException($"Failed to recuperate Stream for {taskData.TaskId}");
  }

  public AsyncDuplexStreamingCall<ProcessRequest, ProcessReply>? Stream { get; private set; }

  public IAsyncStreamReader<ProcessReply>? WorkerResponseStream { get; private set; }

  public IClientStreamWriter<ProcessRequest>? WorkerRequestStream { get; private set; }

  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      await dataPrefetcher_.Init(cancellationToken)
                           .ConfigureAwait(false);
      isInitialized_ = true;
    }
  }

  public ValueTask<bool> Check(HealthCheckTag tag)
    => ValueTask.FromResult(isInitialized_);

  public void Dispose()
  {
    Stream?.Dispose();
    GC.SuppressFinalize(this);
  }
}
