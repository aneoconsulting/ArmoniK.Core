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
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Agent;
using ArmoniK.Api.gRPC.V1.Worker;
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Injection.Options;

using Grpc.Core;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Stream.Worker;

[PublicAPI]
public class WorkerStreamWrapper : Api.gRPC.V1.Worker.Worker.WorkerBase, IAsyncDisposable
{
  private readonly ILoggerFactory               loggerFactory_;
  public           ILogger<WorkerStreamWrapper> logger_;
  private readonly GrpcChannelProvider          channelProvider_;
  private readonly ChannelBase                  channel_;
  private readonly Agent.AgentClient            client_;

  public WorkerStreamWrapper(ILoggerFactory loggerFactory)
  {
    logger_        = loggerFactory.CreateLogger<WorkerStreamWrapper>();
    loggerFactory_ = loggerFactory;


    logger_.LogDebug("Trying to create channel for {address}",
                     "/cache/armonik_agent.sock");

    channelProvider_ = new GrpcChannelProvider(new GrpcChannel
                                               {
                                                 Address    = "/cache/armonik_agent.sock",
                                                 SocketType = GrpcSocketType.UnixSocket,
                                               },
                                               loggerFactory_.CreateLogger<GrpcChannelProvider>());
    channel_ = channelProvider_.Get();

    client_ = new Agent.AgentClient(channel_);
  }

  public sealed override async Task<ProcessReply> Process(IAsyncStreamReader<ProcessRequest> requestStream,
                                                          ServerCallContext                  context)
  {
    Output output;
    {
      await using var taskHandler = await TaskHandler.Create(requestStream,
                                                             client_,
                                                             loggerFactory_,
                                                             context.CancellationToken)
                                                     .ConfigureAwait(false);

      using var _ = logger_.BeginNamedScope("Execute task",
                                            ("taskId", taskHandler.TaskId),
                                            ("sessionId", taskHandler.SessionId));
      logger_.LogDebug("Execute Process");
      output = await Process(taskHandler)
                 .ConfigureAwait(false);
    }
    return new ProcessReply
           {
             Output = output,
           };
  }

  public virtual Task<Output> Process(ITaskHandler taskHandler)
    => throw new RpcException(new Status(StatusCode.Unimplemented,
                                         ""));

  public override Task<HealthCheckReply> HealthCheck(Empty             request,
                                                     ServerCallContext context)
    => Task.FromResult(new HealthCheckReply
                       {
                         Status = HealthCheckReply.Types.ServingStatus.Serving,
                       });

  public async ValueTask DisposeAsync()
  {
    await channel_.ShutdownAsync().ConfigureAwait(false);
    await channelProvider_.DisposeAsync()
                          .ConfigureAwait(false);
  }
}
