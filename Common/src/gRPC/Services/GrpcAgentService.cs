﻿// This file is part of the ArmoniK project
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

using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Agent;

using Grpc.Core;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.gRPC.Services;

public class GrpcAgentService : Api.gRPC.V1.Agent.Agent.AgentBase
{
  private readonly ILogger<GrpcAgentService> logger_;
  private          IAgent?                   agent_;


  public GrpcAgentService(ILogger<GrpcAgentService> logger)
    => logger_ = logger;

  public Task Start(IAgent agent)
  {
    agent_ = agent;
    return Task.CompletedTask;
  }

  public Task Stop()
  {
    agent_ = null;
    return Task.CompletedTask;
  }

  public override async Task<CreateTaskReply> CreateTask(IAsyncStreamReader<CreateTaskRequest> requestStream,
                                                         ServerCallContext                     context)
  {
    if (agent_ != null)
    {
      return await agent_.CreateTask(requestStream,
                                     context.CancellationToken)
                         .ConfigureAwait(false);
    }

    return new CreateTaskReply
           {
             Error = "No task is accepting request",
           };
  }

  public override async Task GetCommonData(DataRequest                    request,
                                           IServerStreamWriter<DataReply> responseStream,
                                           ServerCallContext              context)
  {
    if (agent_ != null)
    {
      await agent_.GetCommonData(request,
                                 responseStream,
                                 context.CancellationToken)
                  .ConfigureAwait(false);
    }
    else
    {
      await responseStream.WriteAsync(new DataReply
                                      {
                                        Error = "No task is accepting request",
                                      })
                          .ConfigureAwait(false);
    }
  }

  public override async Task GetResourceData(DataRequest                    request,
                                             IServerStreamWriter<DataReply> responseStream,
                                             ServerCallContext              context)
  {
    if (agent_ != null)
    {
      await agent_.GetResourceData(request,
                                   responseStream,
                                   context.CancellationToken)
                  .ConfigureAwait(false);
    }
    else
    {
      await responseStream.WriteAsync(new DataReply
                                      {
                                        Error = "No task is accepting request",
                                      })
                          .ConfigureAwait(false);
    }
  }

  public override async Task<ResultReply> SendResult(IAsyncStreamReader<Result> requestStream,
                                                     ServerCallContext          context)
  {
    if (agent_ != null)
    {
      return await agent_.SendResult(requestStream,
                                     context.CancellationToken)
                         .ConfigureAwait(false);
    }

    return new ResultReply
           {
             Error = "No task is accepting request",
           };
  }
}
