// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-$CURRENT_YEAR$. All rights reserved.
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
// but WITHOUT ANY WARRANTY

using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Agent;

using Grpc.Core;

using Microsoft.Extensions.Logging;

using Result = ArmoniK.Api.gRPC.V1.Agent.Result;

namespace ArmoniK.Core.Common.gRPC.Services;

public class GrpcAgentService : Api.gRPC.V1.Agent.Agent.AgentBase
{
  private readonly IAgent  agent_;
  private readonly ILogger logger_;


  public GrpcAgentService(IAgent  agent,
                          ILogger logger)
  {
    agent_  = agent;
    logger_ = logger;
  }

  public override async Task<CreateTaskReply> CreateTask(IAsyncStreamReader<CreateTaskRequest> requestStream,
                                                         ServerCallContext                     context)
    => await agent_.CreateTask(requestStream,
                               context.CancellationToken)
                   .ConfigureAwait(false);

  public override async Task GetCommonData(DataRequest                    request,
                                           IServerStreamWriter<DataReply> responseStream,
                                           ServerCallContext              context)
    => await agent_.GetCommonData(request,
                                  responseStream,
                                  context.CancellationToken)
                   .ConfigureAwait(false);

  public override async Task GetResourceData(DataRequest                    request,
                                             IServerStreamWriter<DataReply> responseStream,
                                             ServerCallContext              context)
    => await agent_.GetResourceData(request,
                                    responseStream,
                                    context.CancellationToken)
                   .ConfigureAwait(false);

  public override async Task<ResultReply> SendResult(IAsyncStreamReader<Result> requestStream,
                                                     ServerCallContext          context)
    => await agent_.SendResult(requestStream,
                               context.CancellationToken)
                   .ConfigureAwait(false);
}
