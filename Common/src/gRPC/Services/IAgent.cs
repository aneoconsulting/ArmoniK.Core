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

using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Agent;
using ArmoniK.Core.Common.Storage;

using Grpc.Core;

using Result = ArmoniK.Api.gRPC.V1.Agent.Result;

namespace ArmoniK.Core.Common.gRPC.Services;

public interface IAgent
{
  Task FinalizeTaskCreation(CancellationToken cancellationToken);

  Task<CreateTaskReply> CreateTask(IAsyncStreamReader<CreateTaskRequest> requestStream,
                                   CancellationToken                     cancellationToken);

  Task GetCommonData(DataRequest                    request,
                     IServerStreamWriter<DataReply> responseStream,
                     CancellationToken              cancellationToken);

  Task GetDirectData(DataRequest                    request,
                     IServerStreamWriter<DataReply> responseStream,
                     CancellationToken              cancellationToken);

  Task GetResourceData(DataRequest                    request,
                       IServerStreamWriter<DataReply> responseStream,
                       CancellationToken              cancellationToken);

  Task<ResultReply> SendResult(IAsyncStreamReader<Result> requestStream,
                               CancellationToken          cancellationToken);
}