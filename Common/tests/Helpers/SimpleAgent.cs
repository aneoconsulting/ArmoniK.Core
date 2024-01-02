// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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

using ArmoniK.Api.gRPC.V1.Agent;
using ArmoniK.Core.Common.gRPC.Services;

using Grpc.Core;

namespace ArmoniK.Core.Common.Tests.Helpers;

public class SimpleAgent : IAgent
{
  public Task FinalizeTaskCreation(CancellationToken cancellationToken)
    => Task.CompletedTask;

  public Task<CreateTaskReply> CreateTask(IAsyncStreamReader<CreateTaskRequest> requestStream,
                                          CancellationToken                     cancellationToken)
    => Task.FromResult(new CreateTaskReply());

  public Task<DataResponse> GetCommonData(DataRequest       request,
                                          CancellationToken cancellationToken)
    => throw new NotImplementedException();

  public Task<DataResponse> GetDirectData(DataRequest       request,
                                          CancellationToken cancellationToken)
    => throw new NotImplementedException();

  public Task<DataResponse> GetResourceData(DataRequest       request,
                                            CancellationToken cancellationToken)
    => throw new NotImplementedException();

  public Task<CreateResultsMetaDataResponse> CreateResultsMetaData(CreateResultsMetaDataRequest request,
                                                                   CancellationToken            cancellationToken)
    => Task.FromResult(new CreateResultsMetaDataResponse());

  public Task<SubmitTasksResponse> SubmitTasks(SubmitTasksRequest request,
                                               CancellationToken  cancellationToken)
    => Task.FromResult(new SubmitTasksResponse());


  public Task<CreateResultsResponse> CreateResults(CreateResultsRequest request,
                                                   CancellationToken    cancellationToken)
    => Task.FromResult(new CreateResultsResponse());

  public Task<NotifyResultDataResponse> NotifyResultData(NotifyResultDataRequest request,
                                                         CancellationToken       cancellationToken)
    => Task.FromResult(new NotifyResultDataResponse());

  public Task CancelChildTasks(CancellationToken cancellationToken)
    => Task.CompletedTask;

  public void Dispose()
    => GC.SuppressFinalize(this);
}
