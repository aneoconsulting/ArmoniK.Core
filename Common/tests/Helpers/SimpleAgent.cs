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

  public Task GetCommonData(DataRequest                    request,
                            IServerStreamWriter<DataReply> responseStream,
                            CancellationToken              cancellationToken)
    => throw new NotImplementedException();

  public Task GetDirectData(DataRequest                    request,
                            IServerStreamWriter<DataReply> responseStream,
                            CancellationToken              cancellationToken)
    => throw new NotImplementedException();

  public Task GetResourceData(DataRequest                    request,
                              IServerStreamWriter<DataReply> responseStream,
                              CancellationToken              cancellationToken)
    => throw new NotImplementedException();

  public Task<ResultReply> SendResult(IAsyncStreamReader<Result> requestStream,
                                      CancellationToken          cancellationToken)
    => Task.FromResult(new ResultReply());

  public Task<CreateResultsMetaDataResponse> CreateResultsMetaData(CreateResultsMetaDataRequest request,
                                                                   CancellationToken            cancellationToken)
    => Task.FromResult(new CreateResultsMetaDataResponse());

  public Task<SubmitTasksResponse> SubmitTasks(SubmitTasksRequest request,
                                               CancellationToken  cancellationToken)
    => Task.FromResult(new SubmitTasksResponse());

  public Task<UploadResultDataResponse> UploadResultData(IAsyncStreamReader<UploadResultDataRequest> requestStream,
                                                         CancellationToken                           cancellationToken)
    => Task.FromResult(new UploadResultDataResponse());

  public Task<CreateResultsResponse> CreateResults(CreateResultsRequest request,
                                                   CancellationToken    cancellationToken)
    => Task.FromResult(new CreateResultsResponse());

  public void Dispose()
  {
    GC.SuppressFinalize(this);
  }
}
