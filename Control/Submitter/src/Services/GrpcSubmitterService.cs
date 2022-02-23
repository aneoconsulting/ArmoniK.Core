// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
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

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.gRPC.Services;

using Grpc.Core;

using TaskRequest = ArmoniK.Core.Common.gRPC.Services.TaskRequest;

namespace ArmoniK.Core.Control.Submitter.Services;

public class GrpcSubmitterService : Api.gRPC.V1.Submitter.SubmitterBase
{
  private readonly ISubmitter submitter_;

  public GrpcSubmitterService(ISubmitter submitter)
    => submitter_ = submitter;


  /// <inheritdoc />
  public override Task<Configuration> GetServiceConfiguration(Empty request, ServerCallContext context)
    => submitter_.GetServiceConfiguration(request,
                                          context.CancellationToken);

  public override async Task<Empty> CancelSession(Session request, ServerCallContext context)
  {
    await submitter_.CancelSession(request.Id,
                                    context.CancellationToken);
    return new();
  }

  public override async Task<Empty> CancelTasks(TaskFilter request, ServerCallContext context)
  {
    await submitter_.CancelTasks(request,
                                  context.CancellationToken);
    return new();
  }

  /// <inheritdoc />
  public override Task<CreateSessionReply> CreateSession(CreateSessionRequest request, ServerCallContext context)
    => submitter_.CreateSession(request.Id,
                                request.DefaultTaskOption,
                                context.CancellationToken);

  public override Task<CreateTaskReply> CreateSmallTasks(CreateSmallTaskRequest request, ServerCallContext context)
    => submitter_.CreateTasks(request.SessionId,
                              request.SessionId,
                              request.SessionId,
                              request.TaskOptions,
                              request.TaskRequests
                                     .ToAsyncEnumerable()
                                     .Select(taskRequest => new TaskRequest(taskRequest.Id,
                                                                            taskRequest.ExpectedOutputKeys,
                                                                            taskRequest.DataDependencies,
                                                                            new[] { taskRequest.Payload.Memory }.ToAsyncEnumerable())),
                              context.CancellationToken);


  /// <inheritdoc />
  public override async Task<CreateTaskReply> CreateLargeTasks(IAsyncStreamReader<CreateLargeTaskRequest> requestStream, ServerCallContext context)
  {
    var enumerator = requestStream.ReadAllAsync(context.CancellationToken).GetAsyncEnumerator(context.CancellationToken);

    if (!await enumerator.MoveNextAsync(context.CancellationToken))
      throw new RpcException(new(StatusCode.InvalidArgument,
                                 "stream contained no message"));

    var first = enumerator.Current;

    if (first.TypeCase != CreateLargeTaskRequest.TypeOneofCase.InitRequest)
      throw new RpcException(new(StatusCode.InvalidArgument,
                                 "First message in stream must be of type InitRequest"),
                             "First message in stream must be of type InitRequest");

    return await submitter_.CreateTasks(first.InitRequest.SessionId,
                                        first.InitRequest.SessionId,
                                        first.InitRequest.SessionId,
                                        first.InitRequest.TaskOptions,
                                        enumerator.BuildRequests(context.CancellationToken),
                                        context.CancellationToken);
  }

  /// <inheritdoc />
  public override Task<Count> CountTasks(TaskFilter request, ServerCallContext context)
    => submitter_.CountTasks(request,
                             context.CancellationToken);

  /// <inheritdoc />
  public override Task TryGetResultStream(ResultRequest request, IServerStreamWriter<ResultReply> responseStream, ServerCallContext context)
    => submitter_.TryGetResult(request,
                               responseStream,
                               context.CancellationToken);

  public override Task<Count> WaitForCompletion(WaitRequest request, ServerCallContext context)
    => submitter_.WaitForCompletion(request,
                                    context.CancellationToken);

  public override Task<Output> TryGetTaskOutput(ResultRequest request, ServerCallContext context)
    => submitter_.TryGetTaskOutputAsync(request,
                                        context.CancellationToken);

  public override Task<AvailabilityReply> WaitForAvailability(ResultRequest request, ServerCallContext context)
  {
    return submitter_.WaitForAvailabilityAsync(request,
                                               context.CancellationToken);
  }

  public override Task<GetStatusReply> GetStatus(GetStatusrequest request, ServerCallContext context)
  {
    return submitter_.GetStatusAsync(request,
                                     context.CancellationToken);
  }

  public override Task<TaskIdList> ListTasks(TaskFilter request, ServerCallContext context)
  {
    return submitter_.ListTasksAsync(request,
                                     context.CancellationToken);
  }
}