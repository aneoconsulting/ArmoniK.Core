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
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Common.Exceptions;

using Grpc.Core;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.gRPC.Services;

public class GrpcSubmitterService : Api.gRPC.V1.Submitter.Submitter.SubmitterBase
{
  private readonly ISubmitter                    submitter_;
  private readonly ILogger<GrpcSubmitterService> logger_;

  public GrpcSubmitterService(ISubmitter                    submitter,
                              ILogger<GrpcSubmitterService> logger)
  {
    submitter_ = submitter;
    logger_    = logger;
  }


  /// <inheritdoc />
  public override async Task<Configuration> GetServiceConfiguration(Empty             request,
                                                              ServerCallContext context)
  {
    try
    {
      return await submitter_.GetServiceConfiguration(request,
                                                      context.CancellationToken)
                             .ConfigureAwait(false);
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                       "Error while getting service configuration");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see Submitter logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                       "Error while getting service configuration");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see Submitter logs"));
    }
  }

  public override async Task<Empty> CancelSession(Session           request,
                                                  ServerCallContext context)
  {
    try
    {

      await submitter_.CancelSession(request.Id,
                                     context.CancellationToken)
                      .ConfigureAwait(false);
      return new Empty();
    }
    catch (SessionNotFoundException e)
    {
      logger_.LogWarning(e,
                       "Error while canceling session");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Session not found"));
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                       "Error while canceling session");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see Submitter logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                       "Error while canceling session");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see Submitter logs"));
    }
  }

  public override async Task<Empty> CancelTasks(TaskFilter        request,
                                                ServerCallContext context)
  {
    try
    {
      logger_.LogTrace("request received {request}",
                       request);
      await submitter_.CancelTasks(request,
                                   context.CancellationToken)
                      .ConfigureAwait(false);
      return new Empty();
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                       "Error while canceling tasks");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see Submitter logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                       "Error while canceling tasks");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see Submitter logs"));
    }
  }

  /// <inheritdoc />
  public override Task<CreateSessionReply> CreateSession(CreateSessionRequest request,
                                                         ServerCallContext    context)
  {
    try
    {
      return submitter_.CreateSession(request.Id,
                                      request.PartitionIds,
                                      request.DefaultTaskOption,
                                      context.CancellationToken);
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                       "Error while creating session");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see Submitter logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                       "Error while creating session");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see Submitter logs"));
    }
  }

  public override async Task<CreateTaskReply> CreateSmallTasks(CreateSmallTaskRequest request,
                                                               ServerCallContext      context)
  {
    try
    {
      var tuple = await submitter_.CreateTasks(request.SessionId,
                                               request.SessionId,
                                               request.TaskOptions,
                                               request.TaskRequests.ToAsyncEnumerable()
                                                      .Select(taskRequest => new TaskRequest(taskRequest.Id,
                                                                                             taskRequest.ExpectedOutputKeys,
                                                                                             taskRequest.DataDependencies,
                                                                                             new[]
                                                                                             {
                                                                                               taskRequest.Payload.Memory,
                                                                                             }.ToAsyncEnumerable())),
                                               context.CancellationToken)
                                  .ConfigureAwait(false);

      await submitter_.FinalizeTaskCreation(tuple.requests,
                                            tuple.priority,
                                            request.SessionId,
                                            request.SessionId,
                                            context.CancellationToken)
                      .ConfigureAwait(false);

      return new CreateTaskReply
             {
               Successfull = new Empty(),
             };
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                       "Error while creating tasks");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see Submitter logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                       "Error while creating tasks");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see Submitter logs"));
    }
  }


  /// <inheritdoc />
  public override async Task<CreateTaskReply> CreateLargeTasks(IAsyncStreamReader<CreateLargeTaskRequest> requestStream,
                                                               ServerCallContext                          context)
  {
    try
    {
      var enumerator = requestStream.ReadAllAsync(context.CancellationToken)
                                    .GetAsyncEnumerator(context.CancellationToken);

      if (!await enumerator.MoveNextAsync(context.CancellationToken)
                           .ConfigureAwait(false))
      {
        throw new RpcException(new Status(StatusCode.InvalidArgument,
                                          "stream contained no message"));
      }

      var first = enumerator.Current;

      if (first.TypeCase != CreateLargeTaskRequest.TypeOneofCase.InitRequest)
      {
        throw new RpcException(new Status(StatusCode.InvalidArgument,
                                          "First message in stream must be of type InitRequest"),
                               "First message in stream must be of type InitRequest");
      }

      var tuple = await submitter_.CreateTasks(first.InitRequest.SessionId,
                                               first.InitRequest.SessionId,
                                               first.InitRequest.TaskOptions,
                                               enumerator.BuildRequests(context.CancellationToken),
                                               context.CancellationToken)
                                  .ConfigureAwait(false);

      await submitter_.FinalizeTaskCreation(tuple.requests,
                                            tuple.priority,
                                            first.InitRequest.SessionId,
                                            first.InitRequest.SessionId,
                                            context.CancellationToken)
                      .ConfigureAwait(false);

      return new CreateTaskReply
             {
               Successfull = new Empty(),
             };
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                       "Error while creating tasks");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see Submitter logs"));
    }
    catch (RpcException e)
    {
      logger_.LogWarning(e,
                       "Error while creating tasks");
      throw;
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                       "Error while creating tasks");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see Submitter logs"));
    }
  }

  /// <inheritdoc />
  public override Task<Count> CountTasks(TaskFilter        request,
                                         ServerCallContext context)
  {
    try
    {
      return submitter_.CountTasks(request,
                                   context.CancellationToken);
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                       "Error while counting tasks");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see Submitter logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                       "Error while counting tasks");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see Submitter logs"));
    }
  }

  /// <inheritdoc />
  public override async Task TryGetResultStream(ResultRequest                    request,
                                          IServerStreamWriter<ResultReply> responseStream,
                                          ServerCallContext                context)
  {
    try
    {
      await submitter_.TryGetResult(request,
                                    responseStream,
                                    context.CancellationToken)
                      .ConfigureAwait(false);
    }
    catch (TaskNotFoundException e)
    {
      logger_.LogWarning(e,
                       "Error while getting results");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Task not found"));
    }
    catch (ResultNotFoundException e)
    {
      logger_.LogWarning(e,
                       "Error while getting results");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Result not found"));
    }
    catch (ObjectDataNotFoundException e)
    {
      logger_.LogWarning(e,
                       "Error while getting results");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Result data not found"));
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                       "Error while getting results");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see Submitter logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                       "Error while getting results");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see Submitter logs"));
    }
  }

  /// <inheritdoc />
  public override Task<Count> WaitForCompletion(WaitRequest       request,
                                                ServerCallContext context)
  {
    try
    {
      return submitter_.WaitForCompletion(request,
                                          context.CancellationToken);
    }
    catch (TaskNotFoundException e)
    {
      logger_.LogWarning(e,
                       "Error while waiting for completion");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Task not found"));
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                       "Error while waiting for completion");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see Submitter logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                       "Error while waiting for completion");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see Submitter logs"));
    }
  }

  public override Task<Output> TryGetTaskOutput(ResultRequest     request,
                                                ServerCallContext context)
  {
    try
    {

      return submitter_.TryGetTaskOutputAsync(request,
                                              context.CancellationToken);
    }
    catch (TaskNotFoundException e)
    {
      logger_.LogWarning(e,
                       "Error while getting output");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Task not found"));
    }
    catch (ResultNotFoundException e)
    {
      logger_.LogWarning(e,
                       "Error while getting output");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Result not found"));
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                       "Error while getting output");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see Submitter logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                       "Error while getting output");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see Submitter logs"));
    }
  }

  public override Task<AvailabilityReply> WaitForAvailability(ResultRequest     request,
                                                              ServerCallContext context)
  {
    try
    {
      return submitter_.WaitForAvailabilityAsync(request,
                                                 context.CancellationToken);
    }
    catch (TaskNotFoundException e)
    {
      logger_.LogWarning(e,
                       "Error while waiting for availability");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Task not found"));
    }
    catch (ResultNotFoundException e)
    {
      logger_.LogWarning(e,
                       "Error while waiting for availability");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Result not found"));
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                       "Error while waiting for availability");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see Submitter logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                       "Error while waiting for availability");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see Submitter logs"));
    }
  }

  public override Task<GetTaskStatusReply> GetTaskStatus(GetTaskStatusRequest request,
                                                         ServerCallContext    context)
  {
    try
    {
      return submitter_.GetTaskStatusAsync(request,
                                           context.CancellationToken);
    }
    catch (TaskNotFoundException e)
    {
      logger_.LogWarning(e,
                         "Error while getting status");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Task not found"));
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                         "Error while getting status");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see Submitter logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                         "Error while getting status");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see Submitter logs"));
    }
  }

  public override Task<GetResultStatusReply> GetResultStatus(GetResultStatusRequest request,
                                                             ServerCallContext      context)
  {
    try
    {
      return submitter_.GetResultStatusAsync(request,
                                             context.CancellationToken);
    }
    catch (ResultNotFoundException e)
    {
      logger_.LogWarning(e,
                         "Error while getting status");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Result not found"));
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                         "Error while getting status");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see Submitter logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                         "Error while getting status");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see Submitter logs"));
    }
  }

  public override Task<TaskIdList> ListTasks(TaskFilter        request,
                                             ServerCallContext context)
  {
    try
    {
      return submitter_.ListTasksAsync(request,
                                       context.CancellationToken);
    }
    catch (TaskNotFoundException e)
    {
      logger_.LogWarning(e,
                       "Error while listing tasks");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Task not found"));
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                       "Error while listing tasks");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see Submitter logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                       "Error while listing tasks");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see Submitter logs"));
    }
  }

  public override Task<SessionIdList> ListSessions(SessionFilter     request,
                                                 ServerCallContext context)
  {
    try
    {
      return submitter_.ListSessionsAsync(request,
                                          context.CancellationToken);
    }
    catch (SessionNotFoundException e)
    {
      logger_.LogWarning(e,
                         "Error while listing sessions");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Session not found"));
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                         "Error while listing sessions");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see Submitter logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                         "Error while listing sessions");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see Submitter logs"));
    }
  }
}
