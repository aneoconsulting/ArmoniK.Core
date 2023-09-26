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
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Auth.Authorization;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC.Convertors;
using ArmoniK.Core.Common.Storage;

using Grpc.Core;

using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Logging;

using Output = ArmoniK.Api.gRPC.V1.Output;

namespace ArmoniK.Core.Common.gRPC.Services;

[Authorize(AuthenticationSchemes = Authenticator.SchemeName)]
public class GrpcSubmitterService : Api.gRPC.V1.Submitter.Submitter.SubmitterBase
{
  private readonly ILogger<GrpcSubmitterService> logger_;
  private readonly IResultTable                  resultTable_;
  private readonly ISessionTable                 sessionTable_;
  private readonly ISubmitter                    submitter_;
  private readonly ITaskTable                    taskTable_;

  public GrpcSubmitterService(ISubmitter                    submitter,
                              ITaskTable                    taskTable,
                              ISessionTable                 sessionTable,
                              IResultTable                  resultTable,
                              ILogger<GrpcSubmitterService> logger)
  {
    submitter_    = submitter;
    taskTable_    = taskTable;
    sessionTable_ = sessionTable;
    resultTable_  = resultTable;
    logger_       = logger;
  }


  /// <inheritdoc />
  [RequiresPermission(typeof(GrpcSubmitterService),
                      nameof(GetServiceConfiguration))]
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

  [RequiresPermission(typeof(GrpcSubmitterService),
                      nameof(CancelSession))]
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

  [RequiresPermission(typeof(GrpcSubmitterService),
                      nameof(CancelTasks))]
  public override async Task<Empty> CancelTasks(TaskFilter        request,
                                                ServerCallContext context)
  {
    try
    {
      logger_.LogTrace("request received {request}",
                       request);
      await taskTable_.CancelTasks(request,
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
  [RequiresPermission(typeof(GrpcSubmitterService),
                      nameof(CreateSession))]
  public override async Task<CreateSessionReply> CreateSession(CreateSessionRequest request,
                                                               ServerCallContext    context)
  {
    try
    {
      return await submitter_.CreateSession(request.PartitionIds,
                                            request.DefaultTaskOption.ToTaskOptions(),
                                            context.CancellationToken)
                             .ConfigureAwait(false);
    }
    catch (PartitionNotFoundException e)
    {
      logger_.LogWarning(e,
                         "Partition not found while creating session");
      throw new RpcException(new Status(StatusCode.InvalidArgument,
                                        "Partition not found"));
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

  [RequiresPermission(typeof(GrpcSubmitterService),
                      nameof(CreateSmallTasks))]
  public override async Task<CreateTaskReply> CreateSmallTasks(CreateSmallTaskRequest request,
                                                               ServerCallContext      context)
  {
    try
    {
      var requests = await submitter_.CreateTasks(request.SessionId,
                                                  request.SessionId,
                                                  request.TaskOptions.ToTaskOptions(),
                                                  request.TaskRequests.ToAsyncEnumerable()
                                                         .Select(taskRequest => new TaskRequest(taskRequest.ExpectedOutputKeys,
                                                                                                taskRequest.DataDependencies,
                                                                                                new[]
                                                                                                {
                                                                                                  taskRequest.Payload.Memory,
                                                                                                }.ToAsyncEnumerable())),
                                                  context.CancellationToken)
                                     .ConfigureAwait(false);

      await submitter_.FinalizeTaskCreation(requests,
                                            request.SessionId,
                                            request.SessionId,
                                            context.CancellationToken)
                      .ConfigureAwait(false);

      return new CreateTaskReply
             {
               CreationStatusList = new CreateTaskReply.Types.CreationStatusList
                                    {
                                      CreationStatuses =
                                      {
                                        requests.Select(taskRequest => new CreateTaskReply.Types.CreationStatus
                                                                       {
                                                                         TaskInfo = new CreateTaskReply.Types.TaskInfo
                                                                                    {
                                                                                      TaskId = taskRequest.TaskId,
                                                                                    },
                                                                       }),
                                      },
                                    },
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
  [RequiresPermission(typeof(GrpcSubmitterService),
                      nameof(CreateLargeTasks))]
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

      var requests = await submitter_.CreateTasks(first.InitRequest.SessionId,
                                                  first.InitRequest.SessionId,
                                                  first.InitRequest.TaskOptions.ToNullableTaskOptions(),
                                                  enumerator.BuildRequests(context.CancellationToken),
                                                  context.CancellationToken)
                                     .ConfigureAwait(false);

      await submitter_.FinalizeTaskCreation(requests,
                                            first.InitRequest.SessionId,
                                            first.InitRequest.SessionId,
                                            context.CancellationToken)
                      .ConfigureAwait(false);

      return new CreateTaskReply
             {
               CreationStatusList = new CreateTaskReply.Types.CreationStatusList
                                    {
                                      CreationStatuses =
                                      {
                                        requests.Select(taskRequest => new CreateTaskReply.Types.CreationStatus
                                                                       {
                                                                         TaskInfo = new CreateTaskReply.Types.TaskInfo
                                                                                    {
                                                                                      TaskId = taskRequest.TaskId,
                                                                                      DataDependencies =
                                                                                      {
                                                                                        taskRequest.DataDependencies,
                                                                                      },
                                                                                      ExpectedOutputKeys =
                                                                                      {
                                                                                        taskRequest.ExpectedOutputKeys,
                                                                                      },
                                                                                      PayloadId = taskRequest.PayloadId,
                                                                                    },
                                                                       }),
                                      },
                                    },
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
  [RequiresPermission(typeof(GrpcSubmitterService),
                      nameof(CountTasks))]
  public override async Task<Count> CountTasks(TaskFilter        request,
                                               ServerCallContext context)
  {
    try
    {
      return new Count
             {
               Values =
               {
                 (await taskTable_.CountTasksAsync(request,
                                                   context.CancellationToken)
                                  .ConfigureAwait(false)).Select(count => new StatusCount(count)),
               },
             };
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
  [RequiresPermission(typeof(GrpcSubmitterService),
                      nameof(TryGetResultStream))]
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
  [RequiresPermission(typeof(GrpcSubmitterService),
                      nameof(WaitForCompletion))]
  public override async Task<Count> WaitForCompletion(WaitRequest       request,
                                                      ServerCallContext context)
  {
    try
    {
      return await submitter_.WaitForCompletion(request,
                                                context.CancellationToken)
                             .ConfigureAwait(false);
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

  [RequiresPermission(typeof(GrpcSubmitterService),
                      nameof(TryGetTaskOutput))]
  public override async Task<Output> TryGetTaskOutput(TaskOutputRequest request,
                                                      ServerCallContext context)
  {
    try
    {
      return await taskTable_.GetTaskOutput(request.TaskId,
                                            context.CancellationToken)
                             .ConfigureAwait(false);
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

  [RequiresPermission(typeof(GrpcSubmitterService),
                      nameof(WaitForAvailability))]
  [Obsolete($"{nameof(ISubmitter.WaitForAvailabilityAsync)} is obsolete")]
  public override async Task<AvailabilityReply> WaitForAvailability(ResultRequest     request,
                                                                    ServerCallContext context)
  {
    try
    {
      return await submitter_.WaitForAvailabilityAsync(request,
                                                       context.CancellationToken)
                             .ConfigureAwait(false);
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

  [RequiresPermission(typeof(GrpcSubmitterService),
                      nameof(GetTaskStatus))]
  public override async Task<GetTaskStatusReply> GetTaskStatus(GetTaskStatusRequest request,
                                                               ServerCallContext    context)
  {
    try
    {
      return new GetTaskStatusReply
             {
               IdStatuses =
               {
                 (await taskTable_.GetTaskStatus(request.TaskIds,
                                                 context.CancellationToken)
                                  .ConfigureAwait(false)).Select(status => new GetTaskStatusReply.Types.IdStatus
                                                                           {
                                                                             Status = status.Status.ToGrpcStatus(),
                                                                             TaskId = status.TaskId,
                                                                           }),
               },
             };
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

  [RequiresPermission(typeof(GrpcSubmitterService),
                      nameof(GetResultStatus))]
  [Obsolete($"{nameof(Api.gRPC.V1.Submitter.Submitter.SubmitterBase.GetResultStatus)} is obsolete")]
  public override async Task<GetResultStatusReply> GetResultStatus(GetResultStatusRequest request,
                                                                   ServerCallContext      context)
  {
    try
    {
      return new GetResultStatusReply
             {
               IdStatuses =
               {
                 (await resultTable_.GetResultStatus(request.ResultIds,
                                                     request.SessionId,
                                                     context.CancellationToken)
                                    .ConfigureAwait(false)).Select(status => new GetResultStatusReply.Types.IdStatus
                                                                             {
                                                                               ResultId = status.ResultId,
                                                                               Status   = status.Status.ToGrpcStatus(),
                                                                             }),
               },
             };
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

  [RequiresPermission(typeof(GrpcSubmitterService),
                      nameof(ListTasks))]
  public override async Task<TaskIdList> ListTasks(TaskFilter        request,
                                                   ServerCallContext context)
  {
    try
    {
      return new TaskIdList
             {
               TaskIds =
               {
                 await taskTable_.ListTasksAsync(request,
                                                 context.CancellationToken)
                                 .ToListAsync()
                                 .ConfigureAwait(false),
               },
             };
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

  [RequiresPermission(typeof(GrpcSubmitterService),
                      nameof(ListSessions))]
  public override async Task<SessionIdList> ListSessions(SessionFilter     request,
                                                         ServerCallContext context)
  {
    try
    {
      return new SessionIdList
             {
               SessionIds =
               {
                 await sessionTable_.ListSessionsAsync(request,
                                                       context.CancellationToken)
                                    .ToListAsync()
                                    .ConfigureAwait(false),
               },
             };
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
