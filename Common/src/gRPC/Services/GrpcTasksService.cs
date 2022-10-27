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
// but WITHOUT ANY WARRANTY, without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.


using System;
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Auth.Authorization;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using Grpc.Core;

using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Logging;

using Task = ArmoniK.Api.gRPC.V1.Tasks.Task;

namespace ArmoniK.Core.Common.gRPC.Services;

[Authorize(AuthenticationSchemes = Authenticator.SchemeName)]
public class GrpcTasksService : Tasks.TasksBase
{
  private readonly ILogger<GrpcTasksService> logger_;
  private readonly ITaskTable                taskTable_;

  public GrpcTasksService(ITaskTable                taskTable,
                          ILogger<GrpcTasksService> logger)
  {
    logger_    = logger;
    taskTable_ = taskTable;
  }

  [RequiresPermission(typeof(GrpcTasksService),
                      nameof(GetTask))]
  public override async Task<GetTaskResponse> GetTask(GetTaskRequest    request,
                                                      ServerCallContext context)
  {
    try
    {
      return new GetTaskResponse
             {
               Task = await taskTable_.ReadTaskAsync(request.Id,
                                                     context.CancellationToken)
                                      .ConfigureAwait(false),
             };
    }
    catch (TaskNotFoundException e)
    {
      logger_.LogWarning(e,
                         "Error while getting task");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Task not found"));
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                         "Error while getting task");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see application logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                         "Error while getting task");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see application logs"));
    }
  }

  [RequiresPermission(typeof(GrpcTasksService),
                      nameof(ListTasks))]
  public override async Task<ListTasksResponse> ListTasks(ListTasksRequest  request,
                                                          ServerCallContext context)
  {
    try
    {
      var taskData = await taskTable_.ListTasksAsync(request,
                                                     context.CancellationToken)
                                     .ConfigureAwait(false);

      return new ListTasksResponse
             {
               Page     = request.Page,
               PageSize = request.PageSize,
               Tasks =
               {
                 taskData.Select(data => new Task(data)),
               },
             };
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                         "Error while listing tasks");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see application logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                         "Error while listing tasks");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see application logs"));
    }
  }

  [RequiresPermission(typeof(GrpcTasksService),
                      nameof(GetResultIds))]
  public override async Task<GetResultIdsResponse> GetResultIds(GetResultIdsRequest request,
                                                                ServerCallContext   context)
  {
    try
    {
      return new GetResultIdsResponse
             {
               TaskResults =
               {
                 await taskTable_.GetTasksExpectedOutputKeys(request.TaskId,
                                                             context.CancellationToken)
                                 .Select(r => new GetResultIdsResponse.Types.MapTaskResult
                                              {
                                                TaskId = r.taskId,
                                                ResultIds =
                                                {
                                                  r.expectedOutputKeys,
                                                },
                                              })
                                 .ToListAsync(context.CancellationToken)
                                 .ConfigureAwait(false),
               },
             };
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                         "Error while getting results ids from tasks");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see application logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                         "Error while getting results ids from tasks");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see application logs"));
    }
  }
}
