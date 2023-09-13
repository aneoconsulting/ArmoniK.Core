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
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.SortDirection;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Core.Base;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Auth.Authorization;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Utils;

using Grpc.Core;

using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Logging;

using Task = ArmoniK.Api.gRPC.V1.Tasks.Tasks;
using TaskOptions = ArmoniK.Core.Base.DataStructures.TaskOptions;

namespace ArmoniK.Core.Common.gRPC.Services;

[Authorize(AuthenticationSchemes = Authenticator.SchemeName)]
public class GrpcTasksService : Task.TasksBase
{
  private readonly HttpClient                httpClient_;
  private readonly ILogger<GrpcTasksService> logger_;
  private readonly IPushQueueStorage         pushQueueStorage_;
  private readonly IResultTable              resultTable_;
  private readonly ISessionTable             sessionTable_;
  private readonly ITaskTable                taskTable_;

  public GrpcTasksService(ITaskTable                taskTable,
                          ISessionTable             sessionTable,
                          IResultTable              resultTable,
                          IPushQueueStorage         pushQueueStorage,
                          ILogger<GrpcTasksService> logger,
                          HttpClient                httpClient)
  {
    logger_           = logger;
    taskTable_        = taskTable;
    sessionTable_     = sessionTable;
    resultTable_      = resultTable;
    pushQueueStorage_ = pushQueueStorage;
    httpClient_       = httpClient;
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
               Task = await taskTable_.ReadTaskAsync(request.TaskId,
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
      if (request.Sort is not null && request.Sort.Field.FieldCase == TaskField.FieldOneofCase.TaskOptionGenericField)
      {
        logger_.LogWarning("Sorting on the field {field} is not advised because this field is not part of ArmoniK data schema.",
                           request.Sort.Field.TaskOptionGenericField.Field);
      }

      var (tasks, totalCount) = await taskTable_.ListTasksAsync(request.Filters is null
                                                                  ? data => true
                                                                  : request.Filters.ToTaskDataFilter(),
                                                                request.Sort is null
                                                                  ? data => data.TaskId
                                                                  : request.Sort.ToField(),
                                                                data => new TaskDataSummary(data.SessionId,
                                                                                            data.TaskId,
                                                                                            data.OwnerPodId,
                                                                                            data.OwnerPodName,
                                                                                            data.ParentTaskIds.Count,
                                                                                            data.DataDependencies.Count,
                                                                                            data.ExpectedOutputIds.Count,
                                                                                            data.InitialTaskId,
                                                                                            data.RetryOfIds.Count,
                                                                                            data.Status,
                                                                                            data.StatusMessage,
                                                                                            data.Options,
                                                                                            data.CreationDate,
                                                                                            data.SubmittedDate,
                                                                                            data.StartDate,
                                                                                            data.EndDate,
                                                                                            data.ReceptionDate,
                                                                                            data.AcquisitionDate,
                                                                                            data.PodTtl,
                                                                                            data.ProcessingToEndDuration,
                                                                                            data.CreationToEndDuration,
                                                                                            data.Output),
                                                                request.Sort is null || request.Sort.Direction == SortDirection.Asc,
                                                                request.Page,
                                                                request.PageSize,
                                                                context.CancellationToken)
                                                .ConfigureAwait(false);

      return new ListTasksResponse
             {
               Page     = request.Page,
               PageSize = request.PageSize,
               Tasks =
               {
                 tasks.Select(data => data.ToTaskSummary()),
               },
               Total = (int)totalCount,
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

  [RequiresPermission(typeof(GrpcTasksService),
                      nameof(CancelTasks))]
  public override async Task<CancelTasksResponse> CancelTasks(CancelTasksRequest request,
                                                              ServerCallContext  context)
  {
    try
    {
      await taskTable_.CancelTaskAsync(request.TaskIds,
                                       context.CancellationToken)
                      .ConfigureAwait(false);
      var ownerPodIds = await taskTable_.FindTasksAsync(data => request.TaskIds.Contains(data.TaskId),
                                                        data => data.OwnerPodId)
                                        .ToListAsync()
                                        .ConfigureAwait(false);
      await ownerPodIds.ParallelForEach(new ParallelTaskOptions(10),
                                        async ownerPodId => await httpClient_.GetAsync("http://" + ownerPodId + ":1080/stopcancelledtask")
                                                                             .ConfigureAwait(false))
                       .ConfigureAwait(false);
      return new CancelTasksResponse
             {
               Tasks =
               {
                 (await taskTable_.FindTasksAsync(data => request.TaskIds.Contains(data.TaskId),
                                                  data => new TaskDataSummary(data.SessionId,
                                                                              data.TaskId,
                                                                              data.OwnerPodId,
                                                                              data.OwnerPodName,
                                                                              data.ParentTaskIds.Count,
                                                                              data.DataDependencies.Count,
                                                                              data.ExpectedOutputIds.Count,
                                                                              data.InitialTaskId,
                                                                              data.RetryOfIds.Count,
                                                                              data.Status,
                                                                              data.StatusMessage,
                                                                              data.Options,
                                                                              data.CreationDate,
                                                                              data.SubmittedDate,
                                                                              data.StartDate,
                                                                              data.EndDate,
                                                                              data.ReceptionDate,
                                                                              data.AcquisitionDate,
                                                                              data.PodTtl,
                                                                              data.ProcessingToEndDuration,
                                                                              data.CreationToEndDuration,
                                                                              data.Output))
                                  .ConfigureAwait(false)).Select(data => data.ToTaskSummary()),
               },
             };
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                         "Error while cancelling tasks");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see application logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                         "Error while cancelling tasks");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see application logs"));
    }
  }

  [RequiresPermission(typeof(GrpcTasksService),
                      nameof(CountTasksByStatus))]
  public override async Task<CountTasksByStatusResponse> CountTasksByStatus(CountTasksByStatusRequest request,
                                                                            ServerCallContext         context)
  {
    try
    {
      return new CountTasksByStatusResponse
             {
               Status =
               {
                 (await taskTable_.CountTasksAsync(request.Filters is null
                                                     ? data => true
                                                     : request.Filters.ToTaskDataFilter(),
                                                   context.CancellationToken)
                                  .ConfigureAwait(false)).Select(count => new StatusCount
                                                                          {
                                                                            Status = count.Status,
                                                                            Count  = count.Count,
                                                                          }),
               },
             };
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                         "Error while counting tasks");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see application logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                         "Error while counting tasks");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see application logs"));
    }
  }

  [RequiresPermission(typeof(GrpcTasksService),
                      nameof(ListTasksDetailed))]
  public override async Task<ListTasksDetailedResponse> ListTasksDetailed(ListTasksRequest  request,
                                                                          ServerCallContext context)
  {
    try
    {
      if (request.Sort is not null && request.Sort.Field.FieldCase == TaskField.FieldOneofCase.TaskOptionGenericField)
      {
        logger_.LogWarning("Sorting on the field {field} is not advised because this field is not part of ArmoniK data schema.",
                           request.Sort.Field.TaskOptionGenericField.Field);
      }

      var (tasks, totalCount) = await taskTable_.ListTasksAsync(request.Filters is null
                                                                  ? data => true
                                                                  : request.Filters.ToTaskDataFilter(),
                                                                request.Sort is null
                                                                  ? data => data.TaskId
                                                                  : request.Sort.ToField(),
                                                                data => data,
                                                                request.Sort is null || request.Sort.Direction == SortDirection.Asc,
                                                                request.Page,
                                                                request.PageSize,
                                                                context.CancellationToken)
                                                .ConfigureAwait(false);

      return new ListTasksDetailedResponse
             {
               Page     = request.Page,
               PageSize = request.PageSize,
               Tasks =
               {
                 tasks.Select(data => new TaskDetailed(data)),
               },
               Total = (int)totalCount,
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
                      nameof(SubmitTasks))]
  public override async Task<SubmitTasksResponse> SubmitTasks(SubmitTasksRequest request,
                                                              ServerCallContext  context)
  {
    var sessionData = await sessionTable_.GetSessionAsync(request.SessionId,
                                                          context.CancellationToken)
                                         .ConfigureAwait(false);


    var submissionOptions = TaskLifeCycleHelper.ValidateSession(sessionData,
                                                                request.TaskOptions.ToNullableTaskOptions(),
                                                                request.SessionId,
                                                                pushQueueStorage_.MaxPriority,
                                                                logger_,
                                                                context.CancellationToken);

    var creationRequests = request.TaskCreations.Select(creation => new TaskCreationRequest(Guid.NewGuid()
                                                                                                .ToString(),
                                                                                            creation.PayloadId,
                                                                                            TaskOptions.Merge(creation.TaskOptions.ToNullableTaskOptions(),
                                                                                                              submissionOptions),
                                                                                            creation.ExpectedOutputKeys,
                                                                                            creation.DataDependencies))
                                  .ToList();


    await TaskLifeCycleHelper.CreateTasks(taskTable_,
                                          resultTable_,
                                          request.SessionId,
                                          request.SessionId,
                                          creationRequests,
                                          logger_,
                                          context.CancellationToken)
                             .ConfigureAwait(false);

    await TaskLifeCycleHelper.FinalizeTaskCreation(taskTable_,
                                                   resultTable_,
                                                   pushQueueStorage_,
                                                   creationRequests,
                                                   request.SessionId,
                                                   request.SessionId,
                                                   logger_,
                                                   context.CancellationToken)
                             .ConfigureAwait(false);

    return new SubmitTasksResponse
           {
             TaskInfos =
             {
               creationRequests.Select(creationRequest => new SubmitTasksResponse.Types.TaskInfo
                                                          {
                                                            DataDependencies =
                                                            {
                                                              creationRequest.DataDependencies,
                                                            },
                                                            ExpectedOutputIds =
                                                            {
                                                              creationRequest.ExpectedOutputKeys,
                                                            },
                                                            TaskId = creationRequest.TaskId,
                                                          }),
             },
           };
  }
}
