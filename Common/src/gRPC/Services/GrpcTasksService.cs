// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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
using System.Net.Http;
using System.Net.Sockets;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.SortDirection;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Core.Base;
using ArmoniK.Core.Base.Exceptions;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Auth.Authorization;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC.Convertors;
using ArmoniK.Core.Common.Meter;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Utils;

using Grpc.Core;

using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Logging;

using Task = ArmoniK.Api.gRPC.V1.Tasks.Tasks;
using TaskOptions = ArmoniK.Core.Base.DataStructures.TaskOptions;

namespace ArmoniK.Core.Common.gRPC.Services;

/// <inheritdoc cref="Task" />
[Authorize(AuthenticationSchemes = Authenticator.SchemeName)]
public class GrpcTasksService : Task.TasksBase
{
  private readonly HttpClient                                 httpClient_;
  private readonly ILogger<GrpcTasksService>                  logger_;
  private readonly FunctionExecutionMetrics<GrpcTasksService> meter_;
  private readonly Injection.Options.Submitter                options_;
  private readonly IPushQueueStorage                          pushQueueStorage_;
  private readonly IResultTable                               resultTable_;
  private readonly ISessionTable                              sessionTable_;
  private readonly TaskDataMask                               taskDetailedMask_;
  private readonly TaskDataMask                               taskSummaryMask_;
  private readonly ITaskTable                                 taskTable_;

  /// <summary>
  ///   Initializes a new instance of the <see cref="GrpcTasksService" /> class.
  /// </summary>
  /// <param name="taskTable">The task table for managing tasks.</param>
  /// <param name="sessionTable">The session table for managing sessions.</param>
  /// <param name="resultTable">The result table for managing task inputs and outputs.</param>
  /// <param name="pushQueueStorage">The interface to push tasks in the queue.</param>
  /// <param name="meter">The metrics for function execution.</param>
  /// <param name="httpClient">The HTTP client for making requests.</param>
  /// <param name="options">The submitter options for task submission.</param>
  /// <param name="logger">The logger for logging information.</param>
  public GrpcTasksService(ITaskTable                                 taskTable,
                          ISessionTable                              sessionTable,
                          IResultTable                               resultTable,
                          IPushQueueStorage                          pushQueueStorage,
                          FunctionExecutionMetrics<GrpcTasksService> meter,
                          HttpClient                                 httpClient,
                          Injection.Options.Submitter                options,
                          ILogger<GrpcTasksService>                  logger)
  {
    logger_           = logger;
    taskTable_        = taskTable;
    sessionTable_     = sessionTable;
    resultTable_      = resultTable;
    pushQueueStorage_ = pushQueueStorage;
    httpClient_       = httpClient;
    options_          = options;
    meter_            = meter;

    taskDetailedMask_ = new TaskDataMask(new List<TaskDataFields>
                                         {
                                           TaskDataFields.SessionId,
                                           TaskDataFields.TaskId,
                                           TaskDataFields.PayloadId,
                                           TaskDataFields.Status,
                                           TaskDataFields.InitialTaskId,
                                           TaskDataFields.Output,
                                           TaskDataFields.OwnerPodId,
                                           TaskDataFields.OwnerPodName,
                                           TaskDataFields.Options,
                                           TaskDataFields.DataDependencies,
                                           TaskDataFields.ExpectedOutputIds,
                                           TaskDataFields.RetryOfIds,
                                           TaskDataFields.ParentTaskIds,
                                           TaskDataFields.CreationDate,
                                           TaskDataFields.PodTtl,
                                           TaskDataFields.StartDate,
                                           TaskDataFields.StatusMessage,
                                           TaskDataFields.SubmittedDate,
                                           TaskDataFields.AcquisitionDate,
                                           TaskDataFields.ReceptionDate,
                                           TaskDataFields.CreationToEndDuration,
                                           TaskDataFields.ProcessingToEndDuration,
                                           TaskDataFields.EndDate,
                                           TaskDataFields.FetchedDate,
                                           TaskDataFields.ProcessedDate,
                                           TaskDataFields.CreatedBy,
                                         },
                                         new List<TaskOptionsFields>());


    taskSummaryMask_ = new TaskDataMask(new List<TaskDataFields>
                                        {
                                          TaskDataFields.SessionId,
                                          TaskDataFields.TaskId,
                                          TaskDataFields.PayloadId,
                                          TaskDataFields.Status,
                                          TaskDataFields.InitialTaskId,
                                          TaskDataFields.Output,
                                          TaskDataFields.OwnerPodId,
                                          TaskDataFields.OwnerPodName,
                                          TaskDataFields.Options,
                                          TaskDataFields.DataDependenciesCount,
                                          TaskDataFields.ExpectedOutputIdsCount,
                                          TaskDataFields.RetryOfIdsCount,
                                          TaskDataFields.ParentTaskIdsCount,
                                          TaskDataFields.CreationDate,
                                          TaskDataFields.PodTtl,
                                          TaskDataFields.StartDate,
                                          TaskDataFields.StatusMessage,
                                          TaskDataFields.SubmittedDate,
                                          TaskDataFields.AcquisitionDate,
                                          TaskDataFields.ReceptionDate,
                                          TaskDataFields.CreationToEndDuration,
                                          TaskDataFields.ProcessingToEndDuration,
                                          TaskDataFields.EndDate,
                                          TaskDataFields.FetchedDate,
                                          TaskDataFields.ProcessedDate,
                                          TaskDataFields.CreatedBy,
                                        },
                                        new List<TaskOptionsFields>());
  }

  /// <inheritdoc />
  [RequiresPermission(typeof(GrpcTasksService),
                      nameof(GetTask))]
  public override async Task<GetTaskResponse> GetTask(GetTaskRequest    request,
                                                      ServerCallContext context)
  {
    using var measure = meter_.CountAndTime();
    try
    {
      return new GetTaskResponse
             {
               Task = (await taskTable_.ReadTaskAsync(request.TaskId,
                                                      taskDetailedMask_.GetProjection(),
                                                      context.CancellationToken)
                                       .ConfigureAwait(false)).ToTaskDetailed(),
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

  /// <inheritdoc />
  [RequiresPermission(typeof(GrpcTasksService),
                      nameof(ListTasks))]
  public override async Task<ListTasksResponse> ListTasks(ListTasksRequest  request,
                                                          ServerCallContext context)
  {
    using var measure = meter_.CountAndTime();
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
                                                                taskSummaryMask_.GetProjection(),
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

  /// <inheritdoc />
  [RequiresPermission(typeof(GrpcTasksService),
                      nameof(GetResultIds))]
  public override async Task<GetResultIdsResponse> GetResultIds(GetResultIdsRequest request,
                                                                ServerCallContext   context)
  {
    using var measure = meter_.CountAndTime();
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

  /// <inheritdoc />
  [RequiresPermission(typeof(GrpcTasksService),
                      nameof(CancelTasks))]
  public override async Task<CancelTasksResponse> CancelTasks(CancelTasksRequest request,
                                                              ServerCallContext  context)
  {
    using var measure = meter_.CountAndTime();
    try
    {
      await taskTable_.CancelTaskAsync(request.TaskIds,
                                       context.CancellationToken)
                      .ConfigureAwait(false);
      var ownerPodIds = await taskTable_.FindTasksAsync(data => request.TaskIds.Contains(data.TaskId),
                                                        data => new
                                                                {
                                                                  data.OwnerPodId,
                                                                  data.TaskId,
                                                                })
                                        .ToListAsync()
                                        .ConfigureAwait(false);

      await ownerPodIds.ParallelForEach(new ParallelTaskOptions(options_.DegreeOfParallelism),
                                        async t =>
                                        {
                                          try
                                          {
                                            logger_.LogInformation("Cancel task {TaskId} on {OwnerPodId}",
                                                                   t.TaskId,
                                                                   t.OwnerPodId);

                                            await (string.IsNullOrEmpty(t.OwnerPodId)
                                                     ? System.Threading.Tasks.Task.CompletedTask
                                                     : httpClient_.GetAsync("http://" + t.OwnerPodId + ":1080/stopcancelledtask")).ConfigureAwait(false);
                                          }
                                          // Ignore unreachable agents
                                          catch (HttpRequestException e) when (e is
                                                                               {
                                                                                 InnerException: SocketException
                                                                                                 {
                                                                                                   SocketErrorCode: SocketError.ConnectionRefused,
                                                                                                 },
                                                                               })
                                          {
                                            logger_.LogError(e,
                                                             "The agent with {OwnerPodId} was not reached successfully",
                                                             t.OwnerPodId);
                                          }
                                        })
                       .ConfigureAwait(false);

      await ResultLifeCycleHelper.AbortTasksAndResults(taskTable_,
                                                       resultTable_,
                                                       request.TaskIds,
                                                       reason: $"Client requested cancellation of tasks {string.Join(", ", request.TaskIds)}",
                                                       cancellationToken: context.CancellationToken)
                                 .ConfigureAwait(false);

      return new CancelTasksResponse
             {
               Tasks =
               {
                 (await taskTable_.FindTasksAsync(data => request.TaskIds.Contains(data.TaskId),
                                                  taskSummaryMask_.GetProjection(),
                                                  context.CancellationToken)
                                  .ToListAsync(context.CancellationToken)
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

  /// <inheritdoc />
  [RequiresPermission(typeof(GrpcTasksService),
                      nameof(CountTasksByStatus))]
  public override async Task<CountTasksByStatusResponse> CountTasksByStatus(CountTasksByStatusRequest request,
                                                                            ServerCallContext         context)
  {
    using var measure = meter_.CountAndTime();
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
                                                                            Status = count.Status.ToGrpcStatus(),
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

  /// <inheritdoc />
  [RequiresPermission(typeof(GrpcTasksService),
                      nameof(ListTasksDetailed))]
  public override async Task<ListTasksDetailedResponse> ListTasksDetailed(ListTasksRequest  request,
                                                                          ServerCallContext context)
  {
    using var measure = meter_.CountAndTime();
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
                                                                taskDetailedMask_.GetProjection(),
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
                 tasks.Select(data => data.ToTaskDetailed()),
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

  /// <inheritdoc />
  [RequiresPermission(typeof(GrpcTasksService),
                      nameof(SubmitTasks))]
  public override async Task<SubmitTasksResponse> SubmitTasks(SubmitTasksRequest request,
                                                              ServerCallContext  context)
  {
    using var measure = meter_.CountAndTime();
    try
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
                                                     sessionData,
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
                                                              TaskId    = creationRequest.TaskId,
                                                              PayloadId = creationRequest.PayloadId,
                                                            }),
               },
             };
    }
    catch (SubmissionClosedException e)
    {
      logger_.LogWarning(e,
                         "Error while submitting tasks");
      throw new RpcException(new Status(StatusCode.FailedPrecondition,
                                        "Client submission is closed, no tasks can be submitted"));
    }
  }
}
