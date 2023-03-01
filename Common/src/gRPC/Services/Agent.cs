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
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Agent;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.StateMachines;
using ArmoniK.Core.Common.Storage;

using Google.Protobuf;

using Grpc.Core;

using Microsoft.Extensions.Logging;

using Result = ArmoniK.Api.gRPC.V1.Agent.Result;

namespace ArmoniK.Core.Common.gRPC.Services;

/// <summary>
///   Represents the internal processing requests received by the agent. Provides methods to process those requests
/// </summary>
public class Agent : IAgent
{
  private sealed record TaskIdPriority
  {
    public string TaskId   { get; init; } = string.Empty;
    public int    Priority { get; init; }
  }

  private readonly List<(IEnumerable<Storage.TaskRequest> requests, int priority, string partitionId)> createdTasks_;
  private readonly ILogger                                                                             logger_;
  private readonly IObjectStorage                                                                      resourcesStorage_;
  private readonly SessionData                                                                         sessionData_;
  private readonly ISubmitter                                                                          submitter_;
  private readonly IPushQueueStorage                                                                   pushQueueStorage_;
  private readonly ITaskTable                                                                          taskTable_;
  private readonly Injection.Options.DependencyResolver                                                dependencyResolverOptions_;
  private readonly TaskData                                                                            taskData_;
  private readonly string                                                                              token_;
  private readonly List<TaskIdPriority>                                                                toDependencyResolveTasks_;

  /// <summary>
  ///   Initializes a new instance of the <see cref="Agent" />
  /// </summary>
  /// <param name="submitter">Interface to manage tasks</param>
  /// <param name="objectStorageFactory">Interface class to create object storage</param>
  /// <param name="pushQueueStorage">Interface to put tasks in the queue</param>
  /// <param name="taskTable">Interface to manage task states</param>
  /// <param name="dependencyResolverOptions">Configuration for the Dependency Resolver</param>
  /// <param name="sessionData">Data of the session</param>
  /// <param name="taskData">Data of the task</param>
  /// <param name="token">Token send to the worker to identify the running task</param>
  /// <param name="logger">Logger used to produce logs for this class</param>
  public Agent(ISubmitter                           submitter,
               IObjectStorageFactory                objectStorageFactory,
               IPushQueueStorage                    pushQueueStorage,
               ITaskTable                           taskTable,
               Injection.Options.DependencyResolver dependencyResolverOptions,
               SessionData                          sessionData,
               TaskData                             taskData,
               string                               token,
               ILogger                              logger)
  {
    submitter_                 = submitter;
    pushQueueStorage_          = pushQueueStorage;
    taskTable_                 = taskTable;
    dependencyResolverOptions_ = dependencyResolverOptions;
    logger_                    = logger;
    resourcesStorage_          = objectStorageFactory.CreateResourcesStorage();
    createdTasks_              = new List<(IEnumerable<Storage.TaskRequest> requests, int priority, string partitionId)>();
    toDependencyResolveTasks_  = new List<TaskIdPriority>();
    sessionData_               = sessionData;
    taskData_                  = taskData;
    token_                     = token;
  }

  /// <inheritdoc />
  /// <exception cref="ArmoniKException"></exception>
  public async Task FinalizeTaskCreation(CancellationToken cancellationToken)
  {
    using var _ = logger_.BeginNamedScope(nameof(FinalizeTaskCreation),
                                          ("taskId", taskData_.TaskId),
                                          ("sessionId", sessionData_.SessionId));

    logger_.LogDebug("Finalize child task creation");

    foreach (var createdTask in createdTasks_)
    {
      await submitter_.FinalizeTaskCreation(createdTask.requests,
                                            createdTask.priority,
                                            taskData_.Options.PartitionId,
                                            sessionData_.SessionId,
                                            taskData_.TaskId,
                                            cancellationToken)
                      .ConfigureAwait(false);
    }

    logger_.LogDebug("Send tasks which new data are available in the dependency checker queue");

    foreach (var tasksByPriority in toDependencyResolveTasks_.GroupBy(idPriority => idPriority.Priority))
    {
      await pushQueueStorage_.PushMessagesAsync(tasksByPriority.Select(idPriority => idPriority.TaskId),
                                                dependencyResolverOptions_.UnresolvedDependenciesQueue,
                                                tasksByPriority.Key,
                                                cancellationToken)
                             .ConfigureAwait(false);
    }
  }

  /// <inheritdoc />
  public async Task<CreateTaskReply> CreateTask(IAsyncStreamReader<CreateTaskRequest> requestStream,
                                                CancellationToken                     cancellationToken)
  {
    var                                                                           fsmCreate           = new ProcessReplyCreateLargeTaskStateMachine(logger_);
    Task?                                                                         completionTask      = null;
    Channel<ReadOnlyMemory<byte>>?                                                payloadsChannel     = null;
    var                                                                           taskRequestsChannel = Channel.CreateBounded<TaskRequest>(10);
    (IEnumerable<Storage.TaskRequest> requests, int priority, string partitionId) currentTasks;
    currentTasks.requests = new List<Storage.TaskRequest>();

    using var _ = logger_.BeginNamedScope(nameof(CreateTask),
                                          ("taskId", taskData_.TaskId),
                                          ("sessionId", sessionData_.SessionId));
    await foreach (var request in requestStream.ReadAllAsync(cancellationToken)
                                               .ConfigureAwait(false))
    {
      // todo : check if using validator can do the job ?
      if (string.IsNullOrEmpty(request.CommunicationToken))
      {
        return new CreateTaskReply
               {
                 CommunicationToken = request.CommunicationToken,
                 Error              = "Missing communication token",
               };
      }

      if (request.CommunicationToken != token_)
      {
        return new CreateTaskReply
               {
                 CommunicationToken = request.CommunicationToken,
                 Error              = "Wrong communication token",
               };
      }

      switch (request.TypeCase)
      {
        case CreateTaskRequest.TypeOneofCase.InitRequest:
          fsmCreate.InitRequest();

          completionTask = Task.Run(async () =>
                                    {
                                      currentTasks = await submitter_.CreateTasks(sessionData_.SessionId,
                                                                                  taskData_.TaskId,
                                                                                  request.InitRequest.TaskOptions,
                                                                                  taskRequestsChannel.Reader.ReadAllAsync(cancellationToken),
                                                                                  cancellationToken)
                                                                     .ConfigureAwait(false);
                                      createdTasks_.Add(currentTasks);
                                    },
                                    cancellationToken);

          break;
        case CreateTaskRequest.TypeOneofCase.InitTask:

          switch (request.InitTask.TypeCase)
          {
            case InitTaskRequest.TypeOneofCase.Header:
              fsmCreate.AddHeader();
              payloadsChannel = Channel.CreateUnbounded<ReadOnlyMemory<byte>>(new UnboundedChannelOptions
                                                                              {
                                                                                SingleWriter = true,
                                                                                SingleReader = true,
                                                                              });

              await taskRequestsChannel.Writer.WriteAsync(new TaskRequest(request.InitTask.Header.ExpectedOutputKeys,
                                                                          request.InitTask.Header.DataDependencies,
                                                                          payloadsChannel.Reader.ReadAllAsync(cancellationToken)),
                                                          cancellationToken)
                                       .ConfigureAwait(false);

              break;
            case InitTaskRequest.TypeOneofCase.LastTask:
              fsmCreate.CompleteRequest();
              taskRequestsChannel.Writer.Complete();

              try
              {
                await completionTask!.WaitAsync(cancellationToken)
                                     .ConfigureAwait(false);

                logger_.LogDebug("Send successful {reply}",
                                 nameof(CreateTaskReply));

                return new CreateTaskReply
                       {
                         CreationStatusList = new CreateTaskReply.Types.CreationStatusList
                                              {
                                                CreationStatuses =
                                                {
                                                  currentTasks.requests.Select(taskRequest => new CreateTaskReply.Types.CreationStatus
                                                                                              {
                                                                                                TaskInfo = new CreateTaskReply.Types.TaskInfo
                                                                                                           {
                                                                                                             TaskId = taskRequest.Id,
                                                                                                             DataDependencies =
                                                                                                             {
                                                                                                               taskRequest.DataDependencies,
                                                                                                             },
                                                                                                             ExpectedOutputKeys =
                                                                                                             {
                                                                                                               taskRequest.ExpectedOutputKeys,
                                                                                                             },
                                                                                                           },
                                                                                              }),
                                                },
                                              },
                       };
              }
              catch (Exception e)
              {
                logger_.LogWarning(e,
                                   "Error during task creation");
                return new CreateTaskReply
                       {
                         CreationStatusList = new CreateTaskReply.Types.CreationStatusList
                                              {
                                                CreationStatuses =
                                                {
                                                  currentTasks.requests.Select(_ => new CreateTaskReply.Types.CreationStatus
                                                                                    {
                                                                                      Error = "An error occured during task creation",
                                                                                    }),
                                                },
                                              },
                       };
              }

            case InitTaskRequest.TypeOneofCase.None:
            default:
              throw new ArgumentOutOfRangeException();
          }

          break;

        case CreateTaskRequest.TypeOneofCase.TaskPayload:
          switch (request.TaskPayload.TypeCase)
          {
            case DataChunk.TypeOneofCase.Data:
              fsmCreate.AddDataChunk();
              await payloadsChannel!.Writer.WriteAsync(request.TaskPayload.Data.Memory,
                                                       cancellationToken)
                                    .ConfigureAwait(false);
              break;
            case DataChunk.TypeOneofCase.DataComplete:
              fsmCreate.CompleteData();
              payloadsChannel!.Writer.Complete();
              payloadsChannel = null;
              break;
            case DataChunk.TypeOneofCase.None:
            default:
              throw new ArgumentOutOfRangeException();
          }

          break;
        case CreateTaskRequest.TypeOneofCase.None:
        default:
          throw new ArgumentOutOfRangeException();
      }
    }

    return new CreateTaskReply();
  }

  /// <inheritdoc />
  public async Task GetCommonData(DataRequest                    request,
                                  IServerStreamWriter<DataReply> responseStream,
                                  CancellationToken              cancellationToken)
  {
    using var _ = logger_.BeginNamedScope(nameof(GetCommonData),
                                          ("taskId", taskData_.TaskId),
                                          ("sessionId", sessionData_.SessionId));
    if (string.IsNullOrEmpty(request.CommunicationToken))
    {
      await responseStream.WriteAsync(new DataReply
                                      {
                                        CommunicationToken = request.CommunicationToken,
                                        Error              = "Missing communication token",
                                      },
                                      cancellationToken)
                          .ConfigureAwait(false);
      return;
    }

    if (request.CommunicationToken != token_)
    {
      await responseStream.WriteAsync(new DataReply
                                      {
                                        CommunicationToken = request.CommunicationToken,
                                        Error              = "Wrong communication token",
                                      },
                                      cancellationToken)
                          .ConfigureAwait(false);
      return;
    }

    await responseStream.WriteAsync(new DataReply
                                    {
                                      Error = "Common data are not supported yet",
                                    },
                                    cancellationToken)
                        .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task GetDirectData(DataRequest                    request,
                                  IServerStreamWriter<DataReply> responseStream,
                                  CancellationToken              cancellationToken)
  {
    using var _ = logger_.BeginNamedScope(nameof(GetDirectData),
                                          ("taskId", taskData_.TaskId),
                                          ("sessionId", sessionData_.SessionId));

    if (string.IsNullOrEmpty(request.CommunicationToken))
    {
      await responseStream.WriteAsync(new DataReply
                                      {
                                        CommunicationToken = request.CommunicationToken,
                                        Error              = "Missing communication token",
                                      },
                                      cancellationToken)
                          .ConfigureAwait(false);
      return;
    }

    if (request.CommunicationToken != token_)
    {
      await responseStream.WriteAsync(new DataReply
                                      {
                                        CommunicationToken = request.CommunicationToken,
                                        Error              = "Wrong communication token",
                                      },
                                      cancellationToken)
                          .ConfigureAwait(false);
      return;
    }

    await responseStream.WriteAsync(new DataReply
                                    {
                                      Error = "Direct data are not supported yet",
                                    },
                                    cancellationToken)
                        .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task GetResourceData(DataRequest                    request,
                                    IServerStreamWriter<DataReply> responseStream,
                                    CancellationToken              cancellationToken)
  {
    using var _ = logger_.BeginNamedScope(nameof(GetResourceData),
                                          ("taskId", taskData_.TaskId),
                                          ("sessionId", sessionData_.SessionId));

    if (string.IsNullOrEmpty(request.CommunicationToken))
    {
      await responseStream.WriteAsync(new DataReply
                                      {
                                        CommunicationToken = request.CommunicationToken,
                                        Error              = "Missing communication token",
                                      },
                                      cancellationToken)
                          .ConfigureAwait(false);
      return;
    }

    if (request.CommunicationToken != token_)
    {
      await responseStream.WriteAsync(new DataReply
                                      {
                                        CommunicationToken = request.CommunicationToken,
                                        Error              = "Wrong communication token",
                                      },
                                      cancellationToken)
                          .ConfigureAwait(false);
      return;
    }

    try
    {
      await foreach (var data in resourcesStorage_.GetValuesAsync(request.Key,
                                                                  cancellationToken)
                                                  .ConfigureAwait(false))
      {
        await responseStream.WriteAsync(new DataReply
                                        {
                                          Data = new DataChunk
                                                 {
                                                   Data = UnsafeByteOperations.UnsafeWrap(data),
                                                 },
                                        },
                                        cancellationToken)
                            .ConfigureAwait(false);
      }

      await responseStream.WriteAsync(new DataReply
                                      {
                                        Data = new DataChunk
                                               {
                                                 DataComplete = true,
                                               },
                                      },
                                      cancellationToken)
                          .ConfigureAwait(false);
    }
    catch (ObjectDataNotFoundException)
    {
      await responseStream.WriteAsync(new DataReply
                                      {
                                        Init = new DataReply.Types.Init
                                               {
                                                 Key   = request.Key,
                                                 Error = "Key not found",
                                               },
                                      },
                                      cancellationToken)
                          .ConfigureAwait(false);
    }
  }

  /// <inheritdoc />
  public async Task<ResultReply> SendResult(IAsyncStreamReader<Result> requestStream,
                                            CancellationToken          cancellationToken)
  {
    using var _ = logger_.BeginNamedScope(nameof(SendResult),
                                          ("taskId", taskData_.TaskId),
                                          ("sessionId", sessionData_.SessionId));

    Task? completionTask       = null;
    Task? findDependenciesTask = null;
    var   fsmResult            = new ProcessReplyResultStateMachine(logger_);
    var chunksChannel = Channel.CreateUnbounded<ReadOnlyMemory<byte>>(new UnboundedChannelOptions
                                                                      {
                                                                        SingleWriter = true,
                                                                        SingleReader = true,
                                                                      });

    await foreach (var request in requestStream.ReadAllAsync(cancellationToken)
                                               .ConfigureAwait(false))
    {
      if (string.IsNullOrEmpty(request.CommunicationToken))
      {
        return new ResultReply
               {
                 CommunicationToken = request.CommunicationToken,
                 Error              = "Missing communication token",
               };
      }

      if (request.CommunicationToken != token_)
      {
        return new ResultReply
               {
                 CommunicationToken = request.CommunicationToken,
                 Error              = "Wrong communication token",
               };
      }

      switch (request.TypeCase)
      {
        case Result.TypeOneofCase.Init:
          switch (request.Init.TypeCase)
          {
            case InitKeyedDataStream.TypeOneofCase.Key:
              fsmResult.InitKey();
              completionTask = Task.Run(async () =>
                                        {
                                          await submitter_.SetResult(sessionData_.SessionId,
                                                                     taskData_.TaskId,
                                                                     request.Init.Key,
                                                                     chunksChannel.Reader.ReadAllAsync(cancellationToken),
                                                                     cancellationToken)
                                                          .ConfigureAwait(false);
                                        },
                                        cancellationToken);
              findDependenciesTask = Task.Run(async () =>
                                              {
                                                var taskFound = await taskTable_.FindTasksAsync(data => data.DataDependencies.Contains(request.Init.Key),
                                                                                                data => new TaskIdPriority
                                                                                                        {
                                                                                                          TaskId   = data.TaskId,
                                                                                                          Priority = data.Options.Priority,
                                                                                                        },
                                                                                                cancellationToken)
                                                                                .ConfigureAwait(false);
                                                toDependencyResolveTasks_.AddRange(taskFound);
                                              },
                                              cancellationToken);
              break;
            case InitKeyedDataStream.TypeOneofCase.LastResult:
              fsmResult.CompleteRequest();

              try
              {
                await completionTask!.WaitAsync(cancellationToken)
                                     .ConfigureAwait(false);
                await findDependenciesTask!.WaitAsync(cancellationToken)
                                           .ConfigureAwait(false);
                return new ResultReply
                       {
                         Ok = new Empty(),
                       };
              }
              catch (Exception e)
              {
                logger_.LogWarning(e,
                                   "Error while receiving results");
                return new ResultReply
                       {
                         Error = "Error while receiving results",
                       };
              }

            case InitKeyedDataStream.TypeOneofCase.None:
            default:
              throw new ArgumentOutOfRangeException();
          }

          break;
        case Result.TypeOneofCase.Data:
          switch (request.Data.TypeCase)
          {
            case DataChunk.TypeOneofCase.Data:
              fsmResult.AddDataChunk();
              await chunksChannel.Writer.WriteAsync(request.Data.Data.Memory,
                                                    cancellationToken)
                                 .ConfigureAwait(false);
              break;
            case DataChunk.TypeOneofCase.DataComplete:
              fsmResult.CompleteData();
              chunksChannel.Writer.Complete();
              break;

            case DataChunk.TypeOneofCase.None:
            default:
              throw new ArgumentOutOfRangeException();
          }

          break;
        case Result.TypeOneofCase.None:
        default:
          throw new ArgumentOutOfRangeException();
      }
    }

    return new ResultReply();
  }

  /// <inheritdoc />
  public void Dispose()
  {
  }
}
