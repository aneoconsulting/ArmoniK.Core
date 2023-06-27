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
using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.StateMachines;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Utils;

using Google.Protobuf;

using Grpc.Core;

using Microsoft.Extensions.Logging;

using static Google.Protobuf.WellKnownTypes.Timestamp;

using Result = ArmoniK.Api.gRPC.V1.Agent.Result;
using TaskOptions = ArmoniK.Core.Base.DataStructures.TaskOptions;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.gRPC.Services;

/// <summary>
///   Represents the internal processing requests received by the agent. Provides methods to process those requests
/// </summary>
public class Agent : IAgent
{
  private readonly List<TaskCreationRequest> createdTasks_;
  private readonly ILogger                   logger_;
  private readonly IObjectStorage            objectStorage_;
  private readonly IPushQueueStorage         pushQueueStorage_;
  private readonly IResultTable              resultTable_;
  private readonly List<string>              sentResults_;
  private readonly SessionData               sessionData_;
  private readonly ISubmitter                submitter_;
  private readonly TaskData                  taskData_;
  private readonly ITaskTable                taskTable_;
  private readonly string                    token_;

  /// <summary>
  ///   Initializes a new instance of the <see cref="Agent" />
  /// </summary>
  /// <param name="submitter">Interface to manage tasks</param>
  /// <param name="objectStorage">Interface class to manage tasks data</param>
  /// <param name="pushQueueStorage">Interface to put tasks in the queue</param>
  /// <param name="resultTable">Interface to manage result states</param>
  /// <param name="taskTable">Interface to manage task states</param>
  /// <param name="sessionData">Data of the session</param>
  /// <param name="taskData">Data of the task</param>
  /// <param name="token">Token send to the worker to identify the running task</param>
  /// <param name="logger">Logger used to produce logs for this class</param>
  public Agent(ISubmitter        submitter,
               IObjectStorage    objectStorage,
               IPushQueueStorage pushQueueStorage,
               IResultTable      resultTable,
               ITaskTable        taskTable,
               SessionData       sessionData,
               TaskData          taskData,
               string            token,
               ILogger           logger)
  {
    submitter_        = submitter;
    objectStorage_    = objectStorage;
    pushQueueStorage_ = pushQueueStorage;
    resultTable_      = resultTable;
    taskTable_        = taskTable;
    logger_           = logger;
    createdTasks_     = new List<TaskCreationRequest>();
    sentResults_      = new List<string>();
    sessionData_      = sessionData;
    taskData_         = taskData;
    token_            = token;
  }

  /// <inheritdoc />
  /// <exception cref="ArmoniKException"></exception>
  public async Task FinalizeTaskCreation(CancellationToken cancellationToken)
  {
    using var _ = logger_.BeginNamedScope(nameof(FinalizeTaskCreation),
                                          ("taskId", taskData_.TaskId),
                                          ("sessionId", sessionData_.SessionId));

    logger_.LogDebug("Finalize child task creation");

    await submitter_.FinalizeTaskCreation(createdTasks_,
                                          sessionData_.SessionId,
                                          taskData_.TaskId,
                                          cancellationToken)
                    .ConfigureAwait(false);

    foreach (var result in sentResults_)
    {
      await resultTable_.CompleteResult(taskData_.SessionId,
                                        result,
                                        cancellationToken)
                        .ConfigureAwait(false);
    }

    logger_.LogDebug("Submit tasks which new data are available");

    // Get all tasks that depend on the results that were completed by the current task (removing duplicates)
    var dependentTasks = await resultTable_.GetResults(sessionData_.SessionId,
                                                       sentResults_,
                                                       cancellationToken)
                                           .SelectMany(result => result.DependentTasks.ToAsyncEnumerable())
                                           .ToHashSetAsync(cancellationToken)
                                           .ConfigureAwait(false);

    if (!dependentTasks.Any())
    {
      return;
    }

    if (logger_.IsEnabled(LogLevel.Debug))
    {
      logger_.LogDebug("Dependent Tasks Dictionary {@dependents}",
                       dependentTasks);
    }

    // Remove all results that were completed by the current task from their dependents.
    // This will try to remove more results than strictly necessary.
    // This is completely safe and should be optimized by the DB.
    await taskTable_.RemoveRemainingDataDependenciesAsync(dependentTasks,
                                                          sentResults_,
                                                          cancellationToken)
                    .ConfigureAwait(false);

    // Find all tasks whose dependencies are now complete in order to start them.
    // Multiple agents can see the same task as ready and will try to start it multiple times.
    // This is benign as it will be handled during dequeue with message deduplication.
    var groups = (await taskTable_.FindTasksAsync(data => dependentTasks.Contains(data.TaskId) && data.Status == TaskStatus.Creating &&
                                                          data.RemainingDataDependencies                      == new Dictionary<string, bool>(),
                                                  data => new
                                                          {
                                                            data.TaskId,
                                                            data.SessionId,
                                                            data.Options,
                                                            data.Options.PartitionId,
                                                            data.Options.Priority,
                                                          },
                                                  cancellationToken)
                                  .ConfigureAwait(false)).GroupBy(data => (data.PartitionId, data.Priority));

    foreach (var group in groups)
    {
      var ids = group.Select(data => data.TaskId)
                     .ToList();

      var msgsData = group.Select(data => new MessageData(data.TaskId,
                                                          data.SessionId,
                                                          data.Options));
      await pushQueueStorage_.PushMessagesAsync(msgsData,
                                                group.Key.PartitionId,
                                                cancellationToken)
                             .ConfigureAwait(false);

      await taskTable_.FinalizeTaskCreation(ids,
                                            cancellationToken)
                      .ConfigureAwait(false);
    }
  }

  /// <inheritdoc />
  public async Task<CreateTaskReply> CreateTask(IAsyncStreamReader<CreateTaskRequest> requestStream,
                                                CancellationToken                     cancellationToken)
  {
    var                               fsmCreate           = new ProcessReplyCreateLargeTaskStateMachine(logger_);
    Task?                             completionTask      = null;
    Channel<ReadOnlyMemory<byte>>?    payloadsChannel     = null;
    var                               taskRequestsChannel = Channel.CreateBounded<TaskRequest>(10);
    ICollection<TaskCreationRequest>? currentTasks        = null;

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
                                                                                  request.InitRequest.TaskOptions.ToNullableTaskOptions(),
                                                                                  taskRequestsChannel.Reader.ReadAllAsync(cancellationToken),
                                                                                  cancellationToken)
                                                                     .ConfigureAwait(false);
                                      createdTasks_.AddRange(currentTasks);
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
                                                  currentTasks!.Select(taskRequest => new CreateTaskReply.Types.CreationStatus
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
                                                  currentTasks!.Select(_ => new CreateTaskReply.Types.CreationStatus
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
      await foreach (var data in objectStorage_.GetValuesAsync(request.Key,
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

    var completionTask = Task.CompletedTask;
    var fsmResult      = new ProcessReplyResultStateMachine(logger_);
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
              completionTask = Task.Run(async () => await objectStorage_.AddOrUpdateAsync(request.Init.Key,
                                                                                          chunksChannel.Reader.ReadAllAsync(cancellationToken),
                                                                                          cancellationToken)
                                                                        .ConfigureAwait(false),
                                        cancellationToken);
              sentResults_.Add(request.Init.Key);
              break;
            case InitKeyedDataStream.TypeOneofCase.LastResult:
              fsmResult.CompleteRequest();

              try
              {
                await completionTask.WaitAsync(cancellationToken)
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
  public async Task<CreateResultsMetaDataResponse> CreateResultsMetaData(CreateResultsMetaDataRequest request,
                                                                         CancellationToken            cancellationToken)
  {
    var results = request.Results.Select(rc => new Storage.Result(request.SessionId,
                                                                  Guid.NewGuid()
                                                                      .ToString(),
                                                                  rc.Name,
                                                                  "",
                                                                  ResultStatus.Created,
                                                                  new List<string>(),
                                                                  DateTime.UtcNow,
                                                                  Array.Empty<byte>()))
                         .ToList();

    await resultTable_.Create(results,
                              cancellationToken)
                      .ConfigureAwait(false);

    return new CreateResultsMetaDataResponse
           {
             Results =
             {
               results.Select(result => new ResultMetaData
                                        {
                                          CreatedAt = FromDateTime(result.CreationDate),
                                          Name      = result.Name,
                                          SessionId = result.SessionId,
                                          Status    = result.Status,
                                          ResultId  = result.ResultId,
                                        }),
             },
           };
  }

  /// <inheritdoc />
  public async Task<SubmitTasksResponse> SubmitTasks(SubmitTasksRequest request,
                                                     CancellationToken  cancellationToken)
  {
    var options = TaskLifeCycleHelper.ValidateSession(sessionData_,
                                                      request.TaskOptions.ToNullableTaskOptions(),
                                                      taskData_.TaskId,
                                                      pushQueueStorage_.MaxPriority,
                                                      logger_,
                                                      cancellationToken);

    var createdTasks = request.TaskCreations.Select(creation => new TaskCreationRequest(Guid.NewGuid()
                                                                                            .ToString(),
                                                                                        creation.PayloadId,
                                                                                        TaskOptions.Merge(creation.TaskOptions.ToNullableTaskOptions(),
                                                                                                          options),
                                                                                        creation.ExpectedOutputKeys.ToList(),
                                                                                        creation.DataDependencies.ToList()))
                              .ToList();

    await TaskLifeCycleHelper.CreateTasks(taskTable_,
                                          resultTable_,
                                          request.SessionId,
                                          taskData_.TaskId,
                                          createdTasks,
                                          logger_,
                                          cancellationToken)
                             .ConfigureAwait(false);

    createdTasks_.AddRange(createdTasks);

    return new SubmitTasksResponse
           {
             CommunicationToken = token_,
             TaskInfos =
             {
               createdTasks.Select(creationRequest => new SubmitTasksResponse.Types.TaskInfo
                                                      {
                                                        DataDependencies =
                                                        {
                                                          creationRequest.DataDependencies,
                                                        },
                                                        ExpectedOutputIds =
                                                        {
                                                          creationRequest.ExpectedOutputKeys,
                                                        },
                                                        PayloadId = creationRequest.PayloadId,
                                                        TaskId    = creationRequest.TaskId,
                                                      }),
             },
           };
  }

  /// <inheritdoc />
  public async Task<UploadResultDataResponse> UploadResultData(IAsyncStreamReader<UploadResultDataRequest> requestStream,
                                                               CancellationToken                           cancellationToken)
  {
    if (!await requestStream.MoveNext(cancellationToken)
                            .ConfigureAwait(false))
    {
      throw new RpcException(new Status(StatusCode.FailedPrecondition,
                                        "Missing result metadata"),
                             "Missing result metadata");
    }

    var current = requestStream.Current;

    if (current.TypeCase != UploadResultDataRequest.TypeOneofCase.Id)
    {
      throw new RpcException(new Status(StatusCode.InvalidArgument,
                                        "Message should be an Id"),
                             "Message should be an Id");
    }

    var id = current.Id;


    await objectStorage_.AddOrUpdateAsync(id.ResultId,
                                          requestStream.ReadAllAsync(cancellationToken)
                                                       .Select(r => r.DataChunk.Memory),
                                          cancellationToken)
                        .ConfigureAwait(false);

    sentResults_.Add(id.ResultId);

    return new UploadResultDataResponse
           {
             ResultId           = id.ResultId,
             CommunicationToken = token_,
           };
  }

  /// <inheritdoc />
  public async Task<CreateResultsResponse> CreateResults(CreateResultsRequest request,
                                                         CancellationToken    cancellationToken)
  {
    var results = await request.Results.Select(async rc =>
                                               {
                                                 var result = new Storage.Result(request.SessionId,
                                                                                 Guid.NewGuid()
                                                                                     .ToString(),
                                                                                 rc.Name,
                                                                                 "",
                                                                                 ResultStatus.Created,
                                                                                 new List<string>(),
                                                                                 DateTime.UtcNow,
                                                                                 Array.Empty<byte>());

                                                 await objectStorage_.AddOrUpdateAsync(result.ResultId,
                                                                                       new List<ReadOnlyMemory<byte>>
                                                                                       {
                                                                                         rc.Data.Memory,
                                                                                       }.ToAsyncEnumerable(),
                                                                                       cancellationToken)
                                                                     .ConfigureAwait(false);

                                                 return result;
                                               })
                               .WhenAll()
                               .ConfigureAwait(false);

    await resultTable_.Create(results,
                              cancellationToken)
                      .ConfigureAwait(false);

    sentResults_.AddRange(results.Select(r => r.ResultId));

    return new CreateResultsResponse
           {
             CommunicationToken = token_,
             Results =
             {
               results.Select(r => new ResultMetaData
                                   {
                                     Status    = r.Status,
                                     CreatedAt = FromDateTime(r.CreationDate),
                                     Name      = r.Name,
                                     ResultId  = r.ResultId,
                                     SessionId = r.SessionId,
                                   }),
             },
           };
  }

  /// <inheritdoc />
  public void Dispose()
  {
  }
}
