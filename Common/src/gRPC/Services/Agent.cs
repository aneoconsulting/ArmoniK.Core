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
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Agent;
using ArmoniK.Core.Base;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC.Convertors;
using ArmoniK.Core.Common.StateMachines;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Utils;

using Grpc.Core;

using Microsoft.Extensions.Logging;

using static Google.Protobuf.WellKnownTypes.Timestamp;

using ResultStatus = ArmoniK.Core.Common.Storage.ResultStatus;
using TaskOptions = ArmoniK.Core.Base.DataStructures.TaskOptions;

namespace ArmoniK.Core.Common.gRPC.Services;

/// <summary>
///   Represents the internal processing requests received by the agent. Provides methods to process those requests
/// </summary>
public sealed class Agent : IAgent
{
  private readonly List<TaskCreationRequest> createdTasks_;
  private readonly string                    folder_;
  private readonly ILogger                   logger_;
  private readonly IObjectStorage            objectStorage_;
  private readonly IPushQueueStorage         pushQueueStorage_;
  private readonly IResultTable              resultTable_;
  private readonly Dictionary<string, long>  sentResults_;
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
  /// <param name="folder">Shared folder between Agent and Worker</param>
  /// <param name="token">Token send to the worker to identify the running task</param>
  /// <param name="logger">Logger used to produce logs for this class</param>
  public Agent(ISubmitter        submitter,
               IObjectStorage    objectStorage,
               IPushQueueStorage pushQueueStorage,
               IResultTable      resultTable,
               ITaskTable        taskTable,
               SessionData       sessionData,
               TaskData          taskData,
               string            folder,
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
    sentResults_      = new Dictionary<string, long>();
    sessionData_      = sessionData;
    taskData_         = taskData;
    folder_           = folder;
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

    foreach (var (result, size) in sentResults_)
    {
      await resultTable_.CompleteResult(taskData_.SessionId,
                                        result,
                                        size,
                                        cancellationToken)
                        .ConfigureAwait(false);
    }

    await TaskLifeCycleHelper.ResolveDependencies(taskTable_,
                                                  resultTable_,
                                                  pushQueueStorage_,
                                                  sessionData_.SessionId,
                                                  sentResults_.Keys,
                                                  logger_,
                                                  cancellationToken)
                             .ConfigureAwait(false);
  }

  /// <inheritdoc />
  [SuppressMessage("Usage",
                   "CA2208:Instantiate argument exceptions correctly",
                   Justification = "No correct value for ArgumentOutOfRange Exception in nested code")]
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
                                                                              Error = "An error occurred during task creation",
                                                                            }),
                                                },
                                              },
                       };
              }

            case InitTaskRequest.TypeOneofCase.None:
            default:
              throw new ArgumentOutOfRangeException(nameof(InitTaskRequest.TypeOneofCase.LastTask));
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
              throw new ArgumentOutOfRangeException(nameof(request.TaskPayload.TypeCase));
          }

          break;
        case CreateTaskRequest.TypeOneofCase.None:
        default:
          throw new ArgumentOutOfRangeException(nameof(CreateTaskRequest.TypeOneofCase.InitTask));
      }
    }

    return new CreateTaskReply();
  }

  /// <inheritdoc />
  public async Task<CreateResultsMetaDataResponse> CreateResultsMetaData(CreateResultsMetaDataRequest request,
                                                                         CancellationToken            cancellationToken)
  {
    var results = request.Results.Select(rc => new Result(request.SessionId,
                                                          Guid.NewGuid()
                                                              .ToString(),
                                                          rc.Name,
                                                          "",
                                                          ResultStatus.Created,
                                                          new List<string>(),
                                                          DateTime.UtcNow,
                                                          0,
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
                                          Status    = result.Status.ToGrpcStatus(),
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
  public async Task<CreateResultsResponse> CreateResults(CreateResultsRequest request,
                                                         CancellationToken    cancellationToken)
  {
    var results = await request.Results.Select(async rc =>
                                               {
                                                 var resultId = Guid.NewGuid()
                                                                    .ToString();

                                                 var size = await objectStorage_.AddOrUpdateAsync(resultId,
                                                                                                  new List<ReadOnlyMemory<byte>>
                                                                                                  {
                                                                                                    rc.Data.Memory,
                                                                                                  }.ToAsyncEnumerable(),
                                                                                                  cancellationToken)
                                                                                .ConfigureAwait(false);

                                                 return new Result(request.SessionId,
                                                                   resultId,
                                                                   rc.Name,
                                                                   "",
                                                                   ResultStatus.Created,
                                                                   new List<string>(),
                                                                   DateTime.UtcNow,
                                                                   size,
                                                                   Array.Empty<byte>());
                                               })
                               .WhenAll()
                               .ConfigureAwait(false);

    await resultTable_.Create(results,
                              cancellationToken)
                      .ConfigureAwait(false);

    foreach (var result in results)
    {
      sentResults_.Add(result.ResultId,
                       result.Size);
    }

    return new CreateResultsResponse
           {
             CommunicationToken = token_,
             Results =
             {
               results.Select(r => new ResultMetaData
                                   {
                                     Status    = r.Status.ToGrpcStatus(),
                                     CreatedAt = FromDateTime(r.CreationDate),
                                     Name      = r.Name,
                                     ResultId  = r.ResultId,
                                     SessionId = r.SessionId,
                                   }),
             },
           };
  }

  public async Task<NotifyResultDataResponse> NotifyResultData(NotifyResultDataRequest request,
                                                               CancellationToken       cancellationToken)
  {
    if (string.IsNullOrEmpty(request.CommunicationToken))
    {
      throw new RpcException(new Status(StatusCode.InvalidArgument,
                                        "Missing communication token"),
                             "Missing communication token");
    }

    if (request.CommunicationToken != token_)
    {
      throw new RpcException(new Status(StatusCode.InvalidArgument,
                                        "Wrong communication token"),
                             "Wrong communication token");
    }

    foreach (var result in request.Ids)
    {
      await using var fs = new FileStream(Path.Combine(folder_,
                                                       result.ResultId),
                                          FileMode.OpenOrCreate);
      using var r       = new BinaryReader(fs);
      var       channel = Channel.CreateUnbounded<ReadOnlyMemory<byte>>();

      var add = objectStorage_.AddOrUpdateAsync(result.ResultId,
                                                channel.Reader.ReadAllAsync(cancellationToken),
                                                cancellationToken);

      long size = 0;
      int  read;
      do
      {
        var buffer = new byte[PayloadConfiguration.MaxChunkSize];
        read = r.Read(buffer,
                      0,
                      PayloadConfiguration.MaxChunkSize);
        size += read;
        if (read > 0)
        {
          await channel.Writer.WriteAsync(buffer.AsMemory(0,
                                                          read),
                                          cancellationToken)
                       .ConfigureAwait(false);
        }
      } while (read != 0);

      channel.Writer.Complete();

      await add.ConfigureAwait(false);
      sentResults_.Add(result.ResultId,
                       size);
    }

    return new NotifyResultDataResponse
           {
             ResultIds =
             {
               request.Ids.Select(identifier => identifier.ResultId),
             },
           };
  }

  /// <inheritdoc />
  public void Dispose()
  {
  }

  /// <inheritdoc />
  public async Task<DataResponse> GetResourceData(DataRequest       request,
                                                  CancellationToken cancellationToken)
  {
    using var _ = logger_.BeginNamedScope(nameof(GetResourceData),
                                          ("taskId", taskData_.TaskId),
                                          ("sessionId", sessionData_.SessionId));

    if (string.IsNullOrEmpty(request.CommunicationToken))
    {
      throw new RpcException(new Status(StatusCode.InvalidArgument,
                                        "Missing communication token"),
                             "Missing communication token");
    }

    if (request.CommunicationToken != token_)
    {
      throw new RpcException(new Status(StatusCode.InvalidArgument,
                                        "Wrong communication token"),
                             "Wrong communication token");
    }

    try
    {
      await using (var fs = new FileStream(Path.Combine(folder_,
                                                        request.ResultId),
                                           FileMode.OpenOrCreate))
      {
        await using var w = new BinaryWriter(fs);
        await foreach (var chunk in objectStorage_.GetValuesAsync(request.ResultId,
                                                                  cancellationToken)
                                                  .ConfigureAwait(false))
        {
          w.Write(chunk);
        }
      }


      return new DataResponse
             {
               ResultId = request.ResultId,
             };
    }
    catch (ObjectDataNotFoundException)
    {
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Data not found"),
                             "Data not found");
    }
  }

  /// <inheritdoc />
  public Task<DataResponse> GetCommonData(DataRequest       request,
                                          CancellationToken cancellationToken)
  {
    using var _ = logger_.BeginNamedScope(nameof(GetCommonData),
                                          ("taskId", taskData_.TaskId),
                                          ("sessionId", sessionData_.SessionId));

    if (string.IsNullOrEmpty(request.CommunicationToken))
    {
      throw new RpcException(new Status(StatusCode.InvalidArgument,
                                        "Missing communication token"),
                             "Missing communication token");
    }

    if (request.CommunicationToken != token_)
    {
      throw new RpcException(new Status(StatusCode.InvalidArgument,
                                        "Wrong communication token"),
                             "Wrong communication token");
    }

    throw new NotImplementedException("Common data are not implemented yet");
  }

  /// <inheritdoc />
  public Task<DataResponse> GetDirectData(DataRequest       request,
                                          CancellationToken cancellationToken)
  {
    using var _ = logger_.BeginNamedScope(nameof(GetDirectData),
                                          ("taskId", taskData_.TaskId),
                                          ("sessionId", sessionData_.SessionId));

    if (string.IsNullOrEmpty(request.CommunicationToken))
    {
      throw new RpcException(new Status(StatusCode.InvalidArgument,
                                        "Missing communication token"),
                             "Missing communication token");
    }

    if (request.CommunicationToken != token_)
    {
      throw new RpcException(new Status(StatusCode.InvalidArgument,
                                        "Wrong communication token"),
                             "Wrong communication token");
    }

    throw new NotImplementedException("Direct data are not implemented yet");
  }
}
