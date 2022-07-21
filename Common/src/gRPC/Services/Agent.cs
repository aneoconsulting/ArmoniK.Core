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
// but WITHOUT ANY WARRANTY

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

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
/// Represents the internal processing requests received by the agent. Provides methods to process those requests
/// </summary>
public class Agent : IAgent
{
  private readonly ISubmitter                                                      submitter_;
  private readonly ILogger                                                         logger_;
  private readonly List<(IEnumerable<Storage.TaskRequest> requests, int priority)> createdTasks_;
  private readonly IObjectStorage                                                  resourcesStorage_;
  private          string?                                                         sessionId_;
  private          string?                                                         taskId_;
  private readonly string                                                          token_;

  /// <summary>
  /// Initializes a new instance of the <see cref="Agent"/>
  /// </summary>
  /// <param name="submitter">Interface to manage tasks</param>
  /// <param name="objectStorageFactory">Interface class to create object storage</param>
  /// <param name="sessionId">Id of the session</param>
  /// <param name="taskId">Id of the task</param>
  /// <param name="token">Token send to the worker to identify the running task</param>
  /// <param name="logger">Logger used to produce logs for this class</param>
  public Agent(ISubmitter            submitter,
               IObjectStorageFactory objectStorageFactory,
               string                sessionId,
               string                taskId,
               string                token,
               ILogger               logger)
  {
    submitter_        = submitter;
    logger_           = logger;
    resourcesStorage_ = objectStorageFactory.CreateResourcesStorage();
    createdTasks_     = new List<(IEnumerable<Storage.TaskRequest> requests, int priority)>();
    sessionId_        = sessionId;
    taskId_           = taskId;
    token_            = token;
  }

  public async Task FinalizeTaskCreation(CancellationToken cancellationToken)
  {
    using var _ = logger_.BeginNamedScope(nameof(FinalizeTaskCreation),
                                          ("taskId", taskId_)!,
                                          ("sessionId", sessionId_)!);
    if (createdTasks_ == null)
    {
      throw new ArmoniKException("Created tasks should not be null");
    }

    logger_.LogDebug("Finalize child task creation");

    foreach (var createdTask in createdTasks_)
    {
      await submitter_.FinalizeTaskCreation(createdTask.requests,
                                            createdTask.priority,
                                            sessionId_!,
                                            taskId_!,
                                            cancellationToken)
                      .ConfigureAwait(false);
    }
  }

  public async Task<CreateTaskReply> CreateTask(IAsyncStreamReader<CreateTaskRequest> requestStream,
                                                CancellationToken                     cancellationToken)
  {
    var                            fsmCreate           = new ProcessReplyCreateLargeTaskStateMachine(logger_);
    Task?                          completionTask      = null;
    Channel<ReadOnlyMemory<byte>>? payloadsChannel     = null;
    var                            taskRequestsChannel = Channel.CreateBounded<TaskRequest>(10);

    using var _ = logger_.BeginNamedScope(nameof(CreateTask),
                                          ("taskId", taskId_)!,
                                          ("sessionId", sessionId_)!);
    await foreach (var request in requestStream.ReadAllAsync(cancellationToken: cancellationToken)
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
                                      createdTasks_.Add(await submitter_.CreateTasks(sessionId_!,
                                                                                     taskId_!,
                                                                                     request.InitRequest.TaskOptions,
                                                                                     taskRequestsChannel.Reader.ReadAllAsync(cancellationToken),
                                                                                     cancellationToken)
                                                                        .ConfigureAwait(false));

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

              await taskRequestsChannel.Writer.WriteAsync(new TaskRequest(request.InitTask.Header.Id,
                                                                          request.InitTask.Header.ExpectedOutputKeys,
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
                         Successfull = new Empty(),
                       };
              }
              catch (Exception e)
              {
                logger_.LogWarning(e,
                                   "Error during task creation");
                return new CreateTaskReply
                       {
                         NonSuccessfullIds = new CreateTaskReply.Types.TaskIds(),
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

  public async Task GetCommonData(DataRequest                    request,
                                  IServerStreamWriter<DataReply> responseStream,
                                  CancellationToken              cancellationToken)
  {
    using var _ = logger_.BeginNamedScope(nameof(GetCommonData),
                                          ("taskId", taskId_)!,
                                          ("sessionId", sessionId_)!);
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

  public async Task GetDirectData(DataRequest                    request,
                                  IServerStreamWriter<DataReply> responseStream,
                                  CancellationToken              cancellationToken)
  {
    using var _ = logger_.BeginNamedScope(nameof(GetDirectData),
                                          ("taskId", taskId_)!,
                                          ("sessionId", sessionId_)!);

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

  public async Task GetResourceData(DataRequest                    request,
                                    IServerStreamWriter<DataReply> responseStream,
                                    CancellationToken              cancellationToken)
  {
    using var _ = logger_.BeginNamedScope(nameof(GetResourceData),
                                          ("taskId", taskId_)!,
                                          ("sessionId", sessionId_)!);

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

    IAsyncEnumerable<byte[]> bytes;
    try
    {
      bytes = resourcesStorage_.GetValuesAsync(request.Key,
                                               cancellationToken);
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
      return;
    }

    await responseStream.WriteAsync(new DataReply
                                    {
                                      Init = new DataReply.Types.Init
                                             {
                                               Key = request.Key,
                                               Data = new DataChunk
                                                      {
                                                        Data = UnsafeByteOperations.UnsafeWrap(await bytes.FirstAsync(cancellationToken: cancellationToken)
                                                                                                          .ConfigureAwait(false)),
                                                      },
                                             },
                                    },
                                    cancellationToken)
                        .ConfigureAwait(false);

    await foreach (var data in bytes.Skip(1)
                                    .ConfigureAwait(false))
    {
      await responseStream.WriteAsync(new DataReply
                                      {
                                        Init = new DataReply.Types.Init
                                               {
                                                 Key = request.Key,
                                                 Data = new DataChunk
                                                        {
                                                          Data = UnsafeByteOperations.UnsafeWrap(data),
                                                        },
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

  public async Task<ResultReply> SendResult(IAsyncStreamReader<Result> requestStream,
                                            CancellationToken          cancellationToken)
  {
    using var _ = logger_.BeginNamedScope(nameof(SendResult),
                                          ("taskId", taskId_)!,
                                          ("sessionId", sessionId_)!);

    Task? completionTask = null;
    var   fsmResult      = new ProcessReplyResultStateMachine(logger_);
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
                                          await submitter_.SetResult(sessionId_!,
                                                                     taskId_!,
                                                                     request.Init.Key,
                                                                     chunksChannel.Reader.ReadAllAsync(cancellationToken),
                                                                     cancellationToken)
                                                          .ConfigureAwait(false);
                                        },
                                        cancellationToken);
              break;
            case InitKeyedDataStream.TypeOneofCase.LastResult:
              fsmResult.CompleteRequest();

              try
              {
                await completionTask!.WaitAsync(cancellationToken)
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

  public void Dispose()
  {
  }
}
