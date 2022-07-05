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

public class Agent : IAgent
{
  private readonly ISubmitter                                                       submitter_;
  private readonly ILogger<GrpcAgentService>                                        logger_;
  private          List<(IEnumerable<Storage.TaskRequest> requests, int priority)>? createdTasks_;
  private readonly IObjectStorage                                                   resourcesStorage_;
  private          TaskData?                                                        taskData_;
  private          string?                                                          communicationToken_;

  public Agent(ISubmitter                submitter,
               IObjectStorageFactory     objectStorageFactory,
               ILogger<GrpcAgentService> logger)
  {
    submitter_          = submitter;
    logger_             = logger;
    resourcesStorage_   = objectStorageFactory.CreateResourcesStorage();
    taskData_           = null;
    communicationToken_ = null;
    createdTasks_       = null;
  }

  public Task<string> Activate(TaskData taskData)
  {
    communicationToken_ = Guid.NewGuid().ToString();
    taskData_           = taskData;
    createdTasks_       = new List<(IEnumerable<Storage.TaskRequest> requests, int priority)>();
    return Task.FromResult(communicationToken_);
  }

  public async Task FinalizeTaskCreation(CancellationToken cancellationToken)
  {
    if (createdTasks_ == null)
    {
      throw new ArmoniKException("Created tasks should not be null");
    }

    foreach (var createdTask in createdTasks_)
    {
      await submitter_.FinalizeTaskCreation(createdTask.requests,
                                            createdTask.priority,
                                            taskData_!.SessionId,
                                            taskData_.TaskId,
                                            cancellationToken)
                      .ConfigureAwait(false);
    }
  }


  public Task Deactivate()
  {
    taskData_           = null;
    communicationToken_ = null;
    return Task.CompletedTask;
  }

  public async Task<CreateTaskReply> CreateTask(IAsyncStreamReader<CreateTaskRequest> requestStream,
                                                CancellationToken                     cancellationToken)
  {
    var                            fsmCreate           = new ProcessReplyCreateLargeTaskStateMachine(logger_);
    Task?                          completionTask      = null;
    Channel<ReadOnlyMemory<byte>>? payloadsChannel     = null;
    var                            taskRequestsChannel = Channel.CreateBounded<TaskRequest>(10);

    await foreach (var request in requestStream.ReadAllAsync(cancellationToken: cancellationToken)
                                               .ConfigureAwait(false))
    {
      if (communicationToken_ == null)
      {
        return new CreateTaskReply
               {
                 CommunicationToken = request.CommunicationToken,
                 Error              = "Server not available yet",
               };
      }

      if (request.CommunicationToken != communicationToken_)
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
                                      createdTasks_!.Add(await submitter_.CreateTasks(taskData_!.SessionId,
                                                                                    taskData_.TaskId,
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
              logger_.LogDebug("Send Task creation reply for {RequestId}",
                               request.CommunicationToken);


              try
              {
                await completionTask!.WaitAsync(cancellationToken)
                                     .ConfigureAwait(false);

                // todo : finalize creation
                //await submitter_.FinalizeTaskCreation(taskIds_!,
                //                                      priority_!,
                //                                      sessionId_,
                //                                      parentTaskId_,
                //                                      context.CancellationToken)
                //                .ConfigureAwait(false);

                return new CreateTaskReply
                       {
                         CommunicationToken = request.CommunicationToken,
                         Successfull        = new Empty(),
                       };
              }
              catch (Exception e)
              {
                logger_.LogWarning(e,
                                   "Error during task creation");
                return new CreateTaskReply
                       {
                         CommunicationToken = request.CommunicationToken,
                         NonSuccessfullIds  = new CreateTaskReply.Types.TaskIds(),
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

    return new CreateTaskReply
           {
             CommunicationToken = "",
           };
  }

  public async Task GetCommonData(DataRequest                    request,
                                  IServerStreamWriter<DataReply> responseStream,
                                  CancellationToken              cancellationToken)
  {
    if (communicationToken_ == null)
    {
      await responseStream.WriteAsync(new DataReply
                                      {
                                        CommunicationToken = request.CommunicationToken,
                                        Error              = "Server not yet available",
                                      },
                                      cancellationToken)
                          .ConfigureAwait(false);
    }

    if (request.CommunicationToken != communicationToken_)
    {
      await responseStream.WriteAsync(new DataReply
                                      {
                                        CommunicationToken = request.CommunicationToken,
                                        Error              = "Wrong Communication Token",
                                      },
                                      cancellationToken)
                          .ConfigureAwait(false);
    }

    await responseStream.WriteAsync(new DataReply
                                    {
                                      CommunicationToken = request.CommunicationToken,
                                      Error              = "Common data are not supported yet",
                                    },
                                    cancellationToken)
                        .ConfigureAwait(false);
  }

  public async Task GetDirectData(DataRequest                             request,
                                           IServerStreamWriter<DataReply> responseStream,
                                           CancellationToken              cancellationToken)
  {
    if (communicationToken_ == null)
    {
      await responseStream.WriteAsync(new DataReply
                                      {
                                        CommunicationToken = request.CommunicationToken,
                                        Error              = "Server not yet available",
                                      },
                                      cancellationToken)
                          .ConfigureAwait(false);
    }

    if (request.CommunicationToken != communicationToken_)
    {
      await responseStream.WriteAsync(new DataReply
                                      {
                                        CommunicationToken = request.CommunicationToken,
                                        Error              = "Wrong Communication Token",
                                      },
                                      cancellationToken)
                          .ConfigureAwait(false);
    }

    await responseStream.WriteAsync(new DataReply
                                    {
                                      CommunicationToken = request.CommunicationToken,
                                      Error              = "Direct data are not supported yet",
                                    },
                                    cancellationToken)
                        .ConfigureAwait(false);
  }

  public async Task GetResourceData(DataRequest                    request,
                                    IServerStreamWriter<DataReply> responseStream,
                                    CancellationToken              cancellationToken)
  {
    if (communicationToken_ == null)
    {
      await responseStream.WriteAsync(new DataReply
                                      {
                                        CommunicationToken = request.CommunicationToken,
                                        Error              = "Server not yet available",
                                      },
                                      cancellationToken)
                          .ConfigureAwait(false);
    }

    if (request.CommunicationToken != communicationToken_)
    {
      await responseStream.WriteAsync(new DataReply
                                      {
                                        CommunicationToken = request.CommunicationToken,
                                        Error              = "Wrong Communication Token",
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
                                        CommunicationToken = communicationToken_,
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
                                      CommunicationToken = communicationToken_,
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
                                        CommunicationToken = communicationToken_,
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
                                      CommunicationToken = communicationToken_,
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
      if (communicationToken_ == null)
      {
        return new ResultReply
               {
                 CommunicationToken = request.CommunicationToken,
                 Error              = "Server not yet available",
               };
      }

      if (request.CommunicationToken != communicationToken_)
      {
        return new ResultReply
               {
                 CommunicationToken = request.CommunicationToken,
                 Error              = "Wrong Communication Token",
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
                                          await submitter_.SetResult(taskData_!.SessionId,
                                                                     taskData_.TaskId,
                                                                     request.Init.Key,
                                                                     chunksChannel.Reader.ReadAllAsync(cancellationToken),
                                                                     cancellationToken)
                                                          .ConfigureAwait(false);
                                        },
                                        cancellationToken);
              break;
            case InitKeyedDataStream.TypeOneofCase.LastResult:
              fsmResult.CompleteRequest();
              break;
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

              try
              {
                await completionTask!.WaitAsync(cancellationToken)
                                     .ConfigureAwait(false);
                return new ResultReply
                       {
                         CommunicationToken = request.CommunicationToken,
                         Ok                 = new Empty(),
                       };
              }
              catch (Exception e)
              {
                logger_.LogWarning(e,
                                   "Error while receiving results");
                return new ResultReply
                       {
                         CommunicationToken = request.CommunicationToken,
                         Error              = "Error while receiving results",
                       };
              }
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

    return new ResultReply
           {
             CommunicationToken = "",
           };
  }
}
