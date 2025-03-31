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
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Agent;
using ArmoniK.Core.Common.Auth.Authorization;
using ArmoniK.Core.Common.gRPC.Convertors;
using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.StateMachines;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Utils;

using Grpc.Core;

using Microsoft.Extensions.Logging.Abstractions;

using static Google.Protobuf.WellKnownTypes.Timestamp;

using Agent = ArmoniK.Api.gRPC.V1.Agent.Agent;

namespace ArmoniK.Core.Common.gRPC.Services;

/// <inheritdoc cref="Api.gRPC.V1.Agent.Agent" />
[IgnoreAuthentication]
public class GrpcAgentService : Agent.AgentBase
{
  private IAgent? agent_;

  /// <summary>
  ///   Adds the given <see cref="IAgent" /> thus making the worker able to interact with it.
  /// </summary>
  /// <param name="agent">The agent to start the service with.</param>
  /// <returns>A completed task.</returns>
  public Task Start(IAgent agent)
  {
    agent_ = agent;
    return Task.CompletedTask;
  }

  /// <summary>
  ///   Removes the underlying <see cref="IAgent" /> thus making the worker unable to interact with the agent.
  /// </summary>
  /// <returns>A completed task.</returns>
  public Task Stop()
  {
    agent_ = null;
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public override async Task<CreateTaskReply> CreateTask(IAsyncStreamReader<CreateTaskRequest> requestStream,
                                                         ServerCallContext                     context)
  {
    if (agent_ is null)
    {
      return new CreateTaskReply
             {
               Error = "No task is accepting request",
             };
    }

    var fsmCreate      = new ProcessReplyCreateLargeTaskStateMachine(NullLogger.Instance);
    var createdTasks   = new List<SubmitTasksRequest.Types.TaskCreation>();
    var createdResults = new List<string>();

    if (!await requestStream.MoveNext(context.CancellationToken)
                            .ConfigureAwait(false))
    {
      return new CreateTaskReply
             {
               Error = "Empty request",
             };
    }

    fsmCreate.InitRequest();
    var current = requestStream.Current;

    if (string.IsNullOrEmpty(current.CommunicationToken))
    {
      return new CreateTaskReply
             {
               CommunicationToken = current.CommunicationToken,
               Error              = "Missing communication token",
             };
    }

    if (current.CommunicationToken != agent_.Token)
    {
      return new CreateTaskReply
             {
               CommunicationToken = current.CommunicationToken,
               Error              = "Wrong communication token",
             };
    }

    var taskOptions = current.InitRequest.TaskOptions;
    if (!await requestStream.MoveNext(context.CancellationToken)
                            .ConfigureAwait(false))
    {
      return new CreateTaskReply
             {
               CommunicationToken = current.CommunicationToken,
               Error              = "Missing message in request",
             };
    }

    while (requestStream.Current.TypeCase == CreateTaskRequest.TypeOneofCase.InitTask && requestStream.Current.InitTask.TypeCase == InitTaskRequest.TypeOneofCase.Header)
    {
      fsmCreate.AddHeader();
      var header = requestStream.Current.InitTask.Header;

      if (!await requestStream.MoveNext(context.CancellationToken)
                              .ConfigureAwait(false))
      {
        return new CreateTaskReply
               {
                 CommunicationToken = current.CommunicationToken,
                 Error              = "Missing message in request",
               };
      }

      var result = (await agent_.CreateResultsMetaData(agent_.Token,
                                                       new ResultCreationRequest[]
                                                       {
                                                         new(agent_.SessionId,
                                                             ""),
                                                       },
                                                       context.CancellationToken)
                                .ConfigureAwait(false)).Single();

      await using var fs = new FileStream(Path.Combine(agent_.Folder,
                                                       result.ResultId),
                                          FileMode.OpenOrCreate);
      await using var r = new BinaryWriter(fs);

      while (requestStream.Current.TypeCase == CreateTaskRequest.TypeOneofCase.TaskPayload && requestStream.Current.TaskPayload.TypeCase == DataChunk.TypeOneofCase.Data)
      {
        fsmCreate.AddDataChunk();
        var data = requestStream.Current.TaskPayload.Data;
        r.Write(data.Span);
        if (!await requestStream.MoveNext(context.CancellationToken)
                                .ConfigureAwait(false))
        {
          return new CreateTaskReply
                 {
                   CommunicationToken = current.CommunicationToken,
                   Error              = "Missing message in request",
                 };
        }
      }

      if (requestStream.Current.TypeCase             == CreateTaskRequest.TypeOneofCase.TaskPayload &&
          requestStream.Current.TaskPayload.TypeCase != DataChunk.TypeOneofCase.DataComplete)
      {
        throw new InvalidOperationException("Invalid request");
      }

      fsmCreate.CompleteData();

      createdResults.Add(result.ResultId);
      createdTasks.Add(new SubmitTasksRequest.Types.TaskCreation
                       {
                         DataDependencies =
                         {
                           header.DataDependencies,
                         },
                         ExpectedOutputKeys =
                         {
                           header.ExpectedOutputKeys,
                         },
                         PayloadId = result.ResultId,
                       });

      if (!await requestStream.MoveNext(context.CancellationToken)
                              .ConfigureAwait(false))
      {
        return new CreateTaskReply
               {
                 CommunicationToken = current.CommunicationToken,
                 Error              = "Missing message in request",
               };
      }
    }

    if (requestStream.Current.TypeCase != CreateTaskRequest.TypeOneofCase.InitTask || !requestStream.Current.InitTask.LastTask)
    {
      throw new InvalidOperationException("Invalid request");
    }

    fsmCreate.CompleteRequest();
    if (!fsmCreate.IsComplete())
    {
      throw new InvalidOperationException("Invalid request");
    }

    await agent_.NotifyResultData(agent_.Token,
                                  createdResults,
                                  context.CancellationToken)
                .ConfigureAwait(false);

    var submittedTasks = await agent_.SubmitTasks(createdTasks.ViewSelect(creation => new TaskSubmissionRequest(creation.PayloadId,
                                                                                                                creation.TaskOptions.ToNullableTaskOptions(),
                                                                                                                creation.ExpectedOutputKeys,
                                                                                                                creation.DataDependencies)),
                                                  taskOptions.ToNullableTaskOptions(),
                                                  agent_.SessionId,
                                                  agent_.Token,
                                                  context.CancellationToken)
                                     .ConfigureAwait(false);

    return new CreateTaskReply
           {
             CreationStatusList = new CreateTaskReply.Types.CreationStatusList
                                  {
                                    CreationStatuses =
                                    {
                                      submittedTasks.Select(creationRequest => new CreateTaskReply.Types.CreationStatus
                                                                               {
                                                                                 TaskInfo = new CreateTaskReply.Types.TaskInfo
                                                                                            {
                                                                                              DataDependencies =
                                                                                              {
                                                                                                creationRequest.DataDependencies,
                                                                                              },
                                                                                              PayloadId = creationRequest.PayloadId,
                                                                                              ExpectedOutputKeys =
                                                                                              {
                                                                                                creationRequest.ExpectedOutputKeys,
                                                                                              },
                                                                                              TaskId = creationRequest.TaskId,
                                                                                            },
                                                                               }),
                                    },
                                  },
           };
  }

  /// <inheritdoc />
  public override async Task<DataResponse> GetCommonData(DataRequest       request,
                                                         ServerCallContext context)
  {
    if (agent_ is not null)
    {
      return new DataResponse
             {
               ResultId = await agent_.GetCommonData(request.CommunicationToken,
                                                     request.ResultId,
                                                     context.CancellationToken)
                                      .ConfigureAwait(false),
             };
    }

    throw new RpcException(new Status(StatusCode.Unavailable,
                                      "No task is accepting request"),
                           "No task is accepting request");
  }

  /// <inheritdoc />
  public override async Task<DataResponse> GetResourceData(DataRequest       request,
                                                           ServerCallContext context)
  {
    if (agent_ is not null)
    {
      return new DataResponse
             {
               ResultId = await agent_.GetResourceData(request.CommunicationToken,
                                                       request.ResultId,
                                                       context.CancellationToken)
                                      .ConfigureAwait(false),
             };
    }

    throw new RpcException(new Status(StatusCode.Unavailable,
                                      "No task is accepting request"),
                           "No task is accepting request");
  }

  /// <inheritdoc />
  public override async Task<DataResponse> GetDirectData(DataRequest       request,
                                                         ServerCallContext context)
  {
    if (agent_ is not null)
    {
      return new DataResponse
             {
               ResultId = await agent_.GetDirectData(request.CommunicationToken,
                                                     request.ResultId,
                                                     context.CancellationToken)
                                      .ConfigureAwait(false),
             };
    }

    throw new RpcException(new Status(StatusCode.Unavailable,
                                      "No task is accepting request"),
                           "No task is accepting request");
  }

  /// <inheritdoc />
  public override async Task<NotifyResultDataResponse> NotifyResultData(NotifyResultDataRequest request,
                                                                        ServerCallContext       context)
  {
    if (agent_ is not null)
    {
      var results = await agent_.NotifyResultData(request.CommunicationToken,
                                                  request.Ids.ViewSelect(identifier => identifier.ResultId),
                                                  context.CancellationToken)
                                .ConfigureAwait(false);

      return new NotifyResultDataResponse
             {
               ResultIds =
               {
                 results,
               },
             };
    }

    throw new RpcException(new Status(StatusCode.Unavailable,
                                      "No task is accepting request"),
                           "No task is accepting request");
  }

  /// <inheritdoc />
  public override async Task<CreateResultsMetaDataResponse> CreateResultsMetaData(CreateResultsMetaDataRequest request,
                                                                                  ServerCallContext            context)
  {
    if (agent_ is not null)
    {
      var results = await agent_.CreateResultsMetaData(request.CommunicationToken,
                                                       request.Results.ViewSelect(create => new ResultCreationRequest(request.SessionId,
                                                                                                                      create.Name)),
                                                       context.CancellationToken)
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

    throw new RpcException(new Status(StatusCode.Unavailable,
                                      "No task is accepting request"),
                           "No task is accepting request");
  }

  /// <inheritdoc />
  public override async Task<SubmitTasksResponse> SubmitTasks(SubmitTasksRequest request,
                                                              ServerCallContext  context)
  {
    if (agent_ is not null)
    {
      var createdTasks = await agent_.SubmitTasks(request.TaskCreations.ViewSelect(creation => new TaskSubmissionRequest(creation.PayloadId,
                                                                                                                         creation.TaskOptions.ToNullableTaskOptions(),
                                                                                                                         creation.ExpectedOutputKeys.AsICollection(),
                                                                                                                         creation.DataDependencies.AsICollection())),
                                                  request.TaskOptions.ToNullableTaskOptions(),
                                                  request.SessionId,
                                                  request.CommunicationToken,
                                                  context.CancellationToken)
                                     .ConfigureAwait(false);

      return new SubmitTasksResponse
             {
               CommunicationToken = agent_.Token,
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

    throw new RpcException(new Status(StatusCode.Unavailable,
                                      "No task is accepting request"),
                           "No task is accepting request");
  }

  /// <inheritdoc />
  public override async Task<CreateResultsResponse> CreateResults(CreateResultsRequest request,
                                                                  ServerCallContext    context)
  {
    if (agent_ is not null)
    {
      var results = await agent_.CreateResults(request.CommunicationToken,
                                               request.Results.ViewSelect(create => (new ResultCreationRequest(request.SessionId,
                                                                                                               create.Name), create.Data.Memory)),
                                               context.CancellationToken)
                                .ConfigureAwait(false);

      return new CreateResultsResponse
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

    throw new RpcException(new Status(StatusCode.Unavailable,
                                      "No task is accepting request"),
                           "No task is accepting request");
  }
}
