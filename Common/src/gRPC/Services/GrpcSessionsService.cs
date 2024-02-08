// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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

using ArmoniK.Api.gRPC.V1.Sessions;
using ArmoniK.Api.gRPC.V1.SortDirection;
using ArmoniK.Core.Base;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Auth.Authorization;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC.Convertors;
using ArmoniK.Core.Common.Storage;

using Grpc.Core;

using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.gRPC.Services;

[Authorize(AuthenticationSchemes = Authenticator.SchemeName)]
public class GrpcSessionsService : Sessions.SessionsBase
{
  private readonly ILogger<GrpcSessionsService> logger_;
  private readonly IObjectStorage               objectStorage_;
  private readonly IPartitionTable              partitionTable_;
  private readonly IPushQueueStorage            pushQueueStorage_;
  private readonly IResultTable                 resultTable_;
  private readonly ISessionTable                sessionTable_;
  private readonly Injection.Options.Submitter  submitterOptions_;
  private readonly ITaskTable                   taskTable_;

  public GrpcSessionsService(ISessionTable                sessionTable,
                             IPartitionTable              partitionTable,
                             IObjectStorage               objectStorage,
                             IResultTable                 resultTable,
                             ITaskTable                   taskTable,
                             IPushQueueStorage            pushQueueStorage,
                             Injection.Options.Submitter  submitterOptions,
                             ILogger<GrpcSessionsService> logger)
  {
    logger_           = logger;
    sessionTable_     = sessionTable;
    partitionTable_   = partitionTable;
    objectStorage_    = objectStorage;
    resultTable_      = resultTable;
    taskTable_        = taskTable;
    pushQueueStorage_ = pushQueueStorage;
    submitterOptions_ = submitterOptions;
  }

  [RequiresPermission(typeof(GrpcSessionsService),
                      nameof(CancelSession))]
  public override async Task<CancelSessionResponse> CancelSession(CancelSessionRequest request,
                                                                  ServerCallContext    context)
  {
    try
    {
      return new CancelSessionResponse
             {
               Session = (await sessionTable_.CancelSessionAsync(request.SessionId,
                                                                 context.CancellationToken)
                                             .ConfigureAwait(false)).ToGrpcSessionRaw(),
             };
    }
    catch (SessionNotFoundException e)
    {
      logger_.LogWarning(e,
                         "Error while cancelling session");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Session not found"));
    }
    catch (InvalidSessionTransitionException e)
    {
      logger_.LogWarning(e,
                         "Error while cancelling session");
      throw new RpcException(new Status(StatusCode.FailedPrecondition,
                                        "Session is in a state that cannot be cancelled"));
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                         "Error while cancelling session");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see application logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                         "Error while cancelling session");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see application logs"));
    }
  }

  [RequiresPermission(typeof(GrpcSessionsService),
                      nameof(GetSession))]
  public override async Task<GetSessionResponse> GetSession(GetSessionRequest request,
                                                            ServerCallContext context)
  {
    try
    {
      return new GetSessionResponse
             {
               Session = (await sessionTable_.GetSessionAsync(request.SessionId,
                                                              context.CancellationToken)
                                             .ConfigureAwait(false)).ToGrpcSessionRaw(),
             };
    }
    catch (SessionNotFoundException e)
    {
      logger_.LogWarning(e,
                         "Error while getting session");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Session not found"));
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                         "Error while getting session");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see application logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                         "Error while getting session");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see application logs"));
    }
  }

  [RequiresPermission(typeof(GrpcSessionsService),
                      nameof(ListSessions))]
  public override async Task<ListSessionsResponse> ListSessions(ListSessionsRequest request,
                                                                ServerCallContext   context)
  {
    try
    {
      var (sessions, totalCount) = await sessionTable_.ListSessionsAsync(request.Filters is null
                                                                           ? data => true
                                                                           : request.Filters.ToSessionDataFilter(),
                                                                         request.Sort is null
                                                                           ? data => data.SessionId
                                                                           : request.Sort.ToField(),
                                                                         request.Sort is null || request.Sort.Direction == SortDirection.Asc,
                                                                         request.Page,
                                                                         request.PageSize,
                                                                         context.CancellationToken)
                                                      .ConfigureAwait(false);

      return new ListSessionsResponse
             {
               Page     = request.Page,
               PageSize = request.PageSize,
               Sessions =
               {
                 sessions.Select(data => data.ToGrpcSessionRaw()),
               },
               Total = (int)totalCount,
             };
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                         "Error while listing sessions");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see application logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                         "Error while listing sessions");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see application logs"));
    }
  }

  [RequiresPermission(typeof(GrpcSessionsService),
                      nameof(CreateSession))]
  public override async Task<CreateSessionReply> CreateSession(CreateSessionRequest request,
                                                               ServerCallContext    context)
  {
    try
    {
      return new CreateSessionReply
             {
               SessionId = await SessionLifeCycleHelper.CreateSession(sessionTable_,
                                                                      partitionTable_,
                                                                      request.PartitionIds,
                                                                      request.DefaultTaskOption.ToTaskOptions(),
                                                                      submitterOptions_.DefaultPartition,
                                                                      context.CancellationToken)
                                                       .ConfigureAwait(false),
             };
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
                                        "Internal ArmoniK Exception, see Submitter logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                         "Error while creating session");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see Submitter logs"));
    }
  }

  [RequiresPermission(typeof(GrpcSessionsService),
                      nameof(PauseSession))]
  public override async Task<PauseSessionResponse> PauseSession(PauseSessionRequest request,
                                                                ServerCallContext   context)
  {
    try
    {
      return new PauseSessionResponse
             {
               Session = (await sessionTable_.PauseSessionAsync(request.SessionId,
                                                                context.CancellationToken)
                                             .ConfigureAwait(false)).ToGrpcSessionRaw(),
             };
    }
    catch (SessionNotFoundException e)
    {
      logger_.LogWarning(e,
                         "Error while getting session");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Session not found"));
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                         "Error while getting session");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see application logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                         "Error while getting session");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see application logs"));
    }
  }

  [RequiresPermission(typeof(GrpcSessionsService),
                      nameof(PurgeSession))]
  public override async Task<PurgeSessionResponse> PurgeSession(PurgeSessionRequest request,
                                                                ServerCallContext   context)
  {
    try
    {
      var session = await sessionTable_.GetSessionAsync(request.SessionId,
                                                        context.CancellationToken)
                                       .ConfigureAwait(false);

      await ResultLifeCycleHelper.PurgeResultsAsync(resultTable_,
                                                    objectStorage_,
                                                    request.SessionId,
                                                    context.CancellationToken)
                                 .ConfigureAwait(false);

      logger_.LogInformation("Purged data for {sessionId}",
                             session);

      return new PurgeSessionResponse
             {
               Session = (await sessionTable_.PurgeSessionAsync(request.SessionId,
                                                                session.CreationDate,
                                                                context.CancellationToken)
                                             .ConfigureAwait(false)).ToGrpcSessionRaw(),
             };
    }
    catch (SessionNotFoundException e)
    {
      logger_.LogWarning(e,
                         "Error while purging session");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Session not found"));
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                         "Error while purging session");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see application logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                         "Error while getting session");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see application logs"));
    }
  }

  [RequiresPermission(typeof(GrpcSessionsService),
                      nameof(DeleteSession))]
  public override async Task<DeleteSessionResponse> DeleteSession(DeleteSessionRequest request,
                                                                  ServerCallContext    context)
  {
    try
    {
      var session = await sessionTable_.GetSessionAsync(request.SessionId,
                                                        context.CancellationToken)
                                       .ConfigureAwait(false);

      await sessionTable_.DeleteSessionAsync(request.SessionId,
                                             context.CancellationToken)
                         .ConfigureAwait(false);

      session = session with
                {
                  Status = SessionStatus.Deleted,
                  DeletionDate = DateTime.UtcNow,
                };

      return new DeleteSessionResponse
             {
               Session = session.ToGrpcSessionRaw(),
             };
    }
    catch (SessionNotFoundException e)
    {
      logger_.LogWarning(e,
                         "Error while getting session");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Session not found"));
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                         "Error while getting session");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see application logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                         "Error while getting session");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see application logs"));
    }
  }

  [RequiresPermission(typeof(GrpcSessionsService),
                      nameof(ResumeSession))]
  public override async Task<ResumeSessionResponse> ResumeSession(ResumeSessionRequest request,
                                                                  ServerCallContext    context)
  {
    try
    {
      var session = await TaskLifeCycleHelper.ResumeAsync(taskTable_,
                                                          sessionTable_,
                                                          pushQueueStorage_,
                                                          request.SessionId,
                                                          context.CancellationToken)
                                             .ConfigureAwait(false);

      return new ResumeSessionResponse
             {
               Session = session.ToGrpcSessionRaw(),
             };
    }
    catch (SessionNotFoundException e)
    {
      logger_.LogWarning(e,
                         "Error while getting session");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Session not found"));
    }
    catch (InvalidSessionTransitionException e)
    {
      logger_.LogWarning(e,
                         "Error while cancelling session");
      throw new RpcException(new Status(StatusCode.FailedPrecondition,
                                        "Session is in a state that cannot be cancelled"));
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                         "Error while getting session");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see application logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                         "Error while getting session");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see application logs"));
    }
  }

  [RequiresPermission(typeof(GrpcSessionsService),
                      nameof(StopSubmission))]
  public override async Task<StopSubmissionResponse> StopSubmission(StopSubmissionRequest request,
                                                                    ServerCallContext     context)
  {
    try
    {
      return new StopSubmissionResponse
             {
               Session = (await sessionTable_.StopSubmissionAsync(request.SessionId,
                                                                  request.Client,
                                                                  request.Worker,
                                                                  context.CancellationToken)
                                             .ConfigureAwait(false)).ToGrpcSessionRaw(),
             };
    }
    catch (SessionNotFoundException e)
    {
      logger_.LogWarning(e,
                         "Error while getting session");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Session not found"));
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                         "Error while getting session");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see application logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                         "Error while getting session");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see application logs"));
    }
  }
}
