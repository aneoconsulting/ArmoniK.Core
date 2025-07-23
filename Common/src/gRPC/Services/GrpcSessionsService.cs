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
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Sessions;
using ArmoniK.Api.gRPC.V1.SortDirection;
using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Base.Exceptions;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Auth.Authorization;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC.Convertors;
using ArmoniK.Core.Common.Meter;
using ArmoniK.Core.Common.Storage;

using Grpc.Core;

using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.gRPC.Services;

/// <inheritdoc cref="Sessions" />
[Authorize(AuthenticationSchemes = Authenticator.SchemeName)]
public class GrpcSessionsService : Sessions.SessionsBase
{
  private readonly ILogger<GrpcSessionsService>                  logger_;
  private readonly FunctionExecutionMetrics<GrpcSessionsService> meter_;
  private readonly IObjectStorage                                objectStorage_;
  private readonly IPartitionTable                               partitionTable_;
  private readonly IPushQueueStorage                             pushQueueStorage_;
  private readonly IResultTable                                  resultTable_;
  private readonly ISessionTable                                 sessionTable_;
  private readonly Injection.Options.Submitter                   submitterOptions_;
  private readonly ITaskTable                                    taskTable_;

  /// <summary>
  ///   Initializes a new instance of the <see cref="GrpcSessionsService" /> class.
  /// </summary>
  /// <param name="sessionTable">The session table for managing session data.</param>
  /// <param name="partitionTable">The partition table for managing partitions.</param>
  /// <param name="objectStorage">The object storage for storing session-related data.</param>
  /// <param name="resultTable">The result table for managing session results.</param>
  /// <param name="taskTable">The task table for managing tasks associated with sessions.</param>
  /// <param name="pushQueueStorage">The interface to push tasks in the queue.</param>
  /// <param name="submitterOptions">The submitter options for session configuration.</param>
  /// <param name="meter">The metrics for function execution monitoring.</param>
  /// <param name="logger">The logger for logging information and errors.</param>
  public GrpcSessionsService(ISessionTable                                 sessionTable,
                             IPartitionTable                               partitionTable,
                             IObjectStorage                                objectStorage,
                             IResultTable                                  resultTable,
                             ITaskTable                                    taskTable,
                             IPushQueueStorage                             pushQueueStorage,
                             Injection.Options.Submitter                   submitterOptions,
                             FunctionExecutionMetrics<GrpcSessionsService> meter,
                             ILogger<GrpcSessionsService>                  logger)
  {
    logger_           = logger;
    sessionTable_     = sessionTable;
    partitionTable_   = partitionTable;
    objectStorage_    = objectStorage;
    resultTable_      = resultTable;
    taskTable_        = taskTable;
    pushQueueStorage_ = pushQueueStorage;
    submitterOptions_ = submitterOptions;
    meter_            = meter;
  }

  /// <inheritdoc />
  [RequiresPermission(typeof(GrpcSessionsService),
                      nameof(CancelSession))]
  public override async Task<CancelSessionResponse> CancelSession(CancelSessionRequest request,
                                                                  ServerCallContext    context)
  {
    using var measure = meter_.CountAndTime();
    try
    {
      var tasks = taskTable_.CancelSessionAsync(request.SessionId,
                                                context.CancellationToken);
      var results = resultTable_.AbortSessionResults(request.SessionId,
                                                     context.CancellationToken);
      var sessions = sessionTable_.CancelSessionAsync(request.SessionId,
                                                      context.CancellationToken);

      await tasks.ConfigureAwait(false);
      await results.ConfigureAwait(false);
      return new CancelSessionResponse
             {
               Session = (await sessions.ConfigureAwait(false)).ToGrpcSessionRaw(),
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

  /// <inheritdoc />
  [RequiresPermission(typeof(GrpcSessionsService),
                      nameof(GetSession))]
  public override async Task<GetSessionResponse> GetSession(GetSessionRequest request,
                                                            ServerCallContext context)
  {
    using var measure = meter_.CountAndTime();
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

  /// <inheritdoc />
  [RequiresPermission(typeof(GrpcSessionsService),
                      nameof(ListSessions))]
  public override async Task<ListSessionsResponse> ListSessions(ListSessionsRequest request,
                                                                ServerCallContext   context)
  {
    using var measure = meter_.CountAndTime();

    var sessionTable = request.PageSize == 0
                         ? sessionTable_.Secondary
                         : sessionTable_;

    try
    {
      var (sessions, totalCount) = await sessionTable.ListSessionsAsync(request.Filters is null
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

  /// <inheritdoc />
  [RequiresPermission(typeof(GrpcSessionsService),
                      nameof(CreateSession))]
  public override async Task<CreateSessionReply> CreateSession(CreateSessionRequest request,
                                                               ServerCallContext    context)
  {
    using var measure = meter_.CountAndTime();
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

  /// <inheritdoc />
  [RequiresPermission(typeof(GrpcSessionsService),
                      nameof(PauseSession))]
  public override async Task<PauseSessionResponse> PauseSession(PauseSessionRequest request,
                                                                ServerCallContext   context)
  {
    using var measure = meter_.CountAndTime();
    try
    {
      var session = await TaskLifeCycleHelper.PauseAsync(taskTable_,
                                                         sessionTable_,
                                                         request.SessionId,
                                                         context.CancellationToken)
                                             .ConfigureAwait(false);

      return new PauseSessionResponse
             {
               Session = session.ToGrpcSessionRaw(),
             };
    }
    catch (SessionNotFoundException e)
    {
      logger_.LogWarning(e,
                         "Error while pausing session");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Session not found"));
    }
    catch (InvalidSessionTransitionException e)
    {
      logger_.LogWarning(e,
                         "Error while pausing session");
      throw new RpcException(new Status(StatusCode.FailedPrecondition,
                                        "Session is in a state that cannot be paused"));
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                         "Error while pausing session");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see application logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                         "Error while pausing session");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see application logs"));
    }
  }


  /// <inheritdoc />
  [RequiresPermission(typeof(GrpcSessionsService),
                      nameof(CloseSession))]
  public override async Task<CloseSessionResponse> CloseSession(CloseSessionRequest request,
                                                                ServerCallContext   context)
  {
    using var measure = meter_.CountAndTime();
    try
    {
      var session = await sessionTable_.GetSessionAsync(request.SessionId,
                                                        context.CancellationToken)
                                       .ConfigureAwait(false);

      session = await sessionTable_.CloseSessionAsync(request.SessionId,
                                                      session.CreationDate,
                                                      context.CancellationToken)
                                   .ConfigureAwait(false);

      logger_.LogInformation("Closed {sessionId}",
                             session);

      return new CloseSessionResponse
             {
               Session = session.ToGrpcSessionRaw(),
             };
    }
    catch (SessionNotFoundException e)
    {
      logger_.LogWarning(e,
                         "Error while closing session");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Session not found"));
    }
    catch (InvalidSessionTransitionException e)
    {
      logger_.LogWarning(e,
                         "Error while closing session");
      throw new RpcException(new Status(StatusCode.FailedPrecondition,
                                        "Session is in a state that cannot be closed"));
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                         "Error while closing session");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see application logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                         "Error while closing session");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see application logs"));
    }
  }

  /// <inheritdoc />
  [RequiresPermission(typeof(GrpcSessionsService),
                      nameof(PurgeSession))]
  public override async Task<PurgeSessionResponse> PurgeSession(PurgeSessionRequest request,
                                                                ServerCallContext   context)
  {
    using var measure = meter_.CountAndTime();
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
    catch (InvalidSessionTransitionException e)
    {
      logger_.LogWarning(e,
                         "Error while purging session");
      throw new RpcException(new Status(StatusCode.FailedPrecondition,
                                        "Session is in a state that cannot be purged"));
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
                         "Error while purging session");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see application logs"));
    }
  }

  /// <inheritdoc />
  [RequiresPermission(typeof(GrpcSessionsService),
                      nameof(DeleteSession))]
  public override async Task<DeleteSessionResponse> DeleteSession(DeleteSessionRequest request,
                                                                  ServerCallContext    context)
  {
    using var measure = meter_.CountAndTime();
    try
    {
      var session = new SessionData(request.SessionId,
                                    SessionStatus.Deleted,
                                    Array.Empty<string>(),
                                    new TaskOptions());

      try
      {
        session = await sessionTable_.GetSessionAsync(request.SessionId,
                                                      context.CancellationToken)
                                     .ConfigureAwait(false);
      }
      catch (Exception)
      {
        // Session may not exist or be already deleted
        logger_.LogDebug("Session {sessionId} not found; returning an empty one",
                         request.SessionId);
      }

      await Task.WhenAll(taskTable_.DeleteTasksAsync(request.SessionId,
                                                     context.CancellationToken),
                         resultTable_.DeleteResults(request.SessionId,
                                                    context.CancellationToken))
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
                         "Error while deleting session");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Session not found"));
    }
    catch (InvalidSessionTransitionException e)
    {
      logger_.LogWarning(e,
                         "Error while deleting session");
      throw new RpcException(new Status(StatusCode.FailedPrecondition,
                                        "Session is in a state that cannot be deleted"));
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                         "Error while deleting session");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see application logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                         "Error while deleting session");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see application logs"));
    }
  }

  /// <inheritdoc />
  [RequiresPermission(typeof(GrpcSessionsService),
                      nameof(ResumeSession))]
  public override async Task<ResumeSessionResponse> ResumeSession(ResumeSessionRequest request,
                                                                  ServerCallContext    context)
  {
    using var measure = meter_.CountAndTime();
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
                         "Error while resuming session");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Session not found"));
    }
    catch (InvalidSessionTransitionException e)
    {
      logger_.LogWarning(e,
                         "Error while resuming session");
      throw new RpcException(new Status(StatusCode.FailedPrecondition,
                                        "Session is in a state that cannot be cancelled"));
    }
    catch (ArmoniKException e)
    {
      logger_.LogWarning(e,
                         "Error while resuming session");
      throw new RpcException(new Status(StatusCode.Internal,
                                        "Internal Armonik Exception, see application logs"));
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                         "Error while resuming session");
      throw new RpcException(new Status(StatusCode.Unknown,
                                        "Unknown Exception, see application logs"));
    }
  }

  /// <inheritdoc />
  [RequiresPermission(typeof(GrpcSessionsService),
                      nameof(StopSubmission))]
  public override async Task<StopSubmissionResponse> StopSubmission(StopSubmissionRequest request,
                                                                    ServerCallContext     context)
  {
    using var measure = meter_.CountAndTime();
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
