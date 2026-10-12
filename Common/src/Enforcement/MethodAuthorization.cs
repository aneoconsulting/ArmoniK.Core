// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Events;
using ArmoniK.Api.gRPC.V1.Partitions;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.Sessions;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Core.Authorization;
using ArmoniK.Core.Common.gRPC.Services;

using Grpc.Core;
using Grpc.Reflection;

namespace ArmoniK.Core.Common.Enforcement;

/// <summary>
///   How a method is authorized
/// </summary>
public enum RuleKind
{
  /// <summary>Decided on the sessions the request resolves to</summary>
  Session,

  /// <summary>Decided once per partition the created session declares</summary>
  SessionCreation,

  /// <summary>Decided on one partition</summary>
  Partition,

  /// <summary>Returns what the caller holds the permission on, filtered by its handler</summary>
  Listing,

  /// <summary>Requires a credential and a grant naming the caller or one of its roles</summary>
  Admission,

  /// <summary>Requires a credential only</summary>
  Credential,

  /// <summary>Not supported while authorization is enabled</summary>
  Unsupported,
}

/// <summary>
///   What a listing returns
/// </summary>
public enum ListingTarget
{
  /// <summary>Sessions</summary>
  Sessions,

  /// <summary>Tasks</summary>
  Tasks,

  /// <summary>Results</summary>
  Results,

  /// <summary>Applications, aggregated over tasks</summary>
  Applications,

  /// <summary>Partitions</summary>
  Partitions,
}

/// <summary>
///   What a request names
/// </summary>
/// <param name="SessionId">Session named by the request, null when none</param>
/// <param name="TaskIds">Tasks the request acts on</param>
/// <param name="ResultIds">Results the request acts on</param>
/// <param name="ReferencedResultIds">Results the request uses, which must belong to <paramref name="SessionId" /></param>
/// <param name="Partition">Partition the request acts on</param>
/// <param name="Sessions">Other sessions the request acts on</param>
public sealed record RequestTargets(string?                      SessionId,
                                    IReadOnlyCollection<string>  TaskIds,
                                    IReadOnlyCollection<string>  ResultIds,
                                    IReadOnlyCollection<string>  ReferencedResultIds,
                                    string?                      Partition = null,
                                    IReadOnlyCollection<string>? Sessions  = null)
{
  /// <summary>Whether the request names nothing</summary>
  public bool IsEmpty
    => SessionId is null && TaskIds.Count == 0 && ResultIds.Count == 0 && ReferencedResultIds.Count == 0 && (Sessions?.Count ?? 0) == 0;

  /// <summary>A request naming sessions or tasks through a legacy task filter, nothing when it names neither</summary>
  public static RequestTargets OfTaskFilter(TaskFilter filter)
    => filter.IdsCase switch
       {
         TaskFilter.IdsOneofCase.Session => new RequestTargets(null,
                                                               [],
                                                               [],
                                                               [],
                                                               Sessions: filter.Session.Ids.ToList()),
         TaskFilter.IdsOneofCase.Task => OfTasks(filter.Task.Ids),
         _                            => new RequestTargets(null,
                                                            [],
                                                            [],
                                                            []),
       };

  /// <summary>A request naming one session</summary>
  public static RequestTargets OfSession(string sessionId)
    => new(sessionId,
           [],
           [],
           []);

  /// <summary>A request naming tasks</summary>
  public static RequestTargets OfTasks(IEnumerable<string> taskIds)
    => new(null,
           taskIds.ToList(),
           [],
           []);

  /// <summary>A request naming results, possibly within a declared session</summary>
  public static RequestTargets OfResults(string?             sessionId,
                                         IEnumerable<string> resultIds)
    => new(string.IsNullOrEmpty(sessionId)
             ? null
             : sessionId,
           [],
           resultIds.ToList(),
           []);
}

/// <summary>
///   How a method is authorized
/// </summary>
/// <param name="Kind">How the method is authorized</param>
/// <param name="Permissions">Permissions the method requires, all of them</param>
/// <param name="Extract">What the request names, for <see cref="RuleKind.Session" /> and <see cref="RuleKind.Partition" /></param>
/// <param name="Listing">What the listing returns, for <see cref="RuleKind.Listing" /></param>
/// <param name="Precondition">Precondition the method carries</param>
/// <param name="Creation">Partitions declared and partition of the default task options, for <see cref="RuleKind.SessionCreation" /></param>
/// <param name="ListedSessions">
///   Sessions the filter of the request confines a listing to, null when it reaches any session, for
///   <see cref="RuleKind.Listing" />
/// </param>
public sealed record MethodRule(RuleKind                                                                   Kind,
                                IReadOnlyList<Permission>                                                  Permissions,
                                Func<object, RequestTargets>?                                              Extract        = null,
                                ListingTarget                                                              Listing        = ListingTarget.Sessions,
                                Precondition                                                               Precondition   = Precondition.None,
                                Func<object, (IReadOnlyCollection<string> Declared, string? TaskOptions)>? Creation       = null,
                                Func<object, IReadOnlyCollection<string>?>?                                ListedSessions = null);

/// <summary>
///   The authorization rule of every method of the control plane
/// </summary>
public static class MethodAuthorization
{
  private static readonly Dictionary<(string Service, string Method), MethodRule> RulesByMethod = new()
  {
    // Sessions
    [(nameof(GrpcSessionsService), nameof(GrpcSessionsService.CreateSession))] = new(RuleKind.SessionCreation,
                                                                                      [Permission.SessionCreate],
                                                                                      Creation: request =>
                                                                                                {
                                                                                                  var r = (Api.gRPC.V1.Sessions.CreateSessionRequest)request;
                                                                                                  return (r.PartitionIds, r.DefaultTaskOption?.PartitionId);
                                                                                                }),
    [(nameof(GrpcSessionsService), nameof(GrpcSessionsService.GetSession))] = OnSession<GetSessionRequest>(r => r.SessionId,
                                                                                                           Permission.SessionRead),
    [(nameof(GrpcSessionsService), nameof(GrpcSessionsService.ListSessions))] = Listing(ListingTarget.Sessions,
                                                                                        Permission.SessionRead),
    [(nameof(GrpcSessionsService), nameof(GrpcSessionsService.CancelSession))] = OnSession<CancelSessionRequest>(r => r.SessionId,
                                                                                                                 Permission.SessionCancel),
    [(nameof(GrpcSessionsService), nameof(GrpcSessionsService.PauseSession))] = OnSession<PauseSessionRequest>(r => r.SessionId,
                                                                                                               Permission.SessionPause),
    [(nameof(GrpcSessionsService), nameof(GrpcSessionsService.ResumeSession))] = OnSession<ResumeSessionRequest>(r => r.SessionId,
                                                                                                                 Permission.SessionResume),
    [(nameof(GrpcSessionsService), nameof(GrpcSessionsService.CloseSession))] = OnSession<CloseSessionRequest>(r => r.SessionId,
                                                                                                               Permission.SessionClose),
    [(nameof(GrpcSessionsService), nameof(GrpcSessionsService.PurgeSession))] = OnSession<PurgeSessionRequest>(r => r.SessionId,
                                                                                                               Permission.SessionPurge),
    [(nameof(GrpcSessionsService), nameof(GrpcSessionsService.DeleteSession))] = OnSession<DeleteSessionRequest>(r => r.SessionId,
                                                                                                                 Permission.SessionDelete),
    [(nameof(GrpcSessionsService), nameof(GrpcSessionsService.StopSubmission))] = OnSession<StopSubmissionRequest>(r => r.SessionId,
                                                                                                                   Permission.SessionStopSubmission),

    // Tasks
    [(nameof(GrpcTasksService), nameof(GrpcTasksService.GetTask))] = OnRequest<GetTaskRequest>(r => RequestTargets.OfTasks([r.TaskId]),
                                                                                               Permission.TaskRead),
    [(nameof(GrpcTasksService), nameof(GrpcTasksService.ListTasks))] = Listing<ListTasksRequest>(ListingTarget.Tasks,
                                                                                                  r => ListedSessions(r.Filters),
                                                                                                  Permission.TaskRead),
    [(nameof(GrpcTasksService), nameof(GrpcTasksService.ListTasksDetailed))] = Listing<ListTasksRequest>(ListingTarget.Tasks,
                                                                                                          r => ListedSessions(r.Filters),
                                                                                                          Permission.TaskRead),
    [(nameof(GrpcTasksService), nameof(GrpcTasksService.CountTasksByStatus))] = Listing<CountTasksByStatusRequest>(ListingTarget.Tasks,
                                                                                                                    r => ListedSessions(r.Filters),
                                                                                                                    Permission.TaskRead),
    [(nameof(GrpcTasksService), nameof(GrpcTasksService.GetResultIds))] = OnRequest<GetResultIdsRequest>(r => RequestTargets.OfTasks(r.TaskId),
                                                                                                          Permission.TaskRead),
    [(nameof(GrpcTasksService), nameof(GrpcTasksService.CancelTasks))] = OnRequest<CancelTasksRequest>(r => RequestTargets.OfTasks(r.TaskIds),
                                                                                                        Permission.TaskCancel),
    [(nameof(GrpcTasksService), nameof(GrpcTasksService.SubmitTasks))] = OnRequest<SubmitTasksRequest>(r => new RequestTargets(r.SessionId,
                                                                                                                                [],
                                                                                                                                [],
                                                                                                                                r.TaskCreations
                                                                                                                                 .SelectMany(creation => creation.DataDependencies
                                                                                                                                                                 .Concat(creation.ExpectedOutputKeys)
                                                                                                                                                                 .Append(creation.PayloadId))
                                                                                                                                 .ToList()),
                                                                                                        Permission.TaskSubmit),

    // Results
    [(nameof(GrpcResultsService), nameof(GrpcResultsService.GetResult))] = OnRequest<GetResultRequest>(r => RequestTargets.OfResults(null,
                                                                                                                                     [r.ResultId]),
                                                                                                        Permission.DataRead),
    [(nameof(GrpcResultsService), nameof(GrpcResultsService.GetOwnerTaskId))] = OnRequest<GetOwnerTaskIdRequest>(r => RequestTargets.OfResults(r.SessionId,
                                                                                                                                               r.ResultId),
                                                                                                                  Permission.DataRead),
    [(nameof(GrpcResultsService), nameof(GrpcResultsService.ListResults))] = Listing<ListResultsRequest>(ListingTarget.Results,
                                                                                                    r => ListedSessions(r.Filters),
                                                                                                    Permission.DataRead),
    [(nameof(GrpcResultsService), nameof(GrpcResultsService.CreateResultsMetaData))] = OnSession<CreateResultsMetaDataRequest>(r => r.SessionId,
                                                                                                                               Permission.DataDeclare),
    [(nameof(GrpcResultsService), nameof(GrpcResultsService.CreateResults))] = OnSession<CreateResultsRequest>(r => r.SessionId,
                                                                                                               Permission.DataDeclare,
                                                                                                               Permission.DataProvide),
    [(nameof(GrpcResultsService), nameof(GrpcResultsService.UploadResultData))] = OnRequest<UploadResultDataRequest>(UploadTargets,
                                                                                                                      Permission.DataProvide),
    [(nameof(GrpcResultsService), nameof(GrpcResultsService.ImportResultsData))] = OnRequest<ImportResultsDataRequest>(r => RequestTargets.OfResults(r.SessionId,
                                                                                                                                                     r.Results.Select(result => result.ResultId)),
                                                                                                                        Permission.DataImport),
    [(nameof(GrpcResultsService), nameof(GrpcResultsService.DownloadResultData))] = OnRequest<DownloadResultDataRequest>(r => RequestTargets.OfResults(r.SessionId,
                                                                                                                                                       [r.ResultId]),
                                                                                                                          Permission.DataDownload),
    [(nameof(GrpcResultsService), nameof(GrpcResultsService.DeleteResultsData))] = OnRequest<DeleteResultsDataRequest>(r => RequestTargets.OfResults(r.SessionId,
                                                                                                                                                     r.ResultId),
                                                                                                                        Permission.DataDelete),
    [(nameof(GrpcResultsService), nameof(GrpcResultsService.GetServiceConfiguration))] = new(RuleKind.Admission,
                                                                                              []),

    // Events
    [(nameof(GrpcEventsService), nameof(GrpcEventsService.GetEvents))] = OnSession<EventSubscriptionRequest>(r => r.SessionId,
                                                                                                             Permission.EventWatch),

    // Partitions
    [(nameof(GrpcPartitionsService), nameof(GrpcPartitionsService.GetPartition))] = new(RuleKind.Partition,
                                                                                         [Permission.PartitionRead],
                                                                                         request => new RequestTargets(null,
                                                                                                                       [],
                                                                                                                       [],
                                                                                                                       [],
                                                                                                                       ((GetPartitionRequest)request).Id)),
    [(nameof(GrpcPartitionsService), nameof(GrpcPartitionsService.ListPartitions))] = Listing(ListingTarget.Partitions,
                                                                                              Permission.PartitionRead),

    // Applications
    [(nameof(GrpcApplicationsService), nameof(GrpcApplicationsService.ListApplications))] = Listing(ListingTarget.Applications,
                                                                                                    Permission.TaskRead),

    // Legacy submitter
    [(nameof(GrpcSubmitterService), nameof(GrpcSubmitterService.CreateSession))] = new(RuleKind.SessionCreation,
                                                                                        [Permission.SessionCreate],
                                                                                        Creation: request =>
                                                                                                  {
                                                                                                    var r = (Api.gRPC.V1.Submitter.CreateSessionRequest)request;
                                                                                                    return (r.PartitionIds, r.DefaultTaskOption?.PartitionId);
                                                                                                  }),
    [(nameof(GrpcSubmitterService), nameof(GrpcSubmitterService.CancelSession))] = OnSession<Session>(r => r.Id,
                                                                                                      Permission.SessionCancel),
    [(nameof(GrpcSubmitterService), nameof(GrpcSubmitterService.CreateSmallTasks))] = OnRequest<CreateSmallTaskRequest>(r => new RequestTargets(r.SessionId,
                                                                                                                                                 [],
                                                                                                                                                 [],
                                                                                                                                                 r.TaskRequests
                                                                                                                                                  .SelectMany(task => task.DataDependencies
                                                                                                                                                                          .Concat(task.ExpectedOutputKeys))
                                                                                                                                                  .ToList()),
                                                                                                                         Permission.TaskSubmit,
                                                                                                                         Permission.DataDeclare,
                                                                                                                         Permission.DataProvide),
    // Its dependencies arrive within the stream: not supported while authorization is enabled
    [(nameof(GrpcSubmitterService), nameof(GrpcSubmitterService.CreateLargeTasks))] = new(RuleKind.Unsupported,
                                                                                           []),
    [(nameof(GrpcSubmitterService), nameof(GrpcSubmitterService.CancelTasks))] = OnRequest<TaskFilter>(RequestTargets.OfTaskFilter,
                                                                                                        Permission.TaskCancel),
    [(nameof(GrpcSubmitterService), nameof(GrpcSubmitterService.CountTasks))] = OnRequest<TaskFilter>(RequestTargets.OfTaskFilter,
                                                                                                       Permission.TaskRead),
    [(nameof(GrpcSubmitterService), nameof(GrpcSubmitterService.ListTasks))] = OnRequest<TaskFilter>(RequestTargets.OfTaskFilter,
                                                                                                      Permission.TaskRead),
    [(nameof(GrpcSubmitterService), nameof(GrpcSubmitterService.WaitForCompletion))] = OnRequest<WaitRequest>(r => RequestTargets.OfTaskFilter(r.Filter),
                                                                                                               Permission.TaskRead),
    [(nameof(GrpcSubmitterService), nameof(GrpcSubmitterService.ListSessions))] = OnRequest<SessionFilter>(r => new RequestTargets(null,
                                                                                                                                    [],
                                                                                                                                    [],
                                                                                                                                    [],
                                                                                                                                    Sessions: r.Sessions.ToList()),
                                                                                                            Permission.SessionRead),
    [(nameof(GrpcSubmitterService), nameof(GrpcSubmitterService.TryGetResultStream))] = OnRequest<ResultRequest>(r => RequestTargets.OfResults(r.Session,
                                                                                                                                               [r.ResultId]),
                                                                                                                  Permission.DataDownload),
    [(nameof(GrpcSubmitterService), nameof(GrpcSubmitterService.WaitForAvailability))] = OnRequest<ResultRequest>(r => RequestTargets.OfResults(r.Session,
                                                                                                                                                [r.ResultId]),
                                                                                                                   Permission.DataRead),
    [(nameof(GrpcSubmitterService), nameof(GrpcSubmitterService.TryGetTaskOutput))] = OnRequest<TaskOutputRequest>(r => new RequestTargets(r.Session,
                                                                                                                                            [r.TaskId],
                                                                                                                                            [],
                                                                                                                                            []),
                                                                                                                    Permission.TaskRead),
    [(nameof(GrpcSubmitterService), nameof(GrpcSubmitterService.GetTaskStatus))] = OnRequest<GetTaskStatusRequest>(r => RequestTargets.OfTasks(r.TaskIds),
                                                                                                                    Permission.TaskRead),
    [(nameof(GrpcSubmitterService), nameof(GrpcSubmitterService.GetResultStatus))] = OnRequest<GetResultStatusRequest>(r => RequestTargets.OfResults(r.SessionId,
                                                                                                                                                      r.ResultIds),
                                                                                                                        Permission.DataRead),
    [(nameof(GrpcSubmitterService), nameof(GrpcSubmitterService.GetServiceConfiguration))] = new(RuleKind.Admission,
                                                                                                  []),

    // Without resource
    [(nameof(GrpcAuthService), nameof(GrpcAuthService.GetCurrentUser))] = new(RuleKind.Credential,
                                                                               []),
    [(nameof(GrpcVersionsService), nameof(GrpcVersionsService.ListVersions))] = new(RuleKind.Credential,
                                                                                     []),
    [(nameof(GrpcHealthChecksService), nameof(GrpcHealthChecksService.CheckHealth))] = new(RuleKind.Admission,
                                                                                            []),
  };

  /// <summary>
  ///   Every rule, by service and method
  /// </summary>
  public static IReadOnlyDictionary<(string Service, string Method), MethodRule> Rules
    => RulesByMethod;

  /// <summary>
  ///   The services whose methods must all have a rule
  /// </summary>
  public static IReadOnlyList<Type> Services { get; } =
  [
    typeof(GrpcSessionsService),
    typeof(GrpcTasksService),
    typeof(GrpcResultsService),
    typeof(GrpcEventsService),
    typeof(GrpcPartitionsService),
    typeof(GrpcApplicationsService),
    typeof(GrpcAuthService),
    typeof(GrpcVersionsService),
    typeof(GrpcHealthChecksService),
    typeof(GrpcSubmitterService),
  ];

  /// <summary>
  ///   The rule of a method
  /// </summary>
  /// <param name="service">Name of the service class</param>
  /// <param name="method">Name of the method</param>
  /// <returns>The rule, null for a service served without authorization, <see cref="RuleKind.Unsupported" /> for any other method without rule</returns>
  public static MethodRule? For(string service,
                                string method)
    => RulesByMethod.TryGetValue((service, method),
                                 out var rule)
         ? rule
         : Unauthorized.Any(type => type.Name == service)
           ? null
           : new MethodRule(RuleKind.Unsupported,
                            []);

  /// <summary>
  ///   The services outside the API, served without authorization: a service mapped later is unsupported until it gets rules
  /// </summary>
  private static IReadOnlyList<Type> Unauthorized { get; } =
  [
    typeof(ReflectionServiceImpl),
  ];

  private static MethodRule OnSession<TRequest>(Func<TRequest, string> sessionId,
                                                params Permission[]    permissions)
    => new(RuleKind.Session,
           permissions,
           request => RequestTargets.OfSession(sessionId((TRequest)request)));

  private static MethodRule OnRequest<TRequest>(Func<TRequest, RequestTargets> extract,
                                                params Permission[]            permissions)
    => new(RuleKind.Session,
           permissions,
           request => extract((TRequest)request));

  /// <summary>The result named by the first message of an upload, which must be its identifier</summary>
  private static RequestTargets UploadTargets(UploadResultDataRequest request)
    => request.TypeCase == UploadResultDataRequest.TypeOneofCase.Id
         ? RequestTargets.OfResults(request.Id.SessionId,
                                    [request.Id.ResultId])
         : throw new RpcException(new Status(StatusCode.InvalidArgument,
                                             "Message should be an Id"));

  private static MethodRule Listing(ListingTarget target,
                                    Permission    permission)
    => new(RuleKind.Listing,
           [permission],
           Listing: target);

  private static MethodRule Listing<TRequest>(ListingTarget                              target,
                                              Func<TRequest, IReadOnlyCollection<string>?> listedSessions,
                                              Permission                                 permission)
    => new(RuleKind.Listing,
           [permission],
           Listing: target,
           ListedSessions: request => listedSessions((TRequest)request));

  /// <summary>Sessions a task filter confines a listing to, null when it reaches any session</summary>
  private static IReadOnlyCollection<string>? ListedSessions(Api.gRPC.V1.Tasks.Filters? filters)
    => ListedSessions(filters?.Or.Select(branch => branch.And.Where(field => field.Field.FieldCase == TaskField.FieldOneofCase.TaskSummaryField &&
                                                                             field.Field.TaskSummaryField.Field == TaskSummaryEnumField.SessionId &&
                                                                             field.ValueConditionCase ==
                                                                             Api.gRPC.V1.Tasks.FilterField.ValueConditionOneofCase.FilterString &&
                                                                             field.FilterString.Operator == FilterStringOperator.Equal)
                                                             .Select(field => field.FilterString.Value)));

  /// <summary>Sessions a result filter confines a listing to, null when it reaches any session</summary>
  private static IReadOnlyCollection<string>? ListedSessions(Api.gRPC.V1.Results.Filters? filters)
    => ListedSessions(filters?.Or.Select(branch => branch.And.Where(field => field.Field.FieldCase == ResultField.FieldOneofCase.ResultRawField &&
                                                                             field.Field.ResultRawField.Field == ResultRawEnumField.SessionId &&
                                                                             field.ValueConditionCase ==
                                                                             Api.gRPC.V1.Results.FilterField.ValueConditionOneofCase.FilterString &&
                                                                             field.FilterString.Operator == FilterStringOperator.Equal)
                                                             .Select(field => field.FilterString.Value)));

  /// <summary>
  ///   Sessions a filter confines a listing to: a filter is an OR of branches, each an AND of conditions, so it
  ///   is confined when every branch requires a session to equal a value
  /// </summary>
  /// <remarks>
  ///   Every row the filter matches is in one of the sessions returned, so reading only those keeps the
  ///   listing whole. A branch that requires two different sessions matches nothing: keeping one of them
  ///   still covers it.
  /// </remarks>
  /// <param name="branches">For each branch, the values its session must equal</param>
  /// <returns>The sessions, null when a branch reaches any session or when there is no branch</returns>
  private static HashSet<string>? ListedSessions(IEnumerable<IEnumerable<string>>? branches)
  {
    var sessions = new HashSet<string>(StringComparer.Ordinal);
    foreach (var branch in branches ?? [])
    {
      var session = branch.Take(1)
                          .ToList();
      if (session.Count == 0)
      {
        return null;
      }

      sessions.Add(session[0]);
    }

    return sessions.Count == 0
             ? null
             : sessions;
  }
}
