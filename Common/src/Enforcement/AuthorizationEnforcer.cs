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
using System.Linq.Expressions;
using System.Threading.Tasks;

using ArmoniK.Core.Authorization;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Storage;

using Grpc.Core;

using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Enforcement;

/// <summary>
///   Decides requests and refuses those that are not allowed
/// </summary>
public class AuthorizationEnforcer
{
  /// <summary>
  ///   Key of <see cref="HttpContext.Items" /> where the caller authenticated by <see cref="AuthenticateAsync" /> is kept
  ///   for
  ///   <see cref="AuthorizeAsync" />
  /// </summary>
  public static readonly object CallerKey = new();

  private readonly AuthorizationAudit             audit_;
  private readonly CallerResolver                 callerResolver_;
  private readonly IDecisionEngineProvider        engineProvider_;
  private readonly ILogger<AuthorizationEnforcer> logger_;
  private readonly Submitter                      options_;
  private readonly IResultTable                   resultTable_;
  private readonly ISessionTable                  sessionTable_;
  private readonly ITaskTable                     taskTable_;

  /// <summary>
  ///   Build the enforcer
  /// </summary>
  public AuthorizationEnforcer(IDecisionEngineProvider        engineProvider,
                               CallerResolver                 callerResolver,
                               AuthorizationAudit             audit,
                               ISessionTable                  sessionTable,
                               ITaskTable                     taskTable,
                               IResultTable                   resultTable,
                               Submitter                      options,
                               ILogger<AuthorizationEnforcer> logger)
  {
    engineProvider_ = engineProvider;
    callerResolver_ = callerResolver;
    audit_          = audit;
    sessionTable_   = sessionTable;
    taskTable_      = taskTable;
    resultTable_    = resultTable;
    options_        = options;
    logger_         = logger;
  }

  /// <summary>
  ///   Authenticate the caller of a request from the headers of the request only
  /// </summary>
  /// <param name="service">Name of the service class</param>
  /// <param name="method">Name of the method</param>
  /// <param name="rule">Rule of the method</param>
  /// <param name="http">HTTP context of the request</param>
  /// <returns>The caller</returns>
  /// <exception cref="RpcException">
  ///   <see cref="StatusCode.Unauthenticated" /> without credential or with an invalid one,
  ///   <see cref="StatusCode.Unavailable" /> when the credential could not be checked
  /// </exception>
  public async Task<AuthenticatedCaller> AuthenticateAsync(string      service,
                                                           string      method,
                                                           MethodRule  rule,
                                                           HttpContext http)
  {
    AuthenticatedCaller? caller;

    try
    {
      caller = await callerResolver_.ResolveAsync(http)
                                    .ConfigureAwait(false);
    }
    catch (FormatException e)
    {
      audit_.Record(service,
                    method,
                    null,
                    "Unauthenticated",
                    rule.Permissions,
                    [],
                    [],
                    e.Message);
      throw new RpcException(new Status(StatusCode.Unauthenticated,
                                        e.Message));
    }
    catch (Exception e) when (e is not OperationCanceledException)
    {
      // The credential could not be checked, which says nothing of it: answered as unavailable, so that callers retry
      logger_.LogWarning(e,
                         "Authentication of {Service}.{Method} faulted",
                         service,
                         method);
      audit_.Record(service,
                    method,
                    null,
                    "Fault",
                    rule.Permissions,
                    [],
                    [],
                    e.Message);
      throw new RpcException(new Status(StatusCode.Unavailable,
                                        "The credential could not be checked"));
    }

    if (caller is null)
    {
      audit_.Record(service,
                    method,
                    null,
                    "Unauthenticated",
                    rule.Permissions,
                    [],
                    [],
                    "no credential");
      throw new RpcException(new Status(StatusCode.Unauthenticated,
                                        "A token or a client certificate is required"));
    }

    return caller;
  }

  /// <summary>
  ///   Decide a request, throwing when it is not allowed
  /// </summary>
  /// <param name="service">Name of the service class</param>
  /// <param name="method">Name of the method</param>
  /// <param name="rule">Rule of the method</param>
  /// <param name="request">The request, or its first message for a client streaming method</param>
  /// <param name="context">Context of the call, where the decision leaves what handlers need</param>
  /// <exception cref="RpcException">The request is not allowed</exception>
  public async Task AuthorizeAsync(string            service,
                                   string            method,
                                   MethodRule        rule,
                                   object            request,
                                   ServerCallContext context)
  {
    // Authenticated before the request was read, by the authentication middleware: without it, the request of a
    // caller without valid credentials would have been read, so the request is not decided
    if (!context.GetHttpContext()
                .Items.TryGetValue(CallerKey,
                                   out var authenticated) || authenticated is not AuthenticatedCaller caller)
    {
      throw new InvalidOperationException($"{service}.{method} reached authorization without being authenticated: " +
                                          $"{nameof(CallerAuthenticationMiddlewareExt.UseCallerAuthentication)} must run after routing");
    }

    try
    {
      var engine = await engineProvider_.GetEngineAsync(context.CancellationToken)
                                        .ConfigureAwait(false);
      AuthorizationState.Set(context,
                             caller,
                             engine);

      switch (rule.Kind)
      {
        case RuleKind.Credential:
          Allow(service,
                method,
                caller,
                rule,
                [],
                []);
          return;
        case RuleKind.Admission:
          Enforce(service,
                  method,
                  caller,
                  rule,
                  [],
                  engine.DecideAdmission(caller.Caller),
                  () => new RpcException(new Status(StatusCode.NotFound,
                                                    "Not found")));
          return;
        case RuleKind.SessionCreation:
          DecideSessionCreation(service,
                                method,
                                caller,
                                rule,
                                engine,
                                request,
                                context);
          return;
        case RuleKind.Partition:
        {
          var partition = rule.Extract!(request)
                              .Partition ?? "";
          Enforce(service,
                  method,
                  caller,
                  rule,
                  [partition],
                  engine.DecideOnPartition(caller.Caller,
                                           rule.Permissions.Single(),
                                           partition),
                  () => new PartitionNotFoundException($"Partition {partition} was not found"));
          return;
        }
        case RuleKind.Listing:
          await PrepareListingAsync(service,
                                    method,
                                    caller,
                                    rule,
                                    engine,
                                    request,
                                    context)
            .ConfigureAwait(false);
          return;
        case RuleKind.Session:
          await DecideOnSessionsAsync(service,
                                      method,
                                      caller,
                                      rule,
                                      engine,
                                      rule.Extract!(request),
                                      context)
            .ConfigureAwait(false);
          return;
        case RuleKind.Unsupported:
        default:
          audit_.Record(service,
                        method,
                        caller,
                        nameof(DecisionOutcome.Forbidden),
                        rule.Permissions,
                        [],
                        [],
                        "method not supported while authorization is enabled");
          throw new RpcException(new Status(StatusCode.PermissionDenied,
                                            rule.Instead is null
                                              ? "This method is not supported while authorization is enabled"
                                              : $"This method is not supported while authorization is enabled: {rule.Instead}"));
      }
    }
    catch (Exception e) when (e is not (RpcException or OperationCanceledException) && !IsRefusal(e))
    {
      audit_.Record(service,
                    method,
                    caller,
                    "Fault",
                    rule.Permissions,
                    [],
                    [],
                    e.Message);

      if (e is GrantsUnavailableException)
      {
        // Answered as unavailable by the exception interceptor, the failed read being counted where it happened
        throw;
      }

      // A fault says nothing of the session: it is answered as unavailable, so that callers retry, without counting
      // toward the errors that stop the control plane, since the request itself may cause it
      logger_.LogWarning(e,
                         "Authorization of {Service}.{Method} faulted",
                         service,
                         method);
      throw new AuthorizationUnavailableException("Authorization could not be decided",
                                                  e);
    }
  }

  private static bool IsRefusal(Exception e)
    => e is SessionNotFoundException or TaskNotFoundException or ResultNotFoundException or PartitionNotFoundException;

  private void DecideSessionCreation(string              service,
                                     string              method,
                                     AuthenticatedCaller caller,
                                     MethodRule          rule,
                                     DecisionEngine      engine,
                                     object              request,
                                     ServerCallContext   context)
  {
    var (declared, taskOptions) = rule.Creation!(request);
    var partitions = SessionLifeCycleHelper.EffectivePartitions(declared,
                                                                options_.DefaultPartition);

    var creation = engine.DecideSessionCreation(caller.Caller,
                                                partitions);

    // Only the declared partitions are decided on: the default task options must use one of them, or the session would
    // run tasks in a partition its creation was not decided on. Checked once the creation is allowed, so that a caller
    // refused learns nothing of the partitions, the default one included
    var optionsPartition = SessionLifeCycleHelper.TaskOptionsPartition(taskOptions,
                                                                       options_.DefaultPartition);
    if (creation.Decision.IsAllowed && !partitions.Contains(optionsPartition))
    {
      audit_.Record(service,
                    method,
                    caller,
                    "Invalid",
                    rule.Permissions,
                    partitions,
                    [],
                    "partition of the default task options not declared");
      throw new RpcException(new Status(StatusCode.InvalidArgument,
                                        $"The partition {optionsPartition} of the default task options is not one of the partitions of the session: " +
                                        $"[{string.Join(", ", partitions)}]"));
    }

    Enforce(service,
            method,
            caller,
            rule,
            partitions,
            creation.Decision,
            () => new PartitionNotFoundException("One of the partitions does not exist"));

    AuthorizationState.SetAuthorship(context,
                                     new SessionAuthorship(caller.Caller.Identity.Value,
                                                           creation.CreatedUnder));
  }

  private async Task PrepareListingAsync(string              service,
                                         string              method,
                                         AuthenticatedCaller caller,
                                         MethodRule          rule,
                                         DecisionEngine      engine,
                                         object              request,
                                         ServerCallContext   context)
  {
    var permission = rule.Permissions.Single();

    LambdaExpression filter;
    if (rule.Listing == ListingTarget.Partitions)
    {
      var partitions = engine.Partitions(caller.Caller,
                                         permission);
      var ids = partitions.Partitions.ToList();
      filter = partitions.Unrestricted
                 ? _ => true
                 : (Expression<Func<PartitionData, bool>>)(partition => ids.Contains(partition.PartitionId));
    }
    else if (engine.HoldsUnconditionally(caller.Caller,
                                         permission))
    {
      // Held across the cluster: nothing to resolve, which also spares reading every visible session
      filter = rule.Listing switch
               {
                 ListingTarget.Sessions                            => (Expression<Func<SessionData, bool>>)(_ => true),
                 ListingTarget.Results                             => (Expression<Func<Result, bool>>)(_ => true),
                 ListingTarget.Tasks or ListingTarget.Applications => (Expression<Func<TaskData, bool>>)(_ => true),
                 _ => throw new ArgumentOutOfRangeException(nameof(rule),
                                                            rule.Listing,
                                                            "Unknown listing target"),
               };
    }
    else
    {
      var sessions = engine.SessionPredicate(caller.Caller,
                                             permission,
                                             SessionDataFields.Fields);
      if (rule.Listing == ListingTarget.Sessions)
      {
        filter = sessions;
      }
      else
      {
        // Tasks and results do not carry the fields of their session, so the sessions are resolved to ids first:
        // only those the request is confined to, when it names them
        if (rule.ListedSessions?.Invoke(request) is
            {
            } listed)
        {
          var named = listed.ToList();
          sessions = sessions.ExpressionAnd(session => named.Contains(session.SessionId));
        }

        var ids = await sessionTable_.FindSessionsAsync(sessions,
                                                        session => session.SessionId,
                                                        context.CancellationToken)
                                     .ToListAsync(context.CancellationToken)
                                     .ConfigureAwait(false);
        // Applications are aggregated over tasks
        filter = rule.Listing switch
                 {
                   ListingTarget.Results                             => (Expression<Func<Result, bool>>)(result => ids.Contains(result.SessionId)),
                   ListingTarget.Tasks or ListingTarget.Applications => (Expression<Func<TaskData, bool>>)(task => ids.Contains(task.SessionId)),
                   _ => throw new ArgumentOutOfRangeException(nameof(rule),
                                                              rule.Listing,
                                                              "Unknown listing target"),
                 };
      }
    }

    Allow(service,
          method,
          caller,
          rule,
          [],
          []);
    AuthorizationState.SetListingFilter(context,
                                        filter);
  }

  private async Task DecideOnSessionsAsync(string              service,
                                           string              method,
                                           AuthenticatedCaller caller,
                                           MethodRule          rule,
                                           DecisionEngine      engine,
                                           RequestTargets      targets,
                                           ServerCallContext   context)
  {
    var cancellationToken = context.CancellationToken;

    // A request naming nothing acts on nothing and reveals nothing, as an empty list of tasks to cancel: allowed,
    // answered by the handler as without authorization. Ids that are all empty still name something, refused below.
    if (targets.SessionId is null && targets.TaskIds.Count == 0 && targets.ResultIds.Count == 0 && targets.ReferencedResultIds.Count == 0)
    {
      Allow(service,
            method,
            caller,
            rule,
            [],
            []);
      return;
    }

    async Task<List<SessionData>> ReadSessionsAsync(List<string> ids)
      => await sessionTable_.FindSessionsAsync(session => ids.Contains(session.SessionId),
                                               session => session,
                                               cancellationToken)
                            .ToListAsync(cancellationToken)
                            .ConfigureAwait(false);

    // The session named by the request is the only one decided on: it is read while the tasks and results are looked up
    var namedSessionRead = targets.SessionId is null
                             ? null
                             : ReadSessionsAsync([targets.SessionId]);
    // A refusal of the lookups leaves the read unawaited: its fault is observed here
    _ = namedSessionRead?.ContinueWith(read => read.Exception,
                                       TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously);

    var sessionIds = new HashSet<string>(StringComparer.Ordinal);

    if (targets.SessionId is not null)
    {
      sessionIds.Add(targets.SessionId);
    }

    if (targets.TaskIds.Count != 0)
    {
      var ids = targets.TaskIds.Distinct()
                       .ToList();
      var found = await taskTable_.FindTasksAsync(task => ids.Contains(task.TaskId),
                                                  task => new
                                                          {
                                                            task.TaskId,
                                                            task.SessionId,
                                                          },
                                                  cancellationToken)
                                  .ToListAsync(cancellationToken)
                                  .ConfigureAwait(false);
      if (found.Count != ids.Count)
      {
        Refuse(service,
               method,
               caller,
               rule,
               ids,
               "task not found");
        throw NotFound(targets);
      }

      sessionIds.UnionWith(found.Select(task => task.SessionId));
    }

    var resultIds = targets.ResultIds.Concat(targets.ReferencedResultIds)
                           .Where(id => !string.IsNullOrEmpty(id))
                           .Distinct()
                           .ToList();
    if (resultIds.Count != 0)
    {
      Expression<Func<Result, bool>> namedResults = result => resultIds.Contains(result.ResultId);
      // Whole rows only for the handlers reading them, so that they do not read them again
      var rows = rule.KeepsResults
                   ? await resultTable_.GetResults(namedResults,
                                                   result => result,
                                                   cancellationToken)
                                       .ToListAsync(cancellationToken)
                                       .ConfigureAwait(false)
                   : null;
      var found = rows?.Select(result => (result.ResultId, result.SessionId))
                      .ToList() ?? await resultTable_.GetResults(namedResults,
                                                                 result => new
                                                                           {
                                                                             result.ResultId,
                                                                             result.SessionId,
                                                                           },
                                                                 cancellationToken)
                                                     .Select(result => (result.ResultId, result.SessionId))
                                                     .ToListAsync(cancellationToken)
                                                     .ConfigureAwait(false);
      // Results named next to a session must belong to it
      if (found.Count != resultIds.Count || (targets.SessionId is not null && found.Any(result => result.SessionId != targets.SessionId)))
      {
        Refuse(service,
               method,
               caller,
               rule,
               resultIds,
               "result not found or in another session");
        throw NotFound(targets);
      }

      sessionIds.UnionWith(found.Select(result => result.SessionId));
      if (rows is not null)
      {
        AuthorizationState.SetResolvedResults(context,
                                              rows);
      }
    }

    // Empty result ids name no session: deciding on none would allow the request
    if (sessionIds.Count == 0)
    {
      Refuse(service,
             method,
             caller,
             rule,
             targets.ResultIds,
             "result not found");
      throw NotFound(targets);
    }

    if (targets.SessionId is not null && sessionIds.Count > 1)
    {
      Refuse(service,
             method,
             caller,
             rule,
             sessionIds,
             "ids of another session");
      throw NotFound(targets);
    }

    var decidedSessionIds = sessionIds.ToList();
    var sessions          = await (namedSessionRead ?? ReadSessionsAsync(decidedSessionIds)).ConfigureAwait(false);
    if (sessions.Count != decidedSessionIds.Count)
    {
      Refuse(service,
             method,
             caller,
             rule,
             decidedSessionIds,
             "session not found");
      throw NotFound(targets);
    }

    AuthorizationState.SetResolvedSessions(context,
                                           sessions);

    var decisions = sessions.Select(SessionDataFields.Facts)
                            .SelectMany(facts => rule.Permissions.Select(permission => engine.DecideOnSession(caller.Caller,
                                                                                                              permission,
                                                                                                              facts,
                                                                                                              rule.Precondition)))
                            .ToList();

    var refusal = decisions.FirstOrDefault(decision => decision.Outcome == DecisionOutcome.NotFound) ??
                  decisions.FirstOrDefault(decision => decision.Outcome == DecisionOutcome.Forbidden);

    if (refusal is not null)
    {
      Refuse(service,
             method,
             caller,
             rule,
             decidedSessionIds,
             refusal.Reason.ToString(),
             refusal.Outcome);
      if (refusal.Outcome == DecisionOutcome.NotFound)
      {
        throw NotFound(targets);
      }

      throw new RpcException(new Status(StatusCode.PermissionDenied,
                                        "Not authorized"));
    }

    Allow(service,
          method,
          caller,
          rule,
          decidedSessionIds,
          decisions.SelectMany(decision => decision.AllowingGrants)
                   .DistinctBy(grant => grant.Id));
  }

  /// <summary>
  ///   The refusal of a request naming what does not exist or what the caller may not see, chosen from the shape of the
  ///   request alone: chosen from the check that failed, it would tell an existing resource from a missing one
  /// </summary>
  /// <param name="targets">What the request names</param>
  /// <returns>The exception answered as not found</returns>
  private static Exception NotFound(RequestTargets targets)
    => targets.TaskIds.Count != 0
         ? new TaskNotFoundException("Task not found")
         : targets.ResultIds.Count != 0 || targets.ReferencedResultIds.Count != 0
           ? new ResultNotFoundException("Result not found")
           : new SessionNotFoundException("Session not found");

  private void Enforce(string              service,
                       string              method,
                       AuthenticatedCaller caller,
                       MethodRule          rule,
                       IEnumerable<string> targets,
                       Decision            decision,
                       Func<Exception>     notFound)
  {
    if (decision.IsAllowed)
    {
      Allow(service,
            method,
            caller,
            rule,
            targets,
            decision.AllowingGrants);
      return;
    }

    Refuse(service,
           method,
           caller,
           rule,
           targets,
           decision.Detail is null
             ? decision.Reason.ToString()
             : $"{decision.Reason} {decision.Detail}",
           decision.Outcome);

    throw decision.Outcome == DecisionOutcome.NotFound
            ? notFound()
            : new RpcException(new Status(StatusCode.PermissionDenied,
                                          "Not authorized"));
  }

  private void Allow(string              service,
                     string              method,
                     AuthenticatedCaller caller,
                     MethodRule          rule,
                     IEnumerable<string> targets,
                     IEnumerable<Grant>  grants)
    => audit_.Record(service,
                     method,
                     caller,
                     nameof(DecisionOutcome.Allow),
                     rule.Permissions,
                     targets,
                     grants);

  private void Refuse(string              service,
                      string              method,
                      AuthenticatedCaller caller,
                      MethodRule          rule,
                      IEnumerable<string> targets,
                      string              reason,
                      DecisionOutcome     outcome = DecisionOutcome.NotFound)
    => audit_.Record(service,
                     method,
                     caller,
                     outcome.ToString(),
                     rule.Permissions,
                     targets,
                     [],
                     reason);
}
