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
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Storage;

using Grpc.Core;

namespace ArmoniK.Core.Common.Enforcement;

/// <summary>
///   Who created a session and the principals it was created under, as decided when it was authorized
/// </summary>
/// <param name="Owner">Principal recorded as owner</param>
/// <param name="CreatedUnder">Principals of the grants that permitted the creation</param>
public sealed record SessionAuthorship(string                Owner,
                                       IReadOnlyList<string> CreatedUnder);

/// <summary>
///   What the authorization of a call leaves for its handler. Nothing is left when authorization is disabled
/// </summary>
public static class AuthorizationState
{
  private const string CallerKey     = "ArmoniK.Authorization.Caller";
  private const string EngineKey     = "ArmoniK.Authorization.Engine";
  private const string AuthorshipKey = "ArmoniK.Authorization.Authorship";
  private const string FilterKey     = "ArmoniK.Authorization.Filter";
  private const string ResultsKey    = "ArmoniK.Authorization.Results";
  private const string SessionsKey   = "ArmoniK.Authorization.Sessions";

  internal static void Set(ServerCallContext   context,
                           AuthenticatedCaller caller,
                           DecisionEngine      engine)
  {
    context.UserState[CallerKey] = caller;
    context.UserState[EngineKey] = engine;
  }

  internal static void SetAuthorship(ServerCallContext context,
                                     SessionAuthorship authorship)
    => context.UserState[AuthorshipKey] = authorship;

  internal static void SetResolvedResults(ServerCallContext   context,
                                          IEnumerable<Result> results)
    => context.UserState[ResultsKey] = results.ToDictionary(result => result.ResultId);

  internal static void SetResolvedSessions(ServerCallContext        context,
                                           IEnumerable<SessionData> sessions)
    => context.UserState[SessionsKey] = sessions.ToDictionary(session => session.SessionId);

  /// <summary>
  ///   A result read to decide the call, so that a read-only handler does not read it again
  /// </summary>
  /// <param name="context">Context of the call</param>
  /// <param name="resultId">Identifier of the result</param>
  /// <returns>The result, null when authorization is disabled or did not read it</returns>
  /// <remarks>Read before the handler runs: a handler changing the result must read it itself.</remarks>
  public static Result? ResolvedResult(this ServerCallContext context,
                                       string                 resultId)
    => context.UserState.TryGetValue(ResultsKey,
                                     out var results)
         ? ((Dictionary<string, Result>)results).GetValueOrDefault(resultId)
         : null;

  /// <summary>
  ///   A session read to decide the call, so that a read-only handler does not read it again
  /// </summary>
  /// <param name="context">Context of the call</param>
  /// <param name="sessionId">Identifier of the session</param>
  /// <returns>The session, null when authorization is disabled or did not read it</returns>
  /// <remarks>Read before the handler runs: a handler changing the session must read it itself.</remarks>
  public static SessionData? ResolvedSession(this ServerCallContext context,
                                             string                 sessionId)
    => context.UserState.TryGetValue(SessionsKey,
                                     out var sessions)
         ? ((Dictionary<string, SessionData>)sessions).GetValueOrDefault(sessionId)
         : null;

  /// <summary>
  ///   The session read to decide the call, or read from the table when the decision did not read it
  /// </summary>
  /// <param name="context">Context of the call</param>
  /// <param name="sessionTable">Table read when the decision did not read the session</param>
  /// <param name="sessionId">Identifier of the session</param>
  /// <returns>The session</returns>
  /// <remarks>Read before the handler runs: a handler changing the session must read it itself.</remarks>
  public static Task<SessionData> GetSessionAsync(this ServerCallContext context,
                                                  ISessionTable          sessionTable,
                                                  string                 sessionId)
    => context.ResolvedSession(sessionId) is
       {
       } session
         ? Task.FromResult(session)
         : sessionTable.GetSessionAsync(sessionId,
                                        context.CancellationToken);

  internal static void SetListingFilter(ServerCallContext context,
                                        LambdaExpression  filter)
    => context.UserState[FilterKey] = filter;

  /// <summary>
  ///   Who creates the session, when authorization is enabled
  /// </summary>
  /// <param name="context">Context of the call</param>
  /// <returns>The authorship, null when authorization is disabled</returns>
  public static SessionAuthorship? GetSessionAuthorship(this ServerCallContext context)
    => context.UserState.TryGetValue(AuthorshipKey,
                                     out var authorship)
         ? (SessionAuthorship)authorship
         : null;

  /// <summary>
  ///   Restrict the filter of a listing to what the caller holds the permission on
  /// </summary>
  /// <param name="context">Context of the call</param>
  /// <param name="filter">Filter of the request</param>
  /// <typeparam name="T">Type listed</typeparam>
  /// <returns>The filter, restricted when authorization is enabled</returns>
  public static Expression<Func<T, bool>> RestrictListing<T>(this ServerCallContext    context,
                                                             Expression<Func<T, bool>> filter)
    => context.UserState.TryGetValue(FilterKey,
                                     out var authorized)
         ? filter.ExpressionAnd((Expression<Func<T, bool>>)authorized)
         : filter;

  /// <summary>
  ///   Which of the given sessions may show the data reference of their results, the opaque id: those where the caller
  ///   holds <c>data:download</c>, since the reference gives the data; all of them when authorization is disabled
  /// </summary>
  /// <param name="context">Context of the call</param>
  /// <param name="sessionTable">Sessions, read for those the decision did not resolve already</param>
  /// <param name="sessionIds">Sessions of the results returned</param>
  /// <returns>Whether the results of a session show their data reference</returns>
  public static async Task<Func<string, bool>> DataReferenceShownAsync(this ServerCallContext context,
                                                                       ISessionTable          sessionTable,
                                                                       IEnumerable<string>    sessionIds)
  {
    if (!context.UserState.TryGetValue(CallerKey,
                                       out var stored) || !context.UserState.TryGetValue(EngineKey,
                                                                                         out var storedEngine))
    {
      return _ => true;
    }

    var caller = ((AuthenticatedCaller)stored).Caller;
    var engine = (DecisionEngine)storedEngine;

    if (engine.HoldsUnconditionally(caller,
                                    Permission.DataDownload))
    {
      return _ => true;
    }

    var ids = sessionIds.ToHashSet();
    var sessions = ids.Select(context.ResolvedSession)
                      .OfType<SessionData>()
                      .ToList();
    var missing = ids.Except(sessions.Select(session => session.SessionId))
                     .ToList();
    if (missing.Count != 0)
    {
      sessions.AddRange(await sessionTable.FindSessionsAsync(session => missing.Contains(session.SessionId),
                                                             session => session,
                                                             context.CancellationToken)
                                          .ToListAsync(context.CancellationToken)
                                          .ConfigureAwait(false));
    }

    var shown = sessions.Where(session => engine.DecideOnSession(caller,
                                                                 Permission.DataDownload,
                                                                 SessionDataFields.Facts(session))
                                                .Outcome == DecisionOutcome.Allow)
                        .Select(session => session.SessionId)
                        .ToHashSet();
    return shown.Contains;
  }

  /// <summary>
  ///   What the caller holds, when authorization is enabled
  /// </summary>
  /// <param name="context">Context of the call</param>
  /// <returns>The caller and what it holds, null when authorization is disabled</returns>
  public static (AuthenticatedCaller Caller, CallerDescription Description)? DescribeCaller(this ServerCallContext context)
    => context.UserState.TryGetValue(CallerKey,
                                     out var caller) && context.UserState.TryGetValue(EngineKey,
                                                                                      out var engine)
         ? ((AuthenticatedCaller)caller, ((DecisionEngine)engine).Describe(((AuthenticatedCaller)caller).Caller))
         : null;
}
