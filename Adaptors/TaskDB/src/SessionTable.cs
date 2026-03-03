// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-2026. All rights reserved.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.TaskDB.Protocol;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Base.Exceptions;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using MessagePack;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.TaskDB;

/// <inheritdoc cref="ISessionTable" />
public class SessionTable : ISessionTable
{
  private readonly TaskDbConnection      connection_;
  private readonly ActivitySource        activitySource_;
  private readonly ILogger<SessionTable> logger_;

  public SessionTable(TaskDbConnection      connection,
                      ActivitySource        activitySource,
                      ILogger<SessionTable> logger)
  {
    connection_     = connection;
    activitySource_ = activitySource;
    logger_         = logger;
  }

  /// <inheritdoc />
  public ILogger Logger => logger_;

  // ── IInitializable ───────────────────────────────────────────────────────

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(HealthCheckResult.Healthy());

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
    => Task.CompletedTask;

  // ── ISessionTable ────────────────────────────────────────────────────────

  /// <inheritdoc />
  public async Task<string> SetSessionDataAsync(IEnumerable<string> partitionIds,
                                                  TaskOptions         defaultTaskOptions,
                                                  CancellationToken   cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();

    var req = new WireCreateSessionRequest
    {
      PartitionIds = partitionIds.ToList(),
      Options      = defaultTaskOptions.ToWire(),
    };

    var (status, payload) = await connection_.SendReceiveAsync(OpCode.CreateSession, req, cancellationToken)
                                             .ConfigureAwait(false);

    if (status != StatusCode.Success)
      throw new ArmoniKException($"CreateSession failed with status 0x{status:X2}");

    return MessagePackSerializer.Deserialize<WireCreateSessionResponse>(payload).SessionId;
  }

  /// <inheritdoc />
  public async Task<SessionData> GetSessionAsync(string            sessionId,
                                                   CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    activity?.SetTag("SessionId", sessionId);

    var req = new WireGetSessionRequest { SessionId = sessionId };
    var (status, payload) = await connection_.SendReceiveAsync(OpCode.GetSession, req, cancellationToken)
                                             .ConfigureAwait(false);

    if (status == StatusCode.NotFound)
      throw new SessionNotFoundException($"Session '{sessionId}' not found.");

    if (status != StatusCode.Success)
      throw new ArmoniKException($"GetSession failed with status 0x{status:X2}");

    return MessagePackSerializer.Deserialize<WireSessionData>(payload).ToDomain();
  }

  /// <inheritdoc />
  public async Task<SessionData?> UpdateOneSessionAsync(string                               sessionId,
                                                          Expression<Func<SessionData, bool>>? filter,
                                                          UpdateDefinition<SessionData>        updates,
                                                          bool                                 before            = false,
                                                          CancellationToken                    cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();

    var req = new WireUpdateOneSessionRequest
    {
      SessionId = sessionId,
      Updates   = updates.ToWireUpdates(),
    };

    var (status, payload) = await connection_.SendReceiveAsync(OpCode.UpdateOneSession, req, cancellationToken)
                                             .ConfigureAwait(false);

    if (status is StatusCode.NotFound or StatusCode.FilterMismatch)
      return null;

    if (status != StatusCode.Success)
      throw new ArmoniKException($"UpdateOneSession failed with status 0x{status:X2}");

    if (payload.Length == 0)
      return null;

    var result = MessagePackSerializer.Deserialize<WireSessionData>(payload).ToDomain();

    // If a filter was provided, apply it in-memory for correctness
    if (filter is not null && !filter.Compile()(result))
      return null;

    return result;
  }

  /// <inheritdoc />
  // ISessionTable.ListSessionsAsync returns Task<(IEnumerable<SessionData>, long)>
  public async Task<(IEnumerable<SessionData> sessions, long totalCount)> ListSessionsAsync(
    Expression<Func<SessionData, bool>>    filter,
    Expression<Func<SessionData, object?>> orderField,
    bool                                   ascOrder,
    int                                    page,
    int                                    pageSize,
    CancellationToken                      cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();

    var req = new WireFindSessionsRequest
    {
      Filter    = new WireCoarseSessionFilter(),
      OrderBy   = ExtractFieldName(orderField),
      Ascending = ascOrder,
      Page      = page,
      PageSize  = pageSize,
    };

    var (status, payload) = await connection_.SendReceiveAsync(OpCode.FindSessions, req, cancellationToken)
                                             .ConfigureAwait(false);

    if (status != StatusCode.Success)
      throw new ArmoniKException($"ListSessions failed with status 0x{status:X2}");

    var resp           = MessagePackSerializer.Deserialize<WireListSessionsResponse>(payload);
    var compiledFilter = filter.Compile();

    var sessions = resp.Sessions
                       .Select(w => w.ToDomain())
                       .Where(compiledFilter);

    return (sessions, resp.TotalCount);
  }

  /// <inheritdoc />
  // ISessionTable.FindSessionsAsync<T>: filter + selector
  public IAsyncEnumerable<T> FindSessionsAsync<T>(Expression<Func<SessionData, bool>> filter,
                                                   Expression<Func<SessionData, T>>    selector,
                                                   CancellationToken                   cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    return FindSessionsInternalAsync(filter, selector, cancellationToken);
  }

  private async IAsyncEnumerable<T> FindSessionsInternalAsync<T>(
    Expression<Func<SessionData, bool>> filter,
    Expression<Func<SessionData, T>>    selector,
    [EnumeratorCancellation]
    CancellationToken                   cancellationToken)
  {
    var req = new WireFindSessionsRequest
    {
      Filter    = new WireCoarseSessionFilter(),
      Ascending = true,
      PageSize  = 0, // stream all
    };

    var compiledFilter   = filter.Compile();
    var compiledSelector = selector.Compile();

    await foreach (var frame in connection_.StreamAsync(OpCode.FindSessions, req, cancellationToken)
                                           .ConfigureAwait(false))
    {
      var session = MessagePackSerializer.Deserialize<WireSessionData>(frame).ToDomain();
      if (compiledFilter(session))
        yield return compiledSelector(session);
    }
  }

  /// <inheritdoc />
  public async Task DeleteSessionAsync(string            sessionId,
                                        CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    activity?.SetTag("DeleteSessionId", sessionId);

    var req = new WireDeleteSessionRequest { SessionId = sessionId };
    var (status, _) = await connection_.SendReceiveAsync(OpCode.DeleteSession, req, cancellationToken)
                                       .ConfigureAwait(false);

    if (status != StatusCode.Success && status != StatusCode.NotFound)
      throw new ArmoniKException($"DeleteSession failed with status 0x{status:X2}");
  }

  private static string ExtractFieldName<T>(Expression<Func<T, object?>> expr)
  {
    var body = (Expression)expr.Body;
    if (body is UnaryExpression u) body = u.Operand;
    return body is MemberExpression m ? m.Member.Name : "";
  }
}
