// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-2026. All rights reserved.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

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

using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

namespace ArmoniK.Core.Adapters.TaskDB;

/// <inheritdoc cref="ITaskTable" />
public class TaskTable : ITaskTable
{
  private readonly TaskDbConnection connection_;
  private readonly ActivitySource   activitySource_;
  private readonly ILogger<TaskTable> logger_;

  public TaskTable(TaskDbConnection     connection,
                   ActivitySource       activitySource,
                   ILogger<TaskTable>   logger)
  {
    connection_     = connection;
    activitySource_ = activitySource;
    logger_         = logger;
  }

  /// <inheritdoc />
  public ILogger Logger => logger_;

  /// <inheritdoc />
  public TimeSpan PollingDelayMin { get; set; } = TimeSpan.FromSeconds(1);

  /// <inheritdoc />
  public TimeSpan PollingDelayMax { get; set; } = TimeSpan.FromMinutes(5);

  /// <inheritdoc />
  public ITaskTable Secondary => this; // No secondary/replica concept in TaskDB

  // ── IInitializable ──────────────────────────────────────────────────────

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => connection_.PingAsync(CancellationToken.None)
                  .ContinueWith(t => t.Result
                                       ? HealthCheckResult.Healthy("TaskDB reachable")
                                       : HealthCheckResult.Unhealthy("TaskDB unreachable"));

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
    => connection_.ConnectAsync(cancellationToken);

  // ── ITaskTable ───────────────────────────────────────────────────────────

  /// <inheritdoc />
  public async Task CreateTasks(IEnumerable<TaskData> tasks,
                                CancellationToken     cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    var req = new WireCreateTasksRequest
    {
      Tasks = tasks.Select(t => t.ToWire()).ToList(),
    };
    var (status, _) = await connection_.SendReceiveAsync(OpCode.CreateTasks, req, cancellationToken)
                                       .ConfigureAwait(false);

    if (status == StatusCode.AlreadyExists)
      throw new TaskAlreadyExistsException("One or more tasks already exist");

    if (status != StatusCode.Success)
      throw new ArmoniKException($"CreateTasks failed with status 0x{status:X2}");
  }

  /// <inheritdoc />
  public async Task<T> ReadTaskAsync<T>(string                        taskId,
                                        Expression<Func<TaskData, T>> selector,
                                        CancellationToken             cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    activity?.SetTag("ReadTaskId", taskId);

    var req = new WireReadTaskRequest { TaskId = taskId };
    var (status, payload) = await connection_.SendReceiveAsync(OpCode.ReadTask, req, cancellationToken)
                                             .ConfigureAwait(false);

    if (status == StatusCode.NotFound)
      throw new TaskNotFoundException($"Task '{taskId}' not found.");

    if (status != StatusCode.Success)
      throw new ArmoniKException($"ReadTask failed with status 0x{status:X2}");

    var wire = MessagePackSerializer.Deserialize<WireTaskData>(payload);
    var task = wire.ToDomain();
    return selector.Compile()(task);
  }

  /// <inheritdoc />
  public async Task<TaskData?> UpdateOneTask(string                                         taskId,
                                              Expression<Func<TaskData, bool>>?              filter,
                                              Core.Common.Storage.UpdateDefinition<TaskData> updates,
                                              bool                                           before            = false,
                                              CancellationToken                              cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();

    // Build a coarse filter from the compiled expression if provided.
    // The server evaluates equality-based conditions on known fields.
    var req = new WireUpdateOneTaskRequest
    {
      TaskId  = taskId,
      Filter  = filter is null ? null : BuildCoarseTaskFilter(filter),
      Updates = updates.ToWireUpdates(),
      Before  = before,
    };

    var (status, payload) = await connection_.SendReceiveAsync(OpCode.UpdateOneTask, req, cancellationToken)
                                             .ConfigureAwait(false);

    if (status == StatusCode.FilterMismatch)
      return null;

    if (status == StatusCode.NotFound)
      return null;

    if (status != StatusCode.Success)
      throw new ArmoniKException($"UpdateOneTask failed with status 0x{status:X2}");

    if (payload.Length == 0)
      return null;

    return MessagePackSerializer.Deserialize<WireTaskData>(payload).ToDomain();
  }

  /// <inheritdoc />
  public async Task<long> UpdateManyTasks(Expression<Func<TaskData, bool>>               filter,
                                           Core.Common.Storage.UpdateDefinition<TaskData> updates,
                                           CancellationToken                              cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();

    var req = new WireUpdateManyTasksRequest
    {
      Filter  = BuildCoarseTaskFilter(filter),
      Updates = updates.ToWireUpdates(),
    };

    var (status, payload) = await connection_.SendReceiveAsync(OpCode.UpdateManyTasks, req, cancellationToken)
                                             .ConfigureAwait(false);

    if (status != StatusCode.Success)
      throw new ArmoniKException($"UpdateManyTasks failed with status 0x{status:X2}");

    return MessagePackSerializer.Deserialize<long>(payload);
  }

  /// <inheritdoc />
  public async Task<long> BulkUpdateTasks(IEnumerable<(Expression<Func<TaskData, bool>> filter, Core.Common.Storage.UpdateDefinition<TaskData> updates)> bulkUpdates,
                                           CancellationToken                                                                                             cancellationToken)
  {
    using var activity = activitySource_.StartActivity();

    var req = new WireBulkUpdateTasksRequest
    {
      Items = bulkUpdates.Select(item => new WireBulkUpdateItem
                                        {
                                          Filter  = BuildCoarseTaskFilter(item.filter),
                                          Updates = item.updates.ToWireUpdates(),
                                        })
                         .ToList(),
    };

    var (status, payload) = await connection_.SendReceiveAsync(OpCode.BulkUpdateTasks, req, cancellationToken)
                                             .ConfigureAwait(false);

    if (status != StatusCode.Success)
      throw new ArmoniKException($"BulkUpdateTasks failed with status 0x{status:X2}");

    return MessagePackSerializer.Deserialize<long>(payload);
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<T> FindTasksAsync<T>(Expression<Func<TaskData, bool>> filter,
                                                      Expression<Func<TaskData, T>>    selector,
                                                      [EnumeratorCancellation]
                                                      CancellationToken                cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();

    var req = new WireFindTasksRequest { Filter = BuildCoarseTaskFilter(filter) };
    var compiledFilter   = filter.Compile();
    var compiledSelector = selector.Compile();

    await foreach (var frame in connection_.StreamAsync(OpCode.FindTasks, req, cancellationToken)
                                           .ConfigureAwait(false))
    {
      var wireTask = MessagePackSerializer.Deserialize<WireTaskData>(frame);
      var task     = wireTask.ToDomain();

      // Apply the full LINQ predicate in-memory for correctness (server sends a superset)
      if (compiledFilter(task))
        yield return compiledSelector(task);
    }
  }

  /// <inheritdoc />
  public async Task<(IEnumerable<T> tasks, long totalCount)> ListTasksAsync<T>(
    Expression<Func<TaskData, bool>>    filter,
    Expression<Func<TaskData, object?>> orderField,
    Expression<Func<TaskData, T>>       selector,
    bool                                ascOrder,
    int                                 page,
    int                                 pageSize,
    CancellationToken                   cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();

    var req = new WireListTasksRequest
    {
      Filter    = BuildCoarseTaskFilter(filter),
      OrderBy   = ExtractFieldName(orderField),
      Ascending = ascOrder,
      Page      = page,
      PageSize  = pageSize,
    };

    var (status, payload) = await connection_.SendReceiveAsync(OpCode.ListTasks, req, cancellationToken)
                                             .ConfigureAwait(false);

    if (status != StatusCode.Success)
      throw new ArmoniKException($"ListTasks failed with status 0x{status:X2}");

    var resp             = MessagePackSerializer.Deserialize<WireListTasksResponse>(payload);
    var compiledFilter   = filter.Compile();
    var compiledSelector = selector.Compile();

    var tasks = resp.Tasks
                    .Select(w => w.ToDomain())
                    .Where(compiledFilter)
                    .Select(compiledSelector);

    return (tasks, resp.TotalCount);
  }

  /// <inheritdoc />
  public async Task<IEnumerable<TaskStatusCount>> CountTasksAsync(Expression<Func<TaskData, bool>> filter,
                                                                   CancellationToken                cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();

    var req = new WireCountTasksRequest { Filter = BuildCoarseTaskFilter(filter) };
    var (status, payload) = await connection_.SendReceiveAsync(OpCode.CountTasks, req, cancellationToken)
                                             .ConfigureAwait(false);

    if (status != StatusCode.Success)
      throw new ArmoniKException($"CountTasks failed with status 0x{status:X2}");

    var resp = MessagePackSerializer.Deserialize<WireCountTasksResponse>(payload);
    return resp.Counts.Select(kv => new TaskStatusCount(((WireTaskStatus)kv.Key).ToDomain(), (int)kv.Value));
  }

   /// <inheritdoc />
  // ITaskTable.CountAllTasksAsync returns Task<int>
  public async Task<int> CountAllTasksAsync(TaskStatus        status,
                                             CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();

    var req = new WireCountTasksRequest
    {
      Filter = new WireCoarseFilter
      {
        StatusIn = new List<WireTaskStatus> { status.ToWire() },
      },
    };

    var (wireStatus, payload) = await connection_.SendReceiveAsync(OpCode.CountAllTasks, req, cancellationToken)
                                                 .ConfigureAwait(false);

    if (wireStatus != StatusCode.Success)
      throw new ArmoniKException($"CountAllTasks failed with status 0x{wireStatus:X2}");

    var resp = MessagePackSerializer.Deserialize<WireCountTasksResponse>(payload);
    return (int)resp.Counts.Values.Sum();
  }

  /// <inheritdoc />
  public async Task<IEnumerable<PartitionTaskStatusCount>> CountPartitionTasksAsync(CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();

    var (status, payload) = await connection_.SendReceiveAsync(OpCode.CountPartitionTasks, new { }, cancellationToken)
                                             .ConfigureAwait(false);

    if (status != StatusCode.Success)
      throw new ArmoniKException($"CountPartitionTasks failed with status 0x{status:X2}");

    // Server returns: List<{PartitionId, Status, Count}>
    var resp = MessagePackSerializer.Deserialize<List<WirePartitionTaskCount>>(payload);
    return resp.Select(r => new PartitionTaskStatusCount(r.PartitionId,
                                                          ((WireTaskStatus)r.Status).ToDomain(),
                                                          (int)r.Count));
  }

  /// <inheritdoc />
  public async Task DeleteTaskAsync(string            taskId,
                                     CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    activity?.SetTag("DeleteTaskId", taskId);

    var req = new WireDeleteTasksRequest { TaskIds = new List<string> { taskId } };
    var (status, _) = await connection_.SendReceiveAsync(OpCode.DeleteTasks, req, cancellationToken)
                                       .ConfigureAwait(false);

    if (status != StatusCode.Success)
      throw new ArmoniKException($"DeleteTask failed with status 0x{status:X2}");
  }

  /// <inheritdoc />
  public async Task DeleteTasksAsync(string            sessionId,
                                      CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    activity?.SetTag("DeleteTasksSessionId", sessionId);

    var req = new WireDeleteTasksRequest { SessionId = sessionId };
    var (status, _) = await connection_.SendReceiveAsync(OpCode.DeleteTasks, req, cancellationToken)
                                       .ConfigureAwait(false);

    if (status != StatusCode.Success)
      throw new ArmoniKException($"DeleteTasks(sessionId) failed with status 0x{status:X2}");
  }

  /// <inheritdoc />
  public async Task DeleteTasksAsync(ICollection<string> taskIds,
                                      CancellationToken   cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();

    var req = new WireDeleteTasksRequest { TaskIds = taskIds.ToList() };
    var (status, _) = await connection_.SendReceiveAsync(OpCode.DeleteTasks, req, cancellationToken)
                                       .ConfigureAwait(false);

    if (status != StatusCode.Success)
      throw new ArmoniKException($"DeleteTasks(ids) failed with status 0x{status:X2}");
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<T> RemoveRemainingDataDependenciesAsync<T>(
    ICollection<string>              taskIds,
    ICollection<string>              dependencyIds,
    Expression<Func<TaskData, T>>    selector,
    [EnumeratorCancellation]
    CancellationToken                cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();

    var req = new WireRemoveDepRequest
    {
      TaskIds       = taskIds.ToList(),
      DependencyIds = dependencyIds.ToList(),
    };

    var (status, payload) = await connection_.SendReceiveAsync(OpCode.RemoveRemainingDataDeps, req, cancellationToken)
                                             .ConfigureAwait(false);

    if (status != StatusCode.Success)
      throw new ArmoniKException($"RemoveRemainingDataDeps failed with status 0x{status:X2}");

    var resp             = MessagePackSerializer.Deserialize<WireRemoveDepResponse>(payload);
    var compiledSelector = selector.Compile();

    foreach (var wireTask in resp.ReadyTasks)
      yield return compiledSelector(wireTask.ToDomain());
  }

  /// <inheritdoc />
  public async Task<(IEnumerable<Application> applications, int totalCount)> ListApplicationsAsync(
    Expression<Func<TaskData, bool>>                  filter,
    ICollection<Expression<Func<Application, object?>>> orderFields,
    bool                                              ascOrder,
    int                                               page,
    int                                               pageSize,
    CancellationToken                                 cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();

    var req = new WireListApplicationsRequest
    {
      Filter    = BuildCoarseTaskFilter(filter),
      OrderBy   = orderFields.Count > 0 ? ExtractFieldName(orderFields.First()) : "",
      Ascending = ascOrder,
      Page      = page,
      PageSize  = pageSize,
    };

    var (status, payload) = await connection_.SendReceiveAsync(OpCode.ListApplications, req, cancellationToken)
                                             .ConfigureAwait(false);

    if (status != StatusCode.Success)
      throw new ArmoniKException($"ListApplications failed with status 0x{status:X2}");

    var resp = MessagePackSerializer.Deserialize<WireListApplicationsResponse>(payload);
    var apps = resp.Applications.Select(a => new Application(a.ApplicationName,
                                                              a.ApplicationNamespace,
                                                              a.ApplicationVersion,
                                                              a.ApplicationService));
    return (apps, (int)resp.TotalCount);
  }

  // ── Filter building ──────────────────────────────────────────────────────

  /// <summary>
  ///   Builds a coarse server-side filter from a LINQ expression.
  ///   Extracts equality conditions on well-known indexed fields.
  ///   The full filter is re-evaluated in-memory after results are returned.
  /// </summary>
  private static WireCoarseFilter BuildCoarseTaskFilter(Expression<Func<TaskData, bool>> filter)
  {
    var visitor = new CoarseTaskFilterVisitor();
    visitor.Visit(filter.Body);
    return visitor.Result;
  }

  private static string ExtractFieldName<T>(Expression<Func<T, object?>> expr)
  {
    var body = expr.Body;
    if (body is UnaryExpression u) body = u.Operand;
    return body is MemberExpression m ? m.Member.Name : "";
  }

  private static string ExtractFieldName<T, TRes>(Expression<Func<T, TRes>> expr)
  {
    var body = (Expression)expr.Body;
    if (body is UnaryExpression u) body = u.Operand;
    return body is MemberExpression m ? m.Member.Name : "";
  }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

[MessagePackObject]
internal class WirePartitionTaskCount
{
  [Key("PartitionId")] public string PartitionId { get; set; } = "";
  [Key("Status")]      public int    Status       { get; set; }
  [Key("Count")]       public long   Count        { get; set; }
}

/// <summary>
///   Walks a boolean expression tree and extracts simple equality conditions
///   for fields the TaskDB server indexes (SessionId, Status, PartitionId, TaskIds).
///   Any condition that cannot be represented as a coarse filter is simply ignored;
///   the adapter re-applies the full compiled predicate in-memory.
/// </summary>
internal class CoarseTaskFilterVisitor : ExpressionVisitor
{
  public WireCoarseFilter Result { get; } = new();

  protected override Expression VisitBinary(BinaryExpression node)
  {
    if (node.NodeType == ExpressionType.Equal)
    {
      TryExtractEquality(node.Left, node.Right);
      TryExtractEquality(node.Right, node.Left);
    }
    return base.VisitBinary(node);
  }

  private void TryExtractEquality(Expression member, Expression value)
  {
    if (member is not MemberExpression m) return;
    if (!TryGetConstant(value, out var constant)) return;

    switch (m.Member.Name)
    {
      case nameof(TaskData.SessionId) when constant is string s:
        Result.SessionId = s;
        break;
      case nameof(TaskData.Status) when constant is TaskStatus ts:
        Result.StatusIn ??= new List<WireTaskStatus>();
        Result.StatusIn.Add(ts.ToWire());
        break;
      case "PartitionId" when constant is string pid:
        Result.PartitionId = pid;
        break;
    }
  }

  private static bool TryGetConstant(Expression expr, out object? value)
  {
    // Handle direct constants
    if (expr is ConstantExpression c) { value = c.Value; return true; }
    // Handle member access on closure (captured variable)
    try
    {
      value = Expression.Lambda(expr).Compile().DynamicInvoke();
      return true;
    }
    catch
    {
      value = null;
      return false;
    }
  }
}
