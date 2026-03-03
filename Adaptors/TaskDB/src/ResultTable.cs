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

/// <inheritdoc cref="IResultTable" />
public class ResultTable : IResultTable
{
  private readonly TaskDbConnection     connection_;
  private readonly ActivitySource       activitySource_;
  private readonly ILogger<ResultTable> logger_;

  public ResultTable(TaskDbConnection     connection,
                     ActivitySource       activitySource,
                     ILogger<ResultTable> logger)
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
    => Task.CompletedTask; // Shared connection; init happens in TaskTable.Init

  // ── IResultTable ─────────────────────────────────────────────────────────

  /// <inheritdoc />
  public async Task Create(ICollection<Result> results,
                            CancellationToken   cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();

    var req = new WireCreateResultsRequest
    {
      Results = results.Select(r => r.ToWire()).ToList(),
    };

    var (status, _) = await connection_.SendReceiveAsync(OpCode.CreateResults, req, cancellationToken)
                                       .ConfigureAwait(false);

    if (status == StatusCode.AlreadyExists)
      throw new ArmoniKException("One or more results already exist");

    if (status != StatusCode.Success)
      throw new ArmoniKException($"CreateResults failed with status 0x{status:X2}");
  }

  /// <inheritdoc />
  // IResultTable.GetResults: filter + selector, no sessionId
  public IAsyncEnumerable<T> GetResults<T>(Expression<Func<Result, bool>> filter,
                                            Expression<Func<Result, T>>    convertor,
                                            CancellationToken              cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    return GetResultsInternalAsync(filter, convertor, cancellationToken);
  }

  private async IAsyncEnumerable<T> GetResultsInternalAsync<T>(
    Expression<Func<Result, bool>> filter,
    Expression<Func<Result, T>>    convertor,
    [EnumeratorCancellation]
    CancellationToken              cancellationToken)
  {
    var req              = new WireGetResultsRequest { Filter = new WireCoarseResultFilter() };
    var compiledFilter   = filter.Compile();
    var compiledConvertor = convertor.Compile();

    await foreach (var frame in connection_.StreamAsync(OpCode.GetResults, req, cancellationToken)
                                           .ConfigureAwait(false))
    {
      var result = MessagePackSerializer.Deserialize<WireResultData>(frame).ToDomain();
      if (compiledFilter(result))
        yield return compiledConvertor(result);
    }
  }

  /// <inheritdoc />
  public async Task<(IEnumerable<Result> results, int totalCount)> ListResultsAsync(
    Expression<Func<Result, bool>>    filter,
    Expression<Func<Result, object?>> orderField,
    bool                              ascOrder,
    int                               page,
    int                               pageSize,
    CancellationToken                 cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();

    var req = new WireGetResultsRequest { Filter = new WireCoarseResultFilter() };

    var all            = new List<Result>();
    var compiledFilter = filter.Compile();
    var compiledOrder  = orderField.Compile();

    await foreach (var frame in connection_.StreamAsync(OpCode.GetResults, req, cancellationToken)
                                           .ConfigureAwait(false))
    {
      var r = MessagePackSerializer.Deserialize<WireResultData>(frame).ToDomain();
      if (compiledFilter(r))
        all.Add(r);
    }

    var ordered = ascOrder
                    ? all.OrderBy(compiledOrder)
                    : all.OrderByDescending(compiledOrder);

    var pageResults = pageSize > 0
                        ? ordered.Skip(page * pageSize).Take(pageSize)
                        : ordered;

    return (pageResults, all.Count);
  }

  /// <inheritdoc />
  public async Task SetTaskOwnership(ICollection<(string resultId, string taskId)> requests,
                                      CancellationToken                             cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();

    var req = new WireSetTaskOwnershipRequest
    {
      Assignments = requests.Select(r => new WireOwnershipAssignment
                                         {
                                           ResultId = r.resultId,
                                           TaskId   = r.taskId,
                                         })
                            .ToList(),
    };

    var (status, _) = await connection_.SendReceiveAsync(OpCode.SetTaskOwnership, req, cancellationToken)
                                       .ConfigureAwait(false);

    if (status != StatusCode.Success)
      throw new ArmoniKException($"SetTaskOwnership failed with status 0x{status:X2}");
  }

  /// <inheritdoc />
  public async Task AddTaskDependencies(IDictionary<string, ICollection<string>> dependencies,
                                         CancellationToken                        cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();

    var req = new WireAddTaskDependenciesRequest
    {
      Dependencies = dependencies.ToDictionary(kv => kv.Key, kv => kv.Value.ToList()),
    };

    var (status, _) = await connection_.SendReceiveAsync(OpCode.AddTaskDependencies, req, cancellationToken)
                                       .ConfigureAwait(false);

    if (status != StatusCode.Success)
      throw new ArmoniKException($"AddTaskDependencies failed with status 0x{status:X2}");
  }

  /// <inheritdoc />
  // IResultTable.ChangeResultOwnership: (oldTaskId, requests) — no sessionId
  public async Task ChangeResultOwnership(string                                                 oldTaskId,
                                           IEnumerable<IResultTable.ChangeResultOwnershipRequest> requests,
                                           CancellationToken                                      cancellationToken)
  {
    using var activity = activitySource_.StartActivity();

    var req = new WireChangeResultOwnershipRequest
    {
      OldTaskId = oldTaskId,
      Transfers = requests.Select(r => new WireOwnershipTransfer
                                       {
                                         ResultIds = r.Keys.ToList(),
                                         NewTaskId = r.NewTaskId,
                                       })
                          .ToList(),
    };

    var (status, _) = await connection_.SendReceiveAsync(OpCode.ChangeResultOwnership, req, cancellationToken)
                                       .ConfigureAwait(false);

    if (status != StatusCode.Success)
      throw new ArmoniKException($"ChangeResultOwnership failed with status 0x{status:X2}");
  }

  /// <inheritdoc />
  public async Task DeleteResult(string            key,
                                  CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    activity?.SetTag("ResultId", key);

    var req = new WireDeleteResultsRequest { ResultIds = new List<string> { key } };
    var (status, _) = await connection_.SendReceiveAsync(OpCode.DeleteResults, req, cancellationToken)
                                       .ConfigureAwait(false);

    if (status == StatusCode.NotFound)
      throw new ResultNotFoundException($"Result '{key}' not found.");

    if (status != StatusCode.Success)
      throw new ArmoniKException($"DeleteResult failed with status 0x{status:X2}");
  }

  /// <inheritdoc />
  public async Task DeleteResults(string            sessionId,
                                   CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();

    var req = new WireDeleteResultsRequest { SessionId = sessionId };
    var (status, _) = await connection_.SendReceiveAsync(OpCode.DeleteResults, req, cancellationToken)
                                       .ConfigureAwait(false);

    if (status != StatusCode.Success)
      throw new ArmoniKException($"DeleteResults(sessionId) failed with status 0x{status:X2}");
  }

  /// <inheritdoc />
  public async Task DeleteResults(ICollection<string> results,
                                   CancellationToken   cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();

    var req = new WireDeleteResultsRequest { ResultIds = results.ToList() };
    var (status, _) = await connection_.SendReceiveAsync(OpCode.DeleteResults, req, cancellationToken)
                                       .ConfigureAwait(false);

    if (status != StatusCode.Success)
      throw new ArmoniKException($"DeleteResults(ids) failed with status 0x{status:X2}");
  }

  /// <inheritdoc />
  public async Task<Result> UpdateOneResult(string                              resultId,
                                             Core.Common.Storage.UpdateDefinition<Result> updates,
                                             CancellationToken                   cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();

    var req = new WireUpdateOneResultRequest
    {
      ResultId = resultId,
      Updates  = updates.ToWireUpdates(),
    };

    var (status, payload) = await connection_.SendReceiveAsync(OpCode.UpdateOneResult, req, cancellationToken)
                                             .ConfigureAwait(false);

    if (status == StatusCode.NotFound)
      throw new ResultNotFoundException($"Result '{resultId}' not found.");

    if (status != StatusCode.Success)
      throw new ArmoniKException($"UpdateOneResult failed with status 0x{status:X2}");

    return MessagePackSerializer.Deserialize<WireResultData>(payload).ToDomain();
  }

  /// <inheritdoc />
  public async Task<long> UpdateManyResults(Expression<Func<Result, bool>>               filter,
                                             Core.Common.Storage.UpdateDefinition<Result> updates,
                                             CancellationToken                            cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();

    var req = new WireUpdateManyResultsRequest
    {
      Filter  = new WireCoarseResultFilter(),
      Updates = updates.ToWireUpdates(),
    };

    var (status, payload) = await connection_.SendReceiveAsync(OpCode.UpdateManyResults, req, cancellationToken)
                                             .ConfigureAwait(false);

    if (status != StatusCode.Success)
      throw new ArmoniKException($"UpdateManyResults failed with status 0x{status:X2}");

    return MessagePackSerializer.Deserialize<long>(payload);
  }

  /// <inheritdoc />
  public async Task AbortTaskResults(string            sessionId,
                                      string            ownerTaskId,
                                      CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();

    var req = new WireAbortTaskResultsRequest
    {
      SessionId   = sessionId,
      OwnerTaskId = ownerTaskId,
    };

    var (status, _) = await connection_.SendReceiveAsync(OpCode.AbortTaskResults, req, cancellationToken)
                                       .ConfigureAwait(false);

    if (status != StatusCode.Success)
      throw new ArmoniKException($"AbortTaskResults failed with status 0x{status:X2}");
  }
}
