// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Memory;

public class ResultTable : IResultTable
{
  private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, Result>> results_;

  private bool isInitialized_;

  public ResultTable(ConcurrentDictionary<string, ConcurrentDictionary<string, Result>> results,
                     ILogger<ResultTable>                                               logger)
  {
    results_ = results;
    Logger   = logger;
  }

  /// <inheritdoc />
  public Task ChangeResultOwnership(string                                                 sessionId,
                                    string                                                 oldTaskId,
                                    IEnumerable<IResultTable.ChangeResultOwnershipRequest> requests,
                                    CancellationToken                                      cancellationToken)
  {
    foreach (var request in requests)
    {
      foreach (var result in results_[sessionId]
                             .Values.ToImmutableList()
                             .Where(result => result.OwnerTaskId == oldTaskId))
      {
        results_[result.SessionId]
          .TryUpdate(result.ResultId,
                     result with
                     {
                       OwnerTaskId = request.NewTaskId,
                     },
                     result);
      }
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task Create(IEnumerable<Result> results,
                     CancellationToken   cancellationToken = default)
  {
    foreach (var result in results)
    {
      var sessionResults = results_.GetOrAdd(result.SessionId,
                                             new ConcurrentDictionary<string, Result>());
      if (!sessionResults.TryAdd(result.ResultId,
                                 result))
      {
        throw new ArmoniKException($"Key {result.ResultId} already exists");
      }
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task AddTaskDependency(string              sessionId,
                                ICollection<string> resultIds,
                                ICollection<string> taskIds,
                                CancellationToken   cancellationToken)
  {
    if (!results_.TryGetValue(sessionId,
                              out var session))
    {
      throw new SessionNotFoundException($"Session '{session}' not found");
    }

    foreach (var resultId in resultIds)
    {
      if (!session.TryGetValue(resultId,
                               out var result))
      {
        throw new ResultNotFoundException($"Key '{resultId}' not found");
      }

      result.DependentTasks.AddRange(taskIds);
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task DeleteResult(string            session,
                           string            key,
                           CancellationToken cancellationToken = default)
  {
    if (!results_.ContainsKey(session))
    {
      throw new SessionNotFoundException($"Session '{session}' not found");
    }

    if (!results_[session]
          .ContainsKey(key))
    {
      throw new ResultNotFoundException($"Key '{key}' not found");
    }

    return Task.FromResult(results_[session]
                             .Remove(key,
                                     out _));
  }

  /// <inheritdoc />
  public Task DeleteResults(string            sessionId,
                            CancellationToken cancellationToken = default)
  {
    if (!results_.ContainsKey(sessionId))
    {
      throw new SessionNotFoundException($"Session '{sessionId}' not found");
    }

    results_[sessionId]
      .Clear();
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public IAsyncEnumerable<T> GetResults<T>(Expression<Func<Result, bool>> filter,
                                           Expression<Func<Result, T>>    convertor,
                                           CancellationToken              cancellationToken = default)
    => results_.Values.SelectMany(results => results.Values)
               .Where(filter.Compile())
               .Select(convertor.Compile())
               .ToAsyncEnumerable();

  /// <inheritdoc />
  public Task<(IEnumerable<Result> results, int totalCount)> ListResultsAsync(Expression<Func<Result, bool>>    filter,
                                                                              Expression<Func<Result, object?>> orderField,
                                                                              bool                              ascOrder,
                                                                              int                               page,
                                                                              int                               pageSize,
                                                                              CancellationToken                 cancellationToken = default)
  {
    var queryable = results_.Values.SelectMany(results => results.Values)
                            .AsQueryable()
                            .Where(filter);

    var ordered = ascOrder
                    ? queryable.OrderBy(orderField)
                    : queryable.OrderByDescending(orderField);

    return Task.FromResult<(IEnumerable<Result> results, int totalCount)>((ordered.Skip(page * pageSize)
                                                                                  .Take(pageSize), ordered.Count()));
  }

  /// <inheritdoc />
  public Task SetResult(string            sessionId,
                        string            ownerTaskId,
                        string            key,
                        byte[]            smallPayload,
                        CancellationToken cancellationToken = default)
  {
    var result = results_[sessionId][key];

    results_[result.SessionId]
      .TryUpdate(result.ResultId,
                 result with
                 {
                   Data = smallPayload,
                   Status = ResultStatus.Completed,
                 },
                 result);
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task SetResult(string            sessionId,
                        string            ownerTaskId,
                        string            key,
                        CancellationToken cancellationToken = default)
  {
    var result = results_[sessionId][key];

    results_[result.SessionId]
      .TryUpdate(result.ResultId,
                 result with
                 {
                   Status = ResultStatus.Completed,
                 },
                 result);
    return Task.CompletedTask;
  }

  public Task<Result> CompleteResult(string            sessionId,
                                     string            resultId,
                                     CancellationToken cancellationToken = default)
  {
    try
    {
      var result = results_[sessionId][resultId];
      var newResult = result with
                      {
                        Status = ResultStatus.Completed,
                      };

      results_[result.SessionId]
        .TryUpdate(result.ResultId,
                   newResult,
                   result);

      return Task.FromResult(newResult);
    }
    catch (KeyNotFoundException e)
    {
      throw new ResultNotFoundException($"Result {resultId} not found",
                                        e);
    }
  }

  public Task SetTaskOwnership(string                                        sessionId,
                               ICollection<(string resultId, string taskId)> requests,
                               CancellationToken                             cancellationToken = default)
  {
    if (!results_.TryGetValue(sessionId,
                              out var session))
    {
      throw new SessionNotFoundException($"Session '{session}' not found");
    }

    foreach (var (resultId, taskId) in requests)
    {
      if (!session.TryGetValue(resultId,
                               out var result))
      {
        throw new ResultNotFoundException($"Key '{resultId}' not found");
      }

      session.TryUpdate(resultId,
                        result with
                        {
                          OwnerTaskId = taskId,
                        },
                        result);
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task AbortTaskResults(string            sessionId,
                               string            ownerTaskId,
                               CancellationToken cancellationToken = default)
  {
    foreach (var result in results_[sessionId]
                           .Values.ToImmutableList()
                           .Where(result => result.OwnerTaskId == ownerTaskId))
    {
      results_[result.SessionId]
        .TryUpdate(result.ResultId,
                   result with
                   {
                     Status = ResultStatus.Aborted,
                   },
                   result);
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
  {
    isInitialized_ = true;
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public ILogger Logger { get; }

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(isInitialized_
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy());
}
