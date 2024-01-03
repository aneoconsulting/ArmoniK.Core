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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace ArmoniK.Core.Adapters.Memory;

public class ResultTable : IResultTable
{
  private readonly ConcurrentDictionary<string, Result> results_ = new();

  private bool isInitialized_;

  /// <inheritdoc />
  public Task Create(ICollection<Result> results,
                     CancellationToken   cancellationToken = default)
  {
    foreach (var result in results)
    {
      if (!results_.TryAdd(result.ResultId,
                           result))
      {
        throw new ArmoniKException($"Key {result.ResultId} already exists");
      }
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task DeleteResults(string            sessionId,
                            CancellationToken cancellationToken = default)
  {
    var ids = results_.Values.Where(result => result.SessionId == sessionId)
                      .Select(result => result.ResultId);

    foreach (var id in ids)
    {
      results_.TryRemove(id,
                         out _);
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public IAsyncEnumerable<T> GetResults<T>(Expression<Func<Result, bool>> filter,
                                           Expression<Func<Result, T>>    convertor,
                                           CancellationToken              cancellationToken = default)
    => results_.Values.Where(filter.Compile())
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
    var queryable = results_.Values.AsQueryable()
                            .Where(filter);

    var ordered = ascOrder
                    ? queryable.OrderBy(orderField)
                    : queryable.OrderByDescending(orderField);

    return Task.FromResult<(IEnumerable<Result> results, int totalCount)>((ordered.Skip(page * pageSize)
                                                                                  .Take(pageSize), ordered.Count()));
  }

  public Task SetTaskOwnership(ICollection<(string resultId, string taskId)> requests,
                               CancellationToken                             cancellationToken = default)
  {
    foreach (var (resultId, taskId) in requests)
    {
      if (!results_.TryGetValue(resultId,
                                out var result))
      {
        throw new ResultNotFoundException($"Key '{resultId}' not found");
      }

      results_.TryUpdate(resultId,
                         result with
                         {
                           OwnerTaskId = taskId,
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
  public ILogger Logger { get; } = NullLogger.Instance;

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(isInitialized_
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy());

  /// <inheritdoc />
  public Task<long> UpdateManyResults(Expression<Func<Result, bool>>                                              filter,
                                      ICollection<(Expression<Func<Result, object?>> selector, object? newValue)> updates,
                                      CancellationToken                                                           cancellationToken = default)
  {
    long i = 0;
    foreach (var id in results_.Values.AsQueryable()
                               .Where(filter)
                               .Select(data => data.ResultId))
    {
      i++;
      results_.AddOrUpdate(id,
                           _ => throw new ResultNotFoundException("Result not found"),
                           (_,
                            data) => new Result(data,
                                                updates));
    }

    return Task.FromResult(i);
  }

  /// <inheritdoc />
  public Task<Result> UpdateOneResult(string                                                                      resultId,
                                      ICollection<(Expression<Func<Result, object?>> selector, object? newValue)> updates,
                                      CancellationToken                                                           cancellationToken = default)
  {
    if (!results_.TryGetValue(resultId,
                              out var result))
    {
      throw new ResultNotFoundException($"Result '{resultId}' not found");
    }

    results_[resultId] = new Result(result,
                                    updates);
    return Task.FromResult(result);
  }

  /// <inheritdoc />
  public Task ChangeResultOwnership(string                                                 oldTaskId,
                                    IEnumerable<IResultTable.ChangeResultOwnershipRequest> requests,
                                    CancellationToken                                      cancellationToken)
  {
    foreach (var request in requests)
    {
      foreach (var result in results_.Values.ToImmutableList()
                                     .Where(result => result.OwnerTaskId == oldTaskId))
      {
        results_.TryUpdate(result.ResultId,
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
  public Task AddTaskDependencies(IDictionary<string, ICollection<string>> dependencies,
                                  CancellationToken                        cancellationToken = default)
  {
    foreach (var (resultId, taskIds) in dependencies)
    {
      if (!results_.TryGetValue(resultId,
                                out var result))
      {
        throw new ResultNotFoundException($"Key '{resultId}' not found");
      }

      result.DependentTasks.AddRange(taskIds);
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task DeleteResult(string            key,
                           CancellationToken cancellationToken = default)
  {
    if (!results_.ContainsKey(key))
    {
      throw new ResultNotFoundException($"Key '{key}' not found");
    }

    return Task.FromResult(results_.Remove(key,
                                           out _));
  }
}
