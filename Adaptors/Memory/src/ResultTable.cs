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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Base.Exceptions;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Memory;

/// <summary>
///   In-memory implementation of <see cref="IResultTable" />.
/// </summary>
/// <remarks>
///   Every public method runs under a single lock, so that each call is atomic, as a single document operation is in a
///   database. Stored values are never modified in place: an update replaces the stored value with a new one, so values
///   returned to callers are not affected by later writes.
/// </remarks>
public class ResultTable : IResultTable
{
  private readonly Lock                       lock_    = new();
  private readonly Dictionary<string, Result> results_ = new();

  private bool isInitialized_;

  /// <summary>
  ///   Creates an empty result table
  /// </summary>
  /// <param name="logger">Logger used to produce logs for this class</param>
  public ResultTable(ILogger<ResultTable> logger)
    => Logger = logger;

  /// <inheritdoc />
  public Task Create(ICollection<Result> results,
                     CancellationToken   cancellationToken = default)
  {
    var newResults = results.Select(Copy)
                            .ToList();

    lock (lock_)
    {
      // Validate the whole batch before inserting, so that a duplicate does not leave part of the batch inserted.
      var ids = new HashSet<string>();
      foreach (var result in newResults)
      {
        if (results_.ContainsKey(result.ResultId) || !ids.Add(result.ResultId))
        {
          throw new ArmoniKException($"Key {result.ResultId} already exists");
        }
      }

      foreach (var result in newResults)
      {
        results_.Add(result.ResultId,
                     result);
      }
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task DeleteResults(string            sessionId,
                            CancellationToken cancellationToken = default)
  {
    lock (lock_)
    {
      var ids = results_.Values.Where(result => result.SessionId == sessionId)
                        .Select(result => result.ResultId)
                        .ToList();

      foreach (var id in ids)
      {
        results_.Remove(id);
      }
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task DeleteResults(ICollection<string> results,
                            CancellationToken   cancellationToken = default)
  {
    lock (lock_)
    {
      foreach (var id in results)
      {
        results_.Remove(id);
      }
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public IAsyncEnumerable<T> GetResults<T>(Expression<Func<Result, bool>> filter,
                                           Expression<Func<Result, T>>    convertor,
                                           CancellationToken              cancellationToken = default)
    => Snapshot(filter)
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
    var matching = Snapshot(filter);
    var results = matching.OrderByKey(orderField,
                                      ascOrder)
                          .Skip(page * pageSize)
                          .Take(pageSize)
                          .Select(result => result with
                                            {
                                              DependentTasks = [],
                                            })
                          .ToList();

    return Task.FromResult<(IEnumerable<Result> results, int totalCount)>((results, matching.Count));
  }

  /// <inheritdoc />
  public Task SetTaskOwnership(ICollection<(string resultId, string taskId)> requests,
                               CancellationToken                             cancellationToken = default)
  {
    lock (lock_)
    {
      // Validate all the requests before applying them, so that a missing result does not leave part of them applied.
      foreach (var (resultId, _) in requests)
      {
        if (!results_.ContainsKey(resultId))
        {
          throw new ResultNotFoundException($"Key '{resultId}' not found");
        }
      }

      foreach (var (resultId, taskId) in requests)
      {
        results_[resultId] = results_[resultId] with
                             {
                               OwnerTaskId = taskId,
                             };
      }
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

  /// <inheritdoc />
  public Task<long> UpdateManyResults(Expression<Func<Result, bool>> filter,
                                      UpdateDefinition<Result>       updates,
                                      CancellationToken              cancellationToken = default)
  {
    var compiledFilter = filter.Compile();

    lock (lock_)
    {
      // The filter is evaluated and the updates are applied under the same lock,
      // so that a result that no longer matches the filter is not updated.
      var matching = results_.Values.Where(compiledFilter)
                             .ToList();

      foreach (var current in matching)
      {
        results_[current.ResultId] = Copy(new Result(current,
                                                     updates));
      }

      return Task.FromResult<long>(matching.Count);
    }
  }

  /// <inheritdoc />
  public Task<Result> UpdateOneResult(string                   resultId,
                                      UpdateDefinition<Result> updates,
                                      CancellationToken        cancellationToken = default)
  {
    Result oldResult;

    lock (lock_)
    {
      if (!results_.TryGetValue(resultId,
                                out var current))
      {
        throw new ResultNotFoundException($"Result '{resultId}' not found");
      }

      oldResult = current;
      results_[resultId] = Copy(new Result(current,
                                           updates));
    }

    return Task.FromResult(oldResult with
                           {
                             DependentTasks = [],
                           });
  }

  /// <inheritdoc />
  public Task ChangeResultOwnership(string                                                 oldTaskId,
                                    IEnumerable<IResultTable.ChangeResultOwnershipRequest> requests,
                                    CancellationToken                                      cancellationToken)
  {
    var requestList = requests.ToList();

    lock (lock_)
    {
      foreach (var request in requestList)
      {
        foreach (var key in request.Keys)
        {
          // Only the results still owned by the previous owner are transferred.
          if (results_.TryGetValue(key,
                                   out var current) && current.OwnerTaskId == oldTaskId)
          {
            results_[key] = current with
                            {
                              OwnerTaskId = request.NewTaskId,
                            };
          }
        }
      }
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task AddTaskDependencies(IDictionary<string, ICollection<string>> dependencies,
                                  CancellationToken                        cancellationToken = default)
  {
    lock (lock_)
    {
      // Validate all the results before updating them, so that a missing result does not leave part of them updated.
      foreach (var resultId in dependencies.Keys)
      {
        if (!results_.ContainsKey(resultId))
        {
          throw new ResultNotFoundException($"Key '{resultId}' not found");
        }
      }

      foreach (var (resultId, taskIds) in dependencies)
      {
        var current = results_[resultId];
        results_[resultId] = current with
                             {
                               DependentTasks = current.DependentTasks.Union(taskIds)
                                                       .ToList(),
                             };
      }
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task DeleteResult(string            key,
                           CancellationToken cancellationToken = default)
  {
    lock (lock_)
    {
      if (!results_.Remove(key))
      {
        throw new ResultNotFoundException($"Key '{key}' not found");
      }
    }

    return Task.CompletedTask;
  }

  /// <summary>
  ///   Copy the results matching the filter, under the lock
  /// </summary>
  /// <param name="filter">Filter on the results</param>
  /// <returns>
  ///   The matching results, at the time of the call
  /// </returns>
  private List<Result> Snapshot(Expression<Func<Result, bool>> filter)
  {
    var compiledFilter = filter.Compile();
    lock (lock_)
    {
      return results_.Values.Where(compiledFilter)
                     .ToList();
    }
  }

  /// <summary>
  ///   Copy the mutable members of a result, so that the stored value does not share them with the caller
  /// </summary>
  /// <param name="result">Result to copy</param>
  /// <returns>
  ///   The copy of the result
  /// </returns>
  private static Result Copy(Result result)
    => result with
       {
         DependentTasks = new List<string>(result.DependentTasks),
       };
}
