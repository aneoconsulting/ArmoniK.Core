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
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Common;
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
  public Task<IEnumerable<ResultStatusCount>> AreResultsAvailableAsync(string              sessionId,
                                                                       IEnumerable<string> keys,
                                                                       CancellationToken   cancellationToken = default)
    => Task.FromResult(results_[sessionId]
                       .Where(model => keys.Contains(model.Value.Name))
                       .GroupBy(model => model.Value.Status)
                       .Select(models => new ResultStatusCount(models.Key,
                                                               models.Count())));

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
          .TryUpdate(result.Name,
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
      if (!sessionResults.TryAdd(result.Name,
                                 result))
      {
        throw new ArmoniKException($"Key {result.Name} already exists");
      }
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

  public IAsyncEnumerable<Result> GetResults(string              sessionId,
                                             IEnumerable<string> keys,
                                             CancellationToken   cancellationToken = default)
    => results_[sessionId]
       .Values.Where(r => keys.Contains(r.Name))
       .ToAsyncEnumerable();

  /// <inheritdoc />
  public IAsyncEnumerable<string> ListResultsAsync(string            sessionId,
                                                   CancellationToken cancellationToken = default)
    => results_.Values.SelectMany(results => results.Keys)
               .ToImmutableList()
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
      .TryUpdate(result.Name,
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
      .TryUpdate(result.Name,
                 result with
                 {
                   Status = ResultStatus.Completed,
                 },
                 result);
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task<IEnumerable<GetResultStatusReply.Types.IdStatus>> GetResultStatus(IEnumerable<string> ids,
                                                                                string              sessionId,
                                                                                CancellationToken   cancellationToken = default)
    => Task.FromResult(results_[sessionId]
                       .Where(model => ids.Contains(model.Key))
                       .Select(model => new GetResultStatusReply.Types.IdStatus
                                        {
                                          ResultId = model.Value.Name,
                                          Status   = model.Value.Status,
                                        }));

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
        .TryUpdate(result.Name,
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
