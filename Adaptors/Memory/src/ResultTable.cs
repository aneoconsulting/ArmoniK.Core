// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Logging;

using KeyNotFoundException = System.Collections.Generic.KeyNotFoundException;

namespace ArmoniK.Core.Adapters.Memory;

public class ResultTable : IResultTable
{
  private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, Result>> results_;

  public ResultTable(ConcurrentDictionary<string, ConcurrentDictionary<string, Result>> results,
                     ILogger<ResultTable>                                               logger)
  {
    results_ = results;
    Logger   = logger;
  }

  /// <inheritdoc />
  public Task<bool> AreResultsAvailableAsync(string              sessionId,
                                             IEnumerable<string> keys,
                                             CancellationToken   cancellationToken = default)
    => Task.FromResult(keys.All(key => results_[sessionId][key]
                                  .Status == ResultStatus.Completed));

  /// <inheritdoc />
  public Task ChangeResultOwnership(string              sessionId,
                                    IEnumerable<string> keys,
                                    string              oldTaskId,
                                    string              newTaskId,
                                    CancellationToken   cancellationToken)
  {
    foreach (var result in results_[sessionId]
                           .Values.ToImmutableList()
                           .Where(result => result.OwnerTaskId == oldTaskId))
    {
      results_[result.SessionId]
        .TryUpdate(result.Name,
                   result with
                   {
                     OwnerTaskId = newTaskId,
                   },
                   result);
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

    if (!results_[session].ContainsKey(key))
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
  public Task<Result> GetResult(string            sessionId,
                                string            key,
                                CancellationToken cancellationToken = default)
  {
    try
    {
      return Task.FromResult(results_[sessionId][key]);
    }
    catch (KeyNotFoundException)
    {
      throw new ResultNotFoundException($"Key '{key}' not found");
    }
  }

  /// <inheritdoc />
  public IAsyncEnumerable<string> ListResultsAsync(string            sessionId,
                                                   CancellationToken cancellationToken = default)
    => results_.Values.SelectMany(results => results.Keys)
               .ToImmutableList()
               .ToAsyncEnumerable();

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
  public Task Init(CancellationToken cancellationToken)
    => Task.CompletedTask;

  /// <inheritdoc />
  public ILogger Logger { get; }

  /// <inheritdoc />
  public ValueTask<bool> Check(HealthCheckTag tag)
    => ValueTask.FromResult(true);
}
