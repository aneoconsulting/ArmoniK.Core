// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Utils;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Memory;

public class SessionTable : ISessionTable
{
  private readonly ConcurrentDictionary<string, SessionData> storage_;

  private bool isInitialized_;

  public SessionTable(ConcurrentDictionary<string, SessionData> storage,
                      ILogger<SessionTable>                     logger)
  {
    storage_ = storage;
    Logger   = logger;
  }

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
  {
    isInitialized_ = true;
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(isInitialized_
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy());

  /// <inheritdoc />
  public Task<string> SetSessionDataAsync(IEnumerable<string> partitionIds,
                                          TaskOptions         defaultOptions,
                                          CancellationToken   cancellationToken = default)
  {
    var rootSessionId = Guid.NewGuid()
                            .ToString();

    storage_.TryAdd(rootSessionId,
                    new SessionData(rootSessionId,
                                    SessionStatus.Running,
                                    partitionIds.AsIList(),
                                    defaultOptions));
    return Task.FromResult(rootSessionId);
  }

  /// <inheritdoc />
  public IAsyncEnumerable<T> FindSessionsAsync<T>(Expression<Func<SessionData, bool>> filter,
                                                  Expression<Func<SessionData, T>>    selector,
                                                  CancellationToken                   cancellationToken = default)
    => storage_.Select(pair => pair.Value)
               .Where(filter.Compile())
               .Select(selector.Compile())
               .ToAsyncEnumerable();

  /// <inheritdoc />
  public Task DeleteSessionAsync(string            sessionId,
                                 CancellationToken cancellationToken = default)
  {
    storage_.TryRemove(sessionId,
                       out _);
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task<(IEnumerable<SessionData> sessions, long totalCount)> ListSessionsAsync(Expression<Func<SessionData, bool>>    filter,
                                                                                      Expression<Func<SessionData, object?>> orderField,
                                                                                      bool                                   ascOrder,
                                                                                      int                                    page,
                                                                                      int                                    pageSize,
                                                                                      CancellationToken                      cancellationToken = default)
  {
    var queryable = storage_.AsQueryable()
                            .Select(pair => pair.Value)
                            .Where(filter);

    var ordered = ascOrder
                    ? queryable.OrderBy(orderField)
                    : queryable.OrderByDescending(orderField);

    return Task.FromResult<(IEnumerable<SessionData> sessions, long totalCount)>((ordered.Skip(page * pageSize)
                                                                                         .Take(pageSize), ordered.Count()));
  }

  /// <inheritdoc />
  public Task<SessionData?> UpdateOneSessionAsync(string                               sessionId,
                                                  Expression<Func<SessionData, bool>>? filter,
                                                  UpdateDefinition<SessionData>        updates,
                                                  bool                                 before            = false,
                                                  CancellationToken                    cancellationToken = default)
  {
    if (!storage_.TryGetValue(sessionId,
                              out var sessionData))
    {
      return Task.FromResult<SessionData?>(null);
    }

    if (filter is not null)
    {
      if (!filter.Compile()
                 .Invoke(sessionData))
      {
        return Task.FromResult<SessionData?>(null);
      }
    }

    var newSessionData = storage_[sessionId] = new SessionData(sessionData,
                                                               updates);
    return Task.FromResult<SessionData?>(before
                                           ? sessionData
                                           : newSessionData);
  }

  /// <inheritdoc />
  public ILogger Logger { get; }
}
