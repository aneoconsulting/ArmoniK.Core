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
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Utils.Uuid;
using ArmoniK.Utils;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Memory;

/// <summary>
///   In-memory implementation of <see cref="ISessionTable" />.
/// </summary>
/// <remarks>
///   Every public method runs under a single lock, so that each call is atomic, as a single document operation is in a
///   database. Stored values are never modified in place: an update replaces the stored value with a new one.
/// </remarks>
public class SessionTable : ISessionTable
{
  private readonly Lock                            lock_     = new();
  private readonly Dictionary<string, SessionData> sessions_ = new();
  private readonly IUuidGenerator                  uuidGenerator_;

  private bool isInitialized_;

  /// <summary>
  ///   Creates an empty session table
  /// </summary>
  /// <param name="logger">Logger used to produce logs for this class</param>
  /// <param name="uuidGenerator">Generator for the session ids</param>
  public SessionTable(ILogger<SessionTable> logger,
                      IUuidGenerator        uuidGenerator)
  {
    Logger         = logger;
    uuidGenerator_ = uuidGenerator;
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
    var sessionId = uuidGenerator_.GenerateUuid()
                                  .ToString();
    var sessionData = new SessionData(sessionId,
                                      SessionStatus.Running,
                                      partitionIds.AsIList(),
                                      defaultOptions);

    lock (lock_)
    {
      sessions_.Add(sessionId,
                    sessionData);
    }

    return Task.FromResult(sessionId);
  }

  /// <inheritdoc />
  public IAsyncEnumerable<T> FindSessionsAsync<T>(Expression<Func<SessionData, bool>> filter,
                                                  Expression<Func<SessionData, T>>    selector,
                                                  CancellationToken                   cancellationToken = default)
    => Snapshot(filter)
       .Select(selector.Compile())
       .ToAsyncEnumerable();

  /// <inheritdoc />
  public Task DeleteSessionAsync(string            sessionId,
                                 CancellationToken cancellationToken = default)
  {
    lock (lock_)
    {
      sessions_.Remove(sessionId);
    }

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
    var matching = Snapshot(filter);
    var sessions = matching.OrderByKey(orderField,
                                       ascOrder)
                           .Skip(page * pageSize)
                           .Take(pageSize)
                           .ToList();

    return Task.FromResult<(IEnumerable<SessionData> sessions, long totalCount)>((sessions, matching.Count));
  }

  /// <inheritdoc />
  public Task<SessionData?> UpdateOneSessionAsync(string                               sessionId,
                                                  Expression<Func<SessionData, bool>>? filter,
                                                  UpdateDefinition<SessionData>        updates,
                                                  bool                                 before            = false,
                                                  CancellationToken                    cancellationToken = default)
  {
    var compiledFilter = filter?.Compile();

    lock (lock_)
    {
      if (!sessions_.TryGetValue(sessionId,
                                 out var current) || (compiledFilter is not null && !compiledFilter(current)))
      {
        return Task.FromResult<SessionData?>(null);
      }

      var updated = new SessionData(current,
                                    updates);
      sessions_[sessionId] = updated;

      return Task.FromResult<SessionData?>(before
                                             ? current
                                             : updated);
    }
  }

  /// <inheritdoc />
  public ILogger Logger { get; }

  /// <summary>
  ///   Copy the sessions matching the filter, under the lock
  /// </summary>
  /// <param name="filter">Filter on the sessions</param>
  /// <returns>
  ///   The matching sessions, at the time of the call
  /// </returns>
  private List<SessionData> Snapshot(Expression<Func<SessionData, bool>> filter)
  {
    var compiledFilter = filter.Compile();
    lock (lock_)
    {
      return sessions_.Values.Where(compiledFilter)
                      .ToList();
    }
  }
}
