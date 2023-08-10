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
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Utils;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using TaskOptions = ArmoniK.Core.Base.DataStructures.TaskOptions;

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
  public Task<SessionData> GetSessionAsync(string            sessionId,
                                           CancellationToken cancellationToken = default)
  {
    if (!storage_.ContainsKey(sessionId))
    {
      throw new SessionNotFoundException($"Key '{sessionId}' not found");
    }

    return Task.FromResult(storage_[sessionId]);
  }

  /// <inheritdoc />
  public Task<bool> IsSessionCancelledAsync(string            sessionId,
                                            CancellationToken cancellationToken = default)
    => Task.FromResult(GetSessionAsync(sessionId,
                                       cancellationToken)
                       .Result.Status == SessionStatus.Cancelled);

  /// <inheritdoc />
  public Task<TaskOptions> GetDefaultTaskOptionAsync(string            sessionId,
                                                     CancellationToken cancellationToken = default)
  {
    if (!storage_.ContainsKey(sessionId))
    {
      throw new SessionNotFoundException($"Key '{sessionId}' not found");
    }

    return Task.FromResult(storage_[sessionId].Options);
  }

  /// <inheritdoc />
  public Task<SessionData> CancelSessionAsync(string            sessionId,
                                              CancellationToken cancellationToken = default)
    => Task.FromResult(storage_.AddOrUpdate(sessionId,
                                            _ => throw new SessionNotFoundException($"Key '{sessionId}' not found"),
                                            (_,
                                             data) =>
                                            {
                                              if (data.Status == SessionStatus.Cancelled)
                                              {
                                                throw new SessionNotFoundException($"No open session with key '{sessionId}' was found");
                                              }

                                              return data with
                                                     {
                                                       Status = SessionStatus.Cancelled,
                                                       CancellationDate = DateTime.UtcNow,
                                                     };
                                            }));


  /// <inheritdoc />
  public Task DeleteSessionAsync(string            sessionId,
                                 CancellationToken cancellationToken = default)
  {
    if (!storage_.ContainsKey(sessionId))
    {
      throw new SessionNotFoundException($"No session with id '{sessionId}' found");
    }

    storage_.Remove(sessionId,
                    out _);
    return Task.CompletedTask;
  }


  /// <inheritdoc />
  public IAsyncEnumerable<string> ListSessionsAsync(SessionFilter     sessionFilter,
                                                    CancellationToken cancellationToken = default)
  {
    var rawList = storage_.Keys.ToAsyncEnumerable();

    if (sessionFilter.Sessions.Any())
    {
      rawList = storage_.Keys.Intersect(sessionFilter.Sessions)
                        .ToAsyncEnumerable();
    }

    return rawList.Where(sessionId => sessionFilter.StatusesCase switch
                                      {
                                        SessionFilter.StatusesOneofCase.None     => true,
                                        SessionFilter.StatusesOneofCase.Included => sessionFilter.Included.Statuses.Contains(storage_[sessionId].Status),
                                        SessionFilter.StatusesOneofCase.Excluded => !sessionFilter.Excluded.Statuses.Contains(storage_[sessionId].Status),
                                        _                                        => throw new ArgumentException("Filter is set to an unknown StatusesCase."),
                                      });
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
  public ILogger Logger { get; }
}
