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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Sessions;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;

using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

using TaskOptions = ArmoniK.Core.Common.Storage.TaskOptions;

namespace ArmoniK.Core.Adapters.Memory;

public class SessionTable : ISessionTable
{
  private readonly ConcurrentDictionary<string, SessionData> storage_;

  public SessionTable(ConcurrentDictionary<string, SessionData> storage,
                      ILogger<SessionTable>                     logger)
  {
    storage_ = storage;
    Logger   = logger;
  }

  /// <inheritdoc />
  public ValueTask<bool> Check(HealthCheckTag tag)
    => ValueTask.FromResult(true);

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
    => Task.CompletedTask;

  /// <inheritdoc />
  public Task SetSessionDataAsync(string              rootSessionId,
                                  IEnumerable<string> partitionIds,
                                  TaskOptions         defaultOptions,
                                  CancellationToken   cancellationToken = default)
  {
    storage_.TryAdd(rootSessionId,
                    new SessionData(rootSessionId,
                                    SessionStatus.Running,
                                    partitionIds.ToIList(),
                                    defaultOptions));
    return Task.CompletedTask;
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
                       .Result.Status == SessionStatus.Canceled);

  /// <inheritdoc />
  public Task<TaskOptions> GetDefaultTaskOptionAsync(string            sessionId,
                                                     CancellationToken cancellationToken = default)
  {
    if (!storage_.ContainsKey(sessionId))
    {
      throw new SessionNotFoundException($"Key '{sessionId}' not found");
    }

    return Task.FromResult(storage_[sessionId]
                             .Options);
  }

  /// <inheritdoc />
  public Task<SessionData> CancelSessionAsync(string            sessionId,
                                 CancellationToken cancellationToken = default)
  {
    return Task.FromResult(storage_.AddOrUpdate(sessionId,
                                                _ => throw new SessionNotFoundException($"Key '{sessionId}' not found"),
                                                (_,
                                                 data) =>
                                                {
                                                  if (data.Status == SessionStatus.Canceled)
                                                  {
                                                    throw new SessionNotFoundException($"No open session with key '{sessionId}' was found");
                                                  }

                                                  return data with
                                                         {
                                                           Status = SessionStatus.Canceled,
                                                           CancellationDate = DateTime.UtcNow,
                                                         };
                                                }));
  }


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
                                        SessionFilter.StatusesOneofCase.None => true,
                                        SessionFilter.StatusesOneofCase.Included => sessionFilter.Included.Statuses.Contains(storage_[sessionId]
                                                                                                                               .Status),
                                        SessionFilter.StatusesOneofCase.Excluded => !sessionFilter.Excluded.Statuses.Contains(storage_[sessionId]
                                                                                                                                .Status),
                                        _ => throw new ArgumentException("Filter is set to an unknown StatusesCase."),
                                      });
  }

  public async Task<IEnumerable<SessionData>> ListSessionsAsync(ListSessionsRequest request,
                                                                CancellationToken   cancellationToken = default)
  {
    var queryable = storage_.AsQueryable()
                            .Select(pair => pair.Value)
                            .Where(request.Filter.ToSessionDataFilter());

    var ordered = request.Sort.Direction == ListSessionsRequest.Types.OrderDirection.Asc
                    ? queryable.OrderBy(request.Sort.ToSessionDataField())
                    : queryable.OrderByDescending(request.Sort.ToSessionDataField());

    return await ordered.Skip(request.Page * request.PageSize)
                        .Take(request.PageSize)
                        .ToListAsync(cancellationToken)
                        .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public ILogger Logger { get; }
}
