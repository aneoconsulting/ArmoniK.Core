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
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using TaskOptions = ArmoniK.Api.gRPC.V1.TaskOptions;

namespace ArmoniK.Core.Adapters.Memory;

public class SessionTable : ISessionTable
{
  private readonly ConcurrentDictionary<string, SessionData>                          storage_;
  private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, string>> session2Dispatches_;

  public SessionTable(ConcurrentDictionary<string, SessionData>                          storage,
                      ConcurrentDictionary<string, ConcurrentDictionary<string, string>> session2Dispatches,
                      ILogger<SessionTable>                                             logger)
  {
    storage_            = storage;
    session2Dispatches_ = session2Dispatches;
    Logger              = logger;
  }

  /// <inheritdoc />
  public ValueTask<bool> Check(HealthCheckTag tag)
    => ValueTask.FromResult(true);

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
    => Task.CompletedTask;

  /// <inheritdoc />
  public Task CreateSessionDataAsync(string            rootSessionId,
                                     string            parentTaskId,
                                     string            dispatchId,
                                     TaskOptions       defaultOptions,
                                     CancellationToken cancellationToken = default)
  {
    if (dispatchId == rootSessionId)
    {
      var dispatches = new ConcurrentDictionary<string, string>();
      if (!session2Dispatches_.TryAdd(rootSessionId,
                                      dispatches))
        throw new InvalidOperationException("Session already created");


      if (!storage_.TryAdd(dispatchId,
                           new(rootSessionId,
                               dispatchId,
                               false,
                               defaultOptions)))
        throw new InvalidOperationException("Session already created");
    }
    else
    {
      if (!session2Dispatches_.TryGetValue(rootSessionId,
                                           out var dispatches))
        throw new KeyNotFoundException("Session does not exist");

      if (!storage_.TryAdd(dispatchId,
                           new(rootSessionId,
                               dispatchId,
                               storage_[rootSessionId].IsCancelled,
                               defaultOptions)))
        throw new InvalidOperationException("The dispatch value already exists.");
      if (!dispatches.TryAdd(dispatchId,
                             dispatchId))
        throw new InvalidOperationException("The dispatch value already exists.");
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task<Common.Storage.SessionData> GetSessionAsync(string dispatchId, CancellationToken cancellationToken = default) => Task.FromResult<Common.Storage.SessionData>(storage_[dispatchId]);

  /// <inheritdoc />
  public Task<bool> IsSessionCancelledAsync(string sessionId, CancellationToken cancellationToken = default) => Task.FromResult(storage_[sessionId].IsCancelled);

  /// <inheritdoc />
  public Task<bool> IsDispatchCancelledAsync(string rootSessionId, string dispatchId, CancellationToken cancellationToken = default)
    => Task.FromResult(storage_[dispatchId].IsCancelled);

  /// <inheritdoc />
  public Task<TaskOptions> GetDefaultTaskOptionAsync(string sessionId, CancellationToken cancellationToken = default) => Task.FromResult(storage_[sessionId].Options);

  /// <inheritdoc />
  public async Task CancelSessionAsync(string sessionId, CancellationToken cancellationToken = default)
  {
    await session2Dispatches_[sessionId].Keys
                                        .Select(s => CancelDispatchAsync(sessionId,
                                                                         s,
                                                                         cancellationToken))
                                        .WhenAll();
  }

  /// <inheritdoc />
  public Task CancelDispatchAsync(string rootSessionId, string dispatchId, CancellationToken cancellationToken = default)
  {
    storage_.AddOrUpdate(dispatchId,
                         _ => throw new KeyNotFoundException(),
                         (_, data) => data with
                                      {
                                        IsCancelled = true,
                                      });
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public async Task DeleteSessionAsync(string sessionId, CancellationToken cancellationToken = default)
  {
    await session2Dispatches_[sessionId].Keys
                                        .ToImmutableList()
                                        .Select(dispatch => DeleteDispatchAsync(sessionId,
                                                                                dispatch,
                                                                                cancellationToken))
                                        .WhenAll();
  }

  /// <inheritdoc />
  public Task DeleteDispatchAsync(string rootSessionId, string dispatchId, CancellationToken cancellationToken = default)
  {
    session2Dispatches_[rootSessionId].Remove(dispatchId,
                                              out _);
    storage_.Remove(dispatchId,
                    out _);
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public IAsyncEnumerable<string> ListSessionsAsync(CancellationToken cancellationToken = default)
    => session2Dispatches_.Keys.ToAsyncEnumerable();

  /// <inheritdoc />
  public IAsyncEnumerable<string> ListDispatchesAsync(string rootSessionId, CancellationToken cancellationToken = default)
    => session2Dispatches_[rootSessionId].Keys.ToAsyncEnumerable();

  /// <inheritdoc />
  public ILogger Logger { get; }
}
