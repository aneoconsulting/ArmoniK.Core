// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
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

using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Logging;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Adapters.Memory;

public class DispatchTable : IDispatchTable
{

  private readonly ConcurrentDictionary<string, Dispatch> taskIndexedStorage_ = new();
  private readonly ConcurrentDictionary<string, Dispatch> idIndexedStorage_   = new();

  public DispatchTable(ILogger logger)
    => Logger = logger;

  /// <inheritdoc />
  public TimeSpan DispatchTimeToLiveDuration => TimeSpan.FromHours(1);

  /// <inheritdoc />
  public ILogger Logger { get; }

  /// <inheritdoc />
  public TimeSpan DispatchRefreshPeriod => TimeSpan.FromMinutes(1);

  /// <inheritdoc />
  public Task<bool> TryAcquireDispatchAsync(string                      sessionId,
                                            string                      taskId,
                                            string                      dispatchId,
                                            IDictionary<string, string> metadata,
                                            CancellationToken           cancellationToken = default)
  {
    var dispatch = new Dispatch(sessionId,
                                taskId,
                                dispatchId,
                                DateTime.UtcNow + DispatchTimeToLiveDuration,
                                1,
                                new()
                                {
                                  new(TaskStatus.Dispatched,
                                      DateTime.UtcNow,
                                      string.Empty),
                                },
                                DateTime.UtcNow);

    var d = taskIndexedStorage_.GetOrAdd(taskId,
                                         dispatch);

    lock (d)
    {
      // there was a previous dispatch but it has expired
      if (d.Id != dispatchId && d.TimeToLive < DateTime.UtcNow)
      {
        if (!idIndexedStorage_.TryAdd(dispatchId,
                                      d))
        {
          throw new InvalidOperationException("An expired dispatch with same id exists.");
        }

        var old = new Dispatch(d.SessionId,
                               d.TaskId,
                               d.Id,
                               DateTime.MinValue,
                               d.Attempt,
                               new (d.Statuses),
                               d.CreationDate);
        old.StatusesBag.Add(new(TaskStatus.Error,
                             DateTime.UtcNow,
                             "Ttl expired"));

        idIndexedStorage_[d.Id] = old;

        idIndexedStorage_.AddOrUpdate(dispatchId,
                                      s => throw new InvalidOperationException("This path should never be executed"),
                                      (s, dispatch1) => dispatch with
                                                        {
                                                          Attempt = dispatch1.Attempt + 1,
                                                        });
      }
    }


    return Task.FromResult(d.Id == dispatchId);
  }

  /// <inheritdoc />
  public Task<Common.Storage.Dispatch> GetDispatchAsync(string dispatchId, CancellationToken cancellationToken = default)
  {
    // ReSharper disable once InconsistentlySynchronizedField
    var locker = idIndexedStorage_[dispatchId];

    lock (locker)
    {
      return Task.FromResult(idIndexedStorage_[dispatchId] as Common.Storage.Dispatch);
    }
  }

  /// <inheritdoc />
  public Task AddStatusToDispatch(string id, TaskStatus status, CancellationToken cancellationToken = default)
  {
    // ReSharper disable once InconsistentlySynchronizedField
    var locker = idIndexedStorage_[id];

    lock (locker)
    {
      idIndexedStorage_[id].StatusesBag.Add(new(status,
                                             DateTime.UtcNow,
                                             string.Empty));
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task ExtendDispatchTtl(string id, CancellationToken cancellationToken = default)
  {
    // ReSharper disable once InconsistentlySynchronizedField
    var locker = idIndexedStorage_[id];

    lock (locker)
    {
      idIndexedStorage_.AddOrUpdate(id,
                                    _ => throw new InvalidOperationException("Should never come here."),
                                    (s, dispatch) => dispatch.TimeToLive >= DateTime.UtcNow
                                                       ? dispatch with
                                                         {
                                                           TimeToLive = DateTime.UtcNow + DispatchTimeToLiveDuration,
                                                         }
                                                       : throw new InvalidOperationException("Ttl was expired"));
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task DeleteDispatchFromTaskIdAsync(string id, CancellationToken cancellationToken = default)
    => throw new NotImplementedException();

  /// <inheritdoc />
  public Task DeleteDispatch(string id, CancellationToken cancellationToken = default)
  {
    // ReSharper disable once InconsistentlySynchronizedField
    var locker = idIndexedStorage_[id];

    lock (locker)
    {
      idIndexedStorage_.Remove(id,
                               out var dispatch);
      if (taskIndexedStorage_[dispatch.TaskId].Id == id)
      {
        taskIndexedStorage_.Remove(id,
                                   out _);
      }
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public IAsyncEnumerable<string> ListDispatchAsync(string taskId, CancellationToken cancellationToken = default)
    // ReSharper disable once InconsistentlySynchronizedField
    => idIndexedStorage_.Values
                        .ToImmutableList()
                        .Where(dispatch => dispatch.TaskId == taskId)
                        .Select(dispatch => dispatch.Id)
                        .ToAsyncEnumerable();

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
  {
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public ValueTask<bool> Check(HealthCheckTag tag) => ValueTask.FromResult(true);
}
