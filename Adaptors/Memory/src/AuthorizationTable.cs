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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ArmoniK.Core.Adapters.Memory;

/// <inheritdoc />
public class AuthorizationTable : IAuthorizationTable
{
  private readonly ConcurrentDictionary<string, GrantData>                    grants_   = new();
  private readonly SemaphoreSlim                                             lock_     = new(1);
  private readonly ConcurrentDictionary<string, PermissionSetData>            sets_     = new();
  private readonly ConcurrentDictionary<(string, int), PermissionSetData>     versions_ = new();
  private          bool                                                       isInitialized_;
  private          int                                                        revision_;

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(isInitialized_
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy());

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
  {
    isInitialized_ = true;
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public IAsyncEnumerable<GrantData> ListGrantsAsync(CancellationToken cancellationToken = default)
    => grants_.Values.ToList()
              .ToAsyncEnumerable();

  /// <inheritdoc />
  public Task CreateGrantAsync(GrantData         grant,
                               CancellationToken cancellationToken = default)
    => grants_.TryAdd(grant.GrantId,
                      grant)
         ? Task.CompletedTask
         : throw new GrantAlreadyExistsException($"Grant '{grant.GrantId}' already exists");

  /// <inheritdoc />
  public Task<bool> DeleteGrantAsync(string            grantId,
                                     CancellationToken cancellationToken = default)
    => Task.FromResult(grants_.TryRemove(grantId,
                                         out _));

  /// <inheritdoc />
  public IAsyncEnumerable<PermissionSetData> ListPermissionSetsAsync(CancellationToken cancellationToken = default)
    => sets_.Values.ToList()
            .ToAsyncEnumerable();

  /// <inheritdoc />
  public Task<PermissionSetData?> GetPermissionSetVersionAsync(string            name,
                                                               int               version,
                                                               CancellationToken cancellationToken = default)
    => Task.FromResult(versions_.GetValueOrDefault((name, version)));

  /// <inheritdoc />
  public Task SetPermissionSetAsync(PermissionSetData permissionSet,
                                    CancellationToken cancellationToken = default)
  {
    versions_[(permissionSet.Name, permissionSet.Version)] = permissionSet;
    sets_[permissionSet.Name]                              = permissionSet;
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task<bool> DeletePermissionSetAsync(string            name,
                                             CancellationToken cancellationToken = default)
    => Task.FromResult(sets_.TryRemove(name,
                                       out _));

  /// <inheritdoc />
  public Task<int> GetConfigRevisionAsync(CancellationToken cancellationToken = default)
    => Task.FromResult(revision_);

  /// <inheritdoc />
  public Task SetConfigRevisionAsync(int               revision,
                                     CancellationToken cancellationToken = default)
  {
    revision_ = revision;
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public async Task<IAsyncDisposable> LockAsync(CancellationToken cancellationToken = default)
  {
    await lock_.WaitAsync(cancellationToken)
               .ConfigureAwait(false);
    return new Release(lock_);
  }

  private sealed class Release : IAsyncDisposable
  {
    private SemaphoreSlim? semaphore_;

    public Release(SemaphoreSlim semaphore)
      => semaphore_ = semaphore;

    public ValueTask DisposeAsync()
    {
      Interlocked.Exchange(ref semaphore_,
                           null)
                 ?.Release();
      return ValueTask.CompletedTask;
    }
  }
}
