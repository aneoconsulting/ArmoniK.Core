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
using ArmoniK.Core.Common.Injection.Options.Database;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ArmoniK.Core.Adapters.Memory;

/// <summary>
///   In-memory implementation of <see cref="IPartitionTable" />.
/// </summary>
/// <remarks>
///   Every public method runs under a single lock, so that each call is atomic.
/// </remarks>
public class PartitionTable : IPartitionTable
{
  private readonly InitDatabase?                     initDatabase_;
  private readonly Lock                              lock_       = new();
  private readonly Dictionary<string, PartitionData> partitions_ = new();

  private bool isInitialized_;

  /// <summary>
  ///   Creates an empty partition table
  /// </summary>
  /// <param name="initDatabase">Partitions to create when the table is initialized, if any</param>
  public PartitionTable(InitDatabase? initDatabase = null)
    => initDatabase_ = initDatabase;

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
  {
    lock (lock_)
    {
      if (!isInitialized_ && initDatabase_ is
          {
            Init: true,
          })
      {
        // Partitions that already exist are kept, as the database adapters do.
        foreach (var partition in initDatabase_.Partitions)
        {
          partitions_.TryAdd(partition.PartitionId,
                             partition);
        }
      }

      isInitialized_ = true;
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(isInitialized_
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy());

  /// <inheritdoc />
  public Task CreatePartitionsAsync(IEnumerable<PartitionData> partitions,
                                    CancellationToken          cancellationToken = default)
  {
    var newPartitions = partitions.ToList();

    lock (lock_)
    {
      // Validate the whole batch before inserting, so that a duplicate does not leave part of the batch inserted.
      var ids = new HashSet<string>();
      foreach (var partition in newPartitions)
      {
        if (partitions_.ContainsKey(partition.PartitionId) || !ids.Add(partition.PartitionId))
        {
          throw new ArmoniKException($"Partition '{partition.PartitionId}' already exists");
        }
      }

      foreach (var partition in newPartitions)
      {
        partitions_.Add(partition.PartitionId,
                        partition);
      }
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task<PartitionData> ReadPartitionAsync(string            partitionId,
                                                CancellationToken cancellationToken = default)
  {
    lock (lock_)
    {
      return partitions_.TryGetValue(partitionId,
                                     out var partition)
               ? Task.FromResult(partition)
               : throw new PartitionNotFoundException($"Partition '{partitionId}' not found.");
    }
  }

  /// <inheritdoc />
  public IAsyncEnumerable<PartitionData> GetPartitionWithAllocationAsync(CancellationToken cancellationToken = default)
    => Snapshot(partition => partition.PodMax > 0)
      .ToAsyncEnumerable();

  /// <inheritdoc />
  public Task DeletePartitionAsync(string            partitionId,
                                   CancellationToken cancellationToken = default)
  {
    lock (lock_)
    {
      if (!partitions_.Remove(partitionId))
      {
        throw new PartitionNotFoundException($"Partition '{partitionId}' not found.");
      }
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task<bool> ArePartitionsExistingAsync(IEnumerable<string> partitionIds,
                                               CancellationToken   cancellationToken = default)
  {
    var ids = partitionIds.ToList();

    lock (lock_)
    {
      return Task.FromResult(ids.All(partitions_.ContainsKey));
    }
  }

  /// <inheritdoc />
  public Task<(IEnumerable<PartitionData> partitions, int totalCount)> ListPartitionsAsync(Expression<Func<PartitionData, bool>>    filter,
                                                                                           Expression<Func<PartitionData, object?>> orderField,
                                                                                           bool                                     ascOrder,
                                                                                           int                                      page,
                                                                                           int                                      pageSize,
                                                                                           CancellationToken                        cancellationToken = default)
  {
    var matching = Snapshot(filter.Compile());
    var partitions = matching.OrderByKey(orderField,
                                         ascOrder)
                             .Skip(page * pageSize)
                             .Take(pageSize)
                             .ToList();

    return Task.FromResult<(IEnumerable<PartitionData> partitions, int totalCount)>((partitions, matching.Count));
  }

  /// <inheritdoc />
  public IAsyncEnumerable<T> FindPartitionsAsync<T>(Expression<Func<PartitionData, bool>> filter,
                                                    Expression<Func<PartitionData, T>>    selector,
                                                    CancellationToken                     cancellationToken = default)
    => Snapshot(filter.Compile())
       .Select(selector.Compile())
       .ToAsyncEnumerable();

  /// <summary>
  ///   Copy the partitions matching the filter, under the lock
  /// </summary>
  /// <param name="filter">Filter on the partitions</param>
  /// <returns>
  ///   The matching partitions, at the time of the call
  /// </returns>
  private List<PartitionData> Snapshot(Func<PartitionData, bool> filter)
  {
    lock (lock_)
    {
      return partitions_.Values.Where(filter)
                        .ToList();
    }
  }
}
