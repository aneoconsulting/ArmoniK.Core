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
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Logging;

using MongoDB.Driver;
using MongoDB.Driver.Linq;

namespace ArmoniK.Core.Adapters.MongoDB;

public class PartitionTable : BaseTable<PartitionData, PartitionDataModelMapping>, IPartitionTable
{
  /// <inheritdoc />
  public PartitionTable(SessionProvider                                                   sessionProvider,
                        MongoCollectionProvider<PartitionData, PartitionDataModelMapping> partitionCollectionProvider,
                        ActivitySource                                                    activitySource,
                        ILogger<PartitionTable>                                           logger)
    : base(sessionProvider,
           partitionCollectionProvider,
           activitySource,
           logger)
  {
  }

  /// <inheritdoc />
  private PartitionTable(PartitionTable partitionTable,
                         bool           readOnly)
    : base(partitionTable,
           readOnly)
  {
  }

  /// <inheritdoc />
  public IPartitionTable Secondary
    => new PartitionTable(this,
                          true);

  /// <inheritdoc />
  public async Task CreatePartitionsAsync(IEnumerable<PartitionData> partitions,
                                          CancellationToken          cancellationToken = default)
  {
    using var _        = Logger.LogFunction();
    using var activity = StartActivity();

    var taskCollection = GetCollection();

    await taskCollection.InsertManyAsync(partitions,
                                         cancellationToken: cancellationToken)
                        .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task<PartitionData> ReadPartitionAsync(string            partitionId,
                                                      CancellationToken cancellationToken = default)
  {
    using var _        = Logger.LogFunction();
    using var activity = StartActivity();
    activity?.SetTag("ReadPartitionId",
                     partitionId);
    var sessionHandle  = GetSession();
    var taskCollection = GetReadCollection();

    try
    {
      return await taskCollection.AsQueryable(sessionHandle)
                                 .Where(tdm => tdm.PartitionId == partitionId)
                                 .SingleAsync(cancellationToken)
                                 .ConfigureAwait(false);
    }
    catch (InvalidOperationException e)
    {
      throw new PartitionNotFoundException($"Partition '{partitionId}' not found.",
                                           e);
    }
  }

  /// <inheritdoc />
  public IAsyncEnumerable<PartitionData> GetPartitionWithAllocationAsync(CancellationToken cancellationToken = default)
  {
    using var _              = Logger.LogFunction();
    using var activity       = StartActivity();
    var       sessionHandle  = GetSession();
    var       taskCollection = GetReadCollection();

    return taskCollection.AsQueryable(sessionHandle)
                         .Where(tdm => tdm.PodMax > 0)
                         .ToAsyncEnumerable(cancellationToken);
  }

  /// <inheritdoc />
  public async Task DeletePartitionAsync(string            partitionId,
                                         CancellationToken cancellationToken = default)
  {
    using var _        = Logger.LogFunction();
    using var activity = StartActivity();
    activity?.SetTag($"{nameof(DeletePartitionAsync)}_TaskId",
                     partitionId);
    var partitionCollection = GetCollection();

    var result = await partitionCollection.DeleteOneAsync(tdm => tdm.PartitionId == partitionId,
                                                          cancellationToken)
                                          .ConfigureAwait(false);

    if (result.DeletedCount == 0)
    {
      throw new PartitionNotFoundException($"Partition '{partitionId}' not found.");
    }
  }

  /// <inheritdoc />
  public async Task<bool> ArePartitionsExistingAsync(IEnumerable<string> partitionIds,
                                                     CancellationToken   cancellationToken = default)
  {
    using var _              = Logger.LogFunction();
    using var activity       = StartActivity();
    var       sessionHandle  = GetSession();
    var       taskCollection = GetReadCollection();

    return await taskCollection.AsQueryable(sessionHandle)
                               .CountAsync(tdm => partitionIds.Contains(tdm.PartitionId),
                                           cancellationToken)
                               .ConfigureAwait(false) == partitionIds.Count();
  }

  /// <inheritdoc />
  public async Task<(IEnumerable<PartitionData> partitions, int totalCount)> ListPartitionsAsync(Expression<Func<PartitionData, bool>>    filter,
                                                                                                 Expression<Func<PartitionData, object?>> orderField,
                                                                                                 bool                                     ascOrder,
                                                                                                 int                                      page,
                                                                                                 int                                      pageSize,
                                                                                                 CancellationToken                        cancellationToken = default)
  {
    using var activity            = StartActivity();
    var       sessionHandle       = GetSession();
    var       partitionCollection = GetReadCollection();

    var partitionList = Task.FromResult(new List<PartitionData>());
    if (pageSize > 0)
    {
      var findFluent1 = partitionCollection.Find(sessionHandle,
                                                 filter);

      var ordered = ascOrder
                      ? findFluent1.SortBy(orderField)
                      : findFluent1.SortByDescending(orderField);

      partitionList = ordered.Skip(page * pageSize)
                             .Limit(pageSize)
                             .ToListAsync(cancellationToken);
    }

    // Find needs to be duplicated, otherwise, the count is computed on a single page, and not the whole collection
    var partitionCount = partitionCollection.CountDocumentsAsync(sessionHandle,
                                                                 filter,
                                                                 cancellationToken: cancellationToken);

    return (await partitionList.ConfigureAwait(false), (int)await partitionCount.ConfigureAwait(false));
  }

  /// <summary>
  ///   Find all partitions matching the given filter
  /// </summary>
  /// <param name="filter">Filter to select partitions</param>
  /// <param name="selector">Expression to select part of the returned partition</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   List of partitions that match the filter
  /// </returns>
  public async IAsyncEnumerable<T> FindPartitionsAsync<T>(Expression<Func<PartitionData, bool>>      filter,
                                                          Expression<Func<PartitionData, T>>         selector,
                                                          [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    using var activity            = StartActivity();
    var       sessionHandle       = GetSession();
    var       partitionCollection = GetReadCollection();

    await foreach (var partition in partitionCollection.Find(sessionHandle,
                                                             filter)
                                                       .Project(selector)
                                                       .ToAsyncEnumerable(cancellationToken)
                                                       .ConfigureAwait(false))
    {
      yield return partition;
    }
  }
}
