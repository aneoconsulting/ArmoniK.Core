// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-$CURRENT_YEAR$. All rights reserved.
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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Logging;

using MongoDB.Driver;
using MongoDB.Driver.Linq;

namespace ArmoniK.Core.Adapters.MongoDB;

public class PartitionTable : IPartitionTable
{
  private readonly ActivitySource                                                    activitySource_;
  private readonly ILogger<TaskTable>                                                logger_;
  private readonly MongoCollectionProvider<PartitionData, PartitionDataModelMapping> partitionCollectionProvider_;
  private readonly SessionProvider                                                   sessionProvider_;

  private bool isInitialized_;

  public PartitionTable(SessionProvider                                                   sessionProvider,
                        MongoCollectionProvider<PartitionData, PartitionDataModelMapping> partitionCollectionProvider,
                        ILogger<TaskTable>                                                logger,
                        ActivitySource                                                    activitySource)
  {
    sessionProvider_             = sessionProvider;
    partitionCollectionProvider_ = partitionCollectionProvider;
    logger_                      = logger;
    activitySource_              = activitySource;
  }

  public ValueTask<bool> Check(HealthCheckTag tag)
    => ValueTask.FromResult(isInitialized_);

  public Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      sessionProvider_.Get();
      partitionCollectionProvider_.Get();
      isInitialized_ = true;
    }

    return Task.CompletedTask;
  }

  public async Task CreatePartitionsAsync(IEnumerable<PartitionData> partitions,
                                          CancellationToken          cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(CreatePartitionsAsync)}");

    var taskCollection = partitionCollectionProvider_.Get();

    await taskCollection.InsertManyAsync(partitions,
                                         cancellationToken: cancellationToken)
                        .ConfigureAwait(false);
  }

  public async Task<PartitionData> ReadPartitionAsync(string            partitionId,
                                                      CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(ReadPartitionAsync)}");
    activity?.SetTag("ReadPartitionId",
                     partitionId);
    var sessionHandle  = sessionProvider_.Get();
    var taskCollection = partitionCollectionProvider_.Get();

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

  public IAsyncEnumerable<PartitionData> GetPartitionWithAllocationAsync(CancellationToken cancellationToken = default)
  {
    using var activity       = activitySource_.StartActivity($"{nameof(ReadPartitionAsync)}");
    var       sessionHandle  = sessionProvider_.Get();
    var       taskCollection = partitionCollectionProvider_.Get();

    return taskCollection.AsQueryable(sessionHandle)
                         .Where(tdm => tdm.PodMax > 0)
                         .ToAsyncEnumerable();
  }

  public async Task DeletePartitionAsync(string            partitionId,
                                         CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity($"{nameof(DeletePartitionAsync)}");
    activity?.SetTag($"{nameof(DeletePartitionAsync)}_TaskId",
                     partitionId);
    var partitionCollection = partitionCollectionProvider_.Get();

    var result = await partitionCollection.DeleteOneAsync(tdm => tdm.PartitionId == partitionId,
                                                          cancellationToken)
                                          .ConfigureAwait(false);

    if (result.DeletedCount == 0)
    {
      throw new PartitionNotFoundException($"Partition '{partitionId}' not found.");
    }
  }
}
