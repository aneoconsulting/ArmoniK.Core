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
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Storage;

using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB.Table.DataModel;

/// <summary>
///   Implementation of <see cref="IMongoDataModelMapping{T}" /> for <see cref="TaskData" />
/// </summary>
public class TaskDataModelMapping : IMongoDataModelMapping<TaskData>
{
  static TaskDataModelMapping()
  {
    if (!BsonClassMap.IsClassMapRegistered(typeof(TaskData)))
    {
      BsonClassMap.RegisterClassMap<TaskData>(cm =>
                                              {
                                                cm.MapProperty(nameof(TaskData.SessionId))
                                                  .SetIsRequired(true);
                                                cm.MapIdProperty(nameof(TaskData.TaskId))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(TaskData.OwnerPodId))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(TaskData.OwnerPodName))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(TaskData.PayloadId))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(TaskData.ParentTaskIds))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(TaskData.DataDependencies))
                                                  .SetIgnoreIfDefault(true)
                                                  .SetDefaultValue(Array.Empty<string>());
                                                cm.MapProperty(nameof(TaskData.RemainingDataDependencies))
                                                  .SetIgnoreIfDefault(true)
                                                  .SetDefaultValue(new Dictionary<string, bool>());
                                                cm.MapProperty(nameof(TaskData.ExpectedOutputIds))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(TaskData.InitialTaskId))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(TaskData.CreatedBy))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(TaskData.RetryOfIds))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(TaskData.Status))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(TaskData.StatusMessage))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(TaskData.Options))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(TaskData.CreationDate))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(TaskData.SubmittedDate))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(TaskData.StartDate))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(TaskData.EndDate))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(TaskData.ReceptionDate))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(TaskData.AcquisitionDate))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(TaskData.ProcessedDate))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(TaskData.FetchedDate))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(TaskData.PodTtl))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(TaskData.ProcessingToEndDuration))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(TaskData.CreationToEndDuration))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(TaskData.ReceivedToEndDuration))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(TaskData.Output))
                                                  .SetIsRequired(true);
                                                cm.SetIgnoreExtraElements(true);
                                                cm.MapCreator(model => new TaskData(model.SessionId,
                                                                                    model.TaskId,
                                                                                    model.OwnerPodId,
                                                                                    model.OwnerPodName,
                                                                                    model.PayloadId,
                                                                                    model.ParentTaskIds,
                                                                                    model.DataDependencies,
                                                                                    model.RemainingDataDependencies,
                                                                                    model.ExpectedOutputIds,
                                                                                    model.InitialTaskId,
                                                                                    model.CreatedBy,
                                                                                    model.RetryOfIds,
                                                                                    model.Status,
                                                                                    model.StatusMessage,
                                                                                    model.Options,
                                                                                    model.CreationDate,
                                                                                    model.SubmittedDate,
                                                                                    model.StartDate,
                                                                                    model.EndDate,
                                                                                    model.ReceptionDate,
                                                                                    model.AcquisitionDate,
                                                                                    model.ProcessedDate,
                                                                                    model.FetchedDate,
                                                                                    model.PodTtl,
                                                                                    model.ProcessingToEndDuration,
                                                                                    model.CreationToEndDuration,
                                                                                    model.ReceivedToEndDuration,
                                                                                    model.Output));
                                              });
    }

    if (!BsonClassMap.IsClassMapRegistered(typeof(TaskStatusCount)))
    {
      BsonClassMap.RegisterClassMap<TaskStatusCount>(map =>
                                                     {
                                                       map.MapProperty(nameof(TaskStatusCount.Status))
                                                          .SetIsRequired(true);
                                                       map.MapProperty(nameof(TaskStatusCount.Count))
                                                          .SetIsRequired(true);
                                                       map.MapCreator(count => new TaskStatusCount(count.Status,
                                                                                                   count.Count));
                                                     });
    }

    if (!BsonClassMap.IsClassMapRegistered(typeof(TaskOptions)))
    {
      BsonClassMap.RegisterClassMap<TaskOptions>(map =>
                                                 {
                                                   map.MapProperty(nameof(TaskOptions.MaxDuration))
                                                      .SetIsRequired(true);
                                                   map.MapProperty(nameof(TaskOptions.MaxRetries))
                                                      .SetIsRequired(true);
                                                   map.MapProperty(nameof(TaskOptions.Options))
                                                      .SetIsRequired(true);
                                                   map.MapProperty(nameof(TaskOptions.Priority))
                                                      .SetIsRequired(true);
                                                   map.MapProperty(nameof(TaskOptions.PartitionId))
                                                      .SetIsRequired(true);
                                                   map.MapProperty(nameof(TaskOptions.ApplicationName))
                                                      .SetIsRequired(true);
                                                   map.MapProperty(nameof(TaskOptions.ApplicationVersion))
                                                      .SetIsRequired(true);
                                                   map.MapProperty(nameof(TaskOptions.ApplicationService))
                                                      .SetIsRequired(true);
                                                   map.MapProperty(nameof(TaskOptions.ApplicationNamespace))
                                                      .SetIsRequired(true);
                                                   map.MapProperty(nameof(TaskOptions.EngineType))
                                                      .SetIsRequired(true);
                                                   map.MapCreator(options => new TaskOptions(options.Options,
                                                                                             options.MaxDuration,
                                                                                             options.MaxRetries,
                                                                                             options.Priority,
                                                                                             options.PartitionId,
                                                                                             options.ApplicationName,
                                                                                             options.ApplicationVersion,
                                                                                             options.ApplicationNamespace,
                                                                                             options.ApplicationService,
                                                                                             options.EngineType));
                                                 });
    }
  }


  /// <inheritdoc />
  public string CollectionName
    => nameof(TaskData);


  /// <inheritdoc />
  public async Task InitializeIndexesAsync(IClientSessionHandle       sessionHandle,
                                           IMongoCollection<TaskData> collection,
                                           Options.MongoDB            options)
  {
    var indexModels = new List<CreateIndexModel<TaskData>>
                      {
                        IndexHelper.CreateHashedIndex<TaskData>(model => model.Status),
                        IndexHelper.CreateHashedIndex<TaskData>(model => model.Options.PartitionId),
                        IndexHelper.CreateHashedIndex<TaskData>(model => model.SessionId),
                        IndexHelper.CreateHashedIndex<TaskData>(model => model.InitialTaskId),
                        IndexHelper.CreateHashedIndex<TaskData>(model => model.CreatedBy),
                        IndexHelper.CreateCombinedIndex<TaskData>(model => model.Options.PartitionId,
                                                                  model => model.Status),
                      };

    if (options.UseMinimalIndexes == false)
    {
        indexModels.AddRange([
          IndexHelper.CreateHashedIndex<TaskData>(model => model.OwnerPodId),
          IndexHelper.CreateAscendingIndex<TaskData>(model => model.CreationDate,
                                                     expireAfter: options.DataRetention),
          IndexHelper.CreateAscendingIndex<TaskData>(model => model.SubmittedDate),
          IndexHelper.CreateAscendingIndex<TaskData>(model => model.StartDate),
          IndexHelper.CreateAscendingIndex<TaskData>(model => model.EndDate),
          IndexHelper.CreateAscendingIndex<TaskData>(model => model.CreationToEndDuration),
          IndexHelper.CreateAscendingIndex<TaskData>(model => model.ProcessingToEndDuration),
        ]);
    }

    await collection.Indexes.CreateManyAsync(sessionHandle,
                                             indexModels)
                    .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task ShardCollectionAsync(IClientSessionHandle sessionHandle,
                                         Options.MongoDB      options)
    => await sessionHandle.ShardCollection(options,
                                           CollectionName)
                          .ConfigureAwait(false);
}
