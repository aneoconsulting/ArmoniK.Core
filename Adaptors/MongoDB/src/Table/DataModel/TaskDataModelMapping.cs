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
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Common.Storage;

using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB.Table.DataModel;

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
                                                  .SetDefaultValue(Array.Empty<string>());
                                                cm.MapProperty(nameof(TaskData.ExpectedOutputIds))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(TaskData.InitialTaskId))
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
                                                cm.MapProperty(nameof(TaskData.PodTtl))
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
                                                                                    model.PodTtl,
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
                                           IMongoCollection<TaskData> collection)
  {
    var sessionIndex   = Builders<TaskData>.IndexKeys.Hashed(model => model.SessionId);
    var ownerIndex     = Builders<TaskData>.IndexKeys.Hashed(model => model.OwnerPodId);
    var creationIndex  = Builders<TaskData>.IndexKeys.Ascending(model => model.CreationDate);
    var submittedIndex = Builders<TaskData>.IndexKeys.Ascending(model => model.SubmittedDate);
    var startIndex     = Builders<TaskData>.IndexKeys.Ascending(model => model.StartDate);
    var endIndex       = Builders<TaskData>.IndexKeys.Ascending(model => model.EndDate);
    var partitionIndex = Builders<TaskData>.IndexKeys.Hashed(model => model.Options.PartitionId);
    var statusIndex    = Builders<TaskData>.IndexKeys.Hashed(model => model.Status);

    var indexModels = new CreateIndexModel<TaskData>[]
                      {
                        new(sessionIndex,
                            new CreateIndexOptions
                            {
                              Name = nameof(sessionIndex),
                            }),
                        new(ownerIndex,
                            new CreateIndexOptions
                            {
                              Name = nameof(ownerIndex),
                            }),
                        new(creationIndex,
                            new CreateIndexOptions
                            {
                              Name = nameof(creationIndex),
                            }),
                        new(submittedIndex,
                            new CreateIndexOptions
                            {
                              Name = nameof(submittedIndex),
                            }),
                        new(startIndex,
                            new CreateIndexOptions
                            {
                              Name = nameof(startIndex),
                            }),
                        new(endIndex,
                            new CreateIndexOptions
                            {
                              Name = nameof(endIndex),
                            }),
                        new(partitionIndex,
                            new CreateIndexOptions
                            {
                              Name = nameof(partitionIndex),
                            }),
                        new(statusIndex,
                            new CreateIndexOptions
                            {
                              Name = nameof(statusIndex),
                            }),
                      };

    await collection.Indexes.CreateManyAsync(sessionHandle,
                                             indexModels)
                    .ConfigureAwait(false);
  }
}
