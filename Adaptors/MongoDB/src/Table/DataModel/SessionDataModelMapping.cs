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

using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Storage;

using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB.Table.DataModel;

public record SessionDataModelMapping : IMongoDataModelMapping<SessionData>
{
  static SessionDataModelMapping()
  {
    if (!BsonClassMap.IsClassMapRegistered(typeof(SessionData)))
    {
      BsonClassMap.RegisterClassMap<SessionData>(cm =>
                                                 {
                                                   cm.MapIdProperty(nameof(SessionData.SessionId))
                                                     .SetIsRequired(true);
                                                   cm.MapProperty(nameof(SessionData.Status))
                                                     .SetIsRequired(true);
                                                   cm.MapProperty(nameof(SessionData.PartitionIds))
                                                     .SetIsRequired(true);
                                                   cm.MapProperty(nameof(SessionData.Options))
                                                     .SetIsRequired(true);
                                                   cm.MapProperty(nameof(SessionData.CreationDate))
                                                     .SetIsRequired(true);
                                                   cm.MapProperty(nameof(SessionData.CancellationDate))
                                                     .SetIsRequired(true);
                                                   cm.SetIgnoreExtraElements(true);
                                                   cm.MapCreator(model => new SessionData(model.SessionId,
                                                                                          model.Status,
                                                                                          model.CreationDate,
                                                                                          model.CancellationDate,
                                                                                          model.PartitionIds,
                                                                                          model.Options));
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
    => nameof(SessionData);

  /// <inheritdoc />
  public async Task InitializeIndexesAsync(IClientSessionHandle          sessionHandle,
                                           IMongoCollection<SessionData> collection)
  {
    var indexModels = new[]
                      {
                        IndexHelper.CreateAscendingIndex<SessionData>(model => model.CreationDate),
                        IndexHelper.CreateAscendingIndex<SessionData>(model => model.CancellationDate),
                        IndexHelper.CreateAscendingIndex<SessionData>(model => model.PartitionIds),
                        IndexHelper.CreateAscendingIndex<SessionData>(model => model.Options),
                        IndexHelper.CreateHashedIndex<SessionData>(model => model.Status),
                        IndexHelper.CreateHashedIndex<SessionData>(model => model.SessionId),
                        IndexHelper.CreateHashedIndex<SessionData>(model => model.Options.PartitionId),
                      };

    await collection.Indexes.CreateManyAsync(sessionHandle,
                                             indexModels)
                    .ConfigureAwait(false);
  }
}
