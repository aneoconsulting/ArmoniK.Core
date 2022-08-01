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

using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
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
                                                   map.MapCreator(options => new TaskOptions(options.Options,
                                                                                             options.MaxDuration,
                                                                                             options.MaxRetries,
                                                                                             options.Priority,
                                                                                             options.PartitionId));
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
    var statusIndex       = Builders<SessionData>.IndexKeys.Hashed(model => model.Status);
    var partitionIndex    = Builders<SessionData>.IndexKeys.Hashed(model => model.Options.PartitionId);
    var creationIndex     = Builders<SessionData>.IndexKeys.Ascending(model => model.CreationDate);
    var cancellationIndex = Builders<SessionData>.IndexKeys.Ascending(model => model.CancellationDate);

    var indexModels = new CreateIndexModel<SessionData>[]
                      {
                        new(statusIndex,
                            new CreateIndexOptions
                            {
                              Name = nameof(statusIndex),
                            }),
                        new(partitionIndex,
                            new CreateIndexOptions
                            {
                              Name = nameof(partitionIndex),
                            }),
                        new(creationIndex,
                            new CreateIndexOptions
                            {
                              Name = nameof(creationIndex),
                            }),
                        new(cancellationIndex,
                            new CreateIndexOptions
                            {
                              Name = nameof(cancellationIndex),
                            }),
                      };

    await collection.Indexes.CreateManyAsync(sessionHandle,
                                             indexModels)
                    .ConfigureAwait(false);
  }
}
