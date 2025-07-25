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

using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Common.Storage;

using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB.Table.DataModel;

public class PartitionDataModelMapping : IMongoDataModelMapping<PartitionData>
{
  static PartitionDataModelMapping()
  {
    if (!BsonClassMap.IsClassMapRegistered(typeof(PartitionData)))
    {
      BsonClassMap.RegisterClassMap<PartitionData>(cm =>
                                                   {
                                                     cm.MapIdProperty(nameof(PartitionData.PartitionId))
                                                       .SetIsRequired(true);
                                                     cm.MapProperty(nameof(PartitionData.ParentPartitionIds))
                                                       .SetIsRequired(true);
                                                     cm.MapProperty(nameof(PartitionData.PodReserved))
                                                       .SetIsRequired(true);
                                                     cm.MapProperty(nameof(PartitionData.PodMax))
                                                       .SetIsRequired(true);
                                                     cm.MapProperty(nameof(PartitionData.PreemptionPercentage))
                                                       .SetIsRequired(true);
                                                     cm.MapProperty(nameof(PartitionData.Priority))
                                                       .SetIsRequired(true);
                                                     cm.MapProperty(nameof(PartitionData.PodConfiguration))
                                                       .SetIsRequired(true);
                                                     cm.SetIgnoreExtraElements(true);
                                                     cm.MapCreator(model => new PartitionData(model.PartitionId,
                                                                                              model.ParentPartitionIds,
                                                                                              model.PodReserved,
                                                                                              model.PodMax,
                                                                                              model.PreemptionPercentage,
                                                                                              model.Priority,
                                                                                              model.PodConfiguration));
                                                   });
    }

    if (!BsonClassMap.IsClassMapRegistered(typeof(PodConfiguration)))
    {
      BsonClassMap.RegisterClassMap<PodConfiguration>(map =>
                                                      {
                                                        map.MapProperty(nameof(PodConfiguration.Configuration))
                                                           .SetIsRequired(true);

                                                        map.MapCreator(configuration => new PodConfiguration(configuration.Configuration));
                                                      });
    }
  }


  /// <inheritdoc />
  public string CollectionName
    => nameof(PartitionData);


  /// <inheritdoc />
  public async Task InitializeIndexesAsync(IClientSessionHandle            sessionHandle,
                                           IMongoCollection<PartitionData> collection,
                                           Options.MongoDB                 options)
  {
    var indexModels = new[]
                      {
                        IndexHelper.CreateHashedIndex<PartitionData>(model => model.PartitionId),
                      };

    await collection.Indexes.CreateManyAsync(sessionHandle,
                                             indexModels)
                    .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public Task ShardCollectionAsync(IClientSessionHandle sessionHandle,
                                   Options.MongoDB      options)
    => Task.CompletedTask;
}
