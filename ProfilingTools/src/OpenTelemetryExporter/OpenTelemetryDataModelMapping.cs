// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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

using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace ArmoniK.Core.ProfilingTools.OpenTelemetryExporter;

public class OpenTelemetryDataModelMapping : IMongoDataModelMapping<OpenTelemetryData>
{
  static OpenTelemetryDataModelMapping()
  {
    if (!BsonClassMap.IsClassMapRegistered(typeof(OpenTelemetryData)))
    {
      BsonClassMap.RegisterClassMap<OpenTelemetryData>(cm =>
                                                       {
                                                         cm.MapIdProperty(nameof(OpenTelemetryData.ActivityId))
                                                           .SetIsRequired(true);
                                                         cm.MapIdProperty(nameof(OpenTelemetryData.Baggage))
                                                           .SetIsRequired(true);
                                                         cm.MapIdProperty(nameof(OpenTelemetryData.Duration))
                                                           .SetIsRequired(true);
                                                         cm.MapIdProperty(nameof(OpenTelemetryData.Tags))
                                                           .SetIsRequired(true);
                                                         cm.MapIdProperty(nameof(OpenTelemetryData.SpanId))
                                                           .SetIsRequired(true);
                                                         cm.MapIdProperty(nameof(OpenTelemetryData.TraceId))
                                                           .SetIsRequired(true);
                                                         cm.MapIdProperty(nameof(OpenTelemetryData.ParentId))
                                                           .SetIsRequired(true);
                                                         cm.MapIdProperty(nameof(OpenTelemetryData.ParentSpanId))
                                                           .SetIsRequired(true);
                                                         cm.MapIdProperty(nameof(OpenTelemetryData.RootId))
                                                           .SetIsRequired(true);
                                                         cm.MapIdProperty(nameof(OpenTelemetryData.DisplayName))
                                                           .SetIsRequired(true);
                                                         cm.MapIdProperty(nameof(OpenTelemetryData.SourceName))
                                                           .SetIsRequired(true);
                                                         cm.MapIdProperty(nameof(OpenTelemetryData.StartTime))
                                                           .SetIsRequired(true);

                                                         cm.SetIgnoreExtraElements(true);
                                                         cm.MapCreator(model => new OpenTelemetryData(model.ActivityId,
                                                                                                      model.Baggage,
                                                                                                      model.Duration,
                                                                                                      model.Tags,
                                                                                                      model.SpanId,
                                                                                                      model.TraceId,
                                                                                                      model.ParentId,
                                                                                                      model.ParentSpanId,
                                                                                                      model.RootId,
                                                                                                      model.DisplayName,
                                                                                                      model.SourceName,
                                                                                                      model.StartTime));
                                                       });
    }
  }


  /// <inheritdoc />
  public string CollectionName
    => nameof(OpenTelemetryData);


  /// <inheritdoc />
  public async Task InitializeIndexesAsync(IClientSessionHandle                sessionHandle,
                                           IMongoCollection<OpenTelemetryData> collection,
                                           Adapters.MongoDB.Options.MongoDB    options)
  {
    var sourceNameIndex  = Builders<OpenTelemetryData>.IndexKeys.Hashed(model => model.SourceName);
    var displayNameIndex = Builders<OpenTelemetryData>.IndexKeys.Hashed(model => model.DisplayName);
    var activityIdIndex  = Builders<OpenTelemetryData>.IndexKeys.Hashed(model => model.ActivityId);

    var combinedIndex = Builders<OpenTelemetryData>.IndexKeys.Combine(sourceNameIndex,
                                                                      displayNameIndex);

    var indexModels = new CreateIndexModel<OpenTelemetryData>[]
                      {
                        new(combinedIndex,
                            new CreateIndexOptions
                            {
                              Name = nameof(combinedIndex),
                            }),
                        new(activityIdIndex,
                            new CreateIndexOptions
                            {
                              Name = nameof(activityIdIndex),
                            }),
                      };

    await collection.Indexes.CreateManyAsync(sessionHandle,
                                             indexModels)
                    .ConfigureAwait(false);
  }

  public Task ShardCollectionAsync(IClientSessionHandle             sessionHandle,
                                   Adapters.MongoDB.Options.MongoDB options)
    => Task.CompletedTask;
}
