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

using TaskOptions = ArmoniK.Api.gRPC.V1.TaskOptions;

namespace ArmoniK.Core.Adapters.MongoDB.Table.DataModel;

public record SessionDataModelMapping : IMongoDataModelMapping<SessionData>
{
  static SessionDataModelMapping()
  {
    if (!BsonClassMap.IsClassMapRegistered(typeof(SessionData)))
      BsonClassMap.RegisterClassMap<SessionData>(cm =>
                                                      {
                                                        cm.MapProperty(nameof(SessionData.SessionId)).SetIsRequired(true);
                                                        cm.MapProperty(nameof(SessionData.DispatchId)).SetIsRequired(true);
                                                        cm.MapProperty(nameof(SessionData.AncestorsDispatchId)).SetIgnoreIfDefault(true);
                                                        cm.MapProperty(nameof(SessionData.IsCancelled)).SetIsRequired(true);
                                                        cm.MapProperty(nameof(SessionData.Options)).SetIsRequired(true).SetSerializer(new BsonProtoSerializer<TaskOptions>());
                                                        cm.SetIgnoreExtraElements(true);
                                                        cm.MapCreator(model => new(model.SessionId,
                                                                                   model.DispatchId,
                                                                                   model.AncestorsDispatchId,
                                                                                   model.IsCancelled,
                                                                                   model.Options));
                                                      });
  }

  /// <inheritdoc />
  public string CollectionName => nameof(SessionData);

  /// <inheritdoc />
  public Task InitializeIndexesAsync(IClientSessionHandle sessionHandle, IMongoCollection<SessionData> collection)
  {
    var sessionIndex = Builders<SessionData>.IndexKeys.Text(model => model.SessionId);
    var dispatchIndex = Builders<SessionData>.IndexKeys.Text(model => model.DispatchId);

    var indexModels = new CreateIndexModel<SessionData>[]
                      {
                        new(sessionIndex,
                            new()
                            {
                              Name = nameof(sessionIndex),
                            }),
                        new(dispatchIndex,
                            new()
                            {
                              Name = nameof(dispatchIndex),
                            }),
                      };

    return collection.Indexes.CreateManyAsync(sessionHandle,
                                              indexModels);
  }
}