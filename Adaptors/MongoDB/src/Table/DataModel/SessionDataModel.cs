// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
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

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Adapters.MongoDB.Common;

using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB.Table;

public class SessionDataModel : IMongoDataModel<SessionDataModel>
{
  public const string Collection = "SessionData";

  static SessionDataModel()
  {
    if (!BsonClassMap.IsClassMapRegistered(typeof(SessionDataModel)))
      BsonClassMap.RegisterClassMap<SessionDataModel>(cm =>
                                                      {
                                                        cm.MapProperty(nameof(SessionId)).SetIsRequired(true);
                                                        cm.MapProperty(nameof(DispatchId)).SetIsRequired(true);
                                                        cm.MapProperty(nameof(AncestorsDispatchId)).SetIgnoreIfDefault(true);
                                                        cm.MapProperty(nameof(IsCancelled)).SetIsRequired(true);
                                                        cm.MapProperty(nameof(Options)).SetIsRequired(true).SetSerializer(new BsonProtoSerializer<TaskOptions>());
                                                        cm.SetIgnoreExtraElements(true);
                                                      });
  }


  public string SessionId { get; set; }

  public string DispatchId { get; set; }

  public IEnumerable<string> AncestorsDispatchId { get; set; }

  public bool IsCancelled { get; set; }

  public TaskOptions Options { get; set; }

  /// <inheritdoc />
  public string CollectionName { get; } = Collection;

  /// <inheritdoc />
  public Task InitializeIndexesAsync(IClientSessionHandle sessionHandle, IMongoCollection<SessionDataModel> collection)
  {
    var sessionIndex = Builders<SessionDataModel>.IndexKeys.Text(model => model.SessionId);
    var dispatchIndex = Builders<SessionDataModel>.IndexKeys.Text(model => model.DispatchId);

    var indexModels = new CreateIndexModel<SessionDataModel>[]
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