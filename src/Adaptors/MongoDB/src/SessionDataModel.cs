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

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Core.Exceptions;
using ArmoniK.Core.gRPC.V1;

using Google.Protobuf.Collections;

using Microsoft.AspNetCore.DataProtection.KeyManagement;

using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace ArmoniK.Adapters.MongoDB
{
  public class SessionDataModel : IMongoDataModel<SessionDataModel>, ITaggedId
  {
    public string IdTag { get; set; }

    public string SessionId { get; set; }

    public string SubSessionId { get; set; }

    public List<string> ParentsId { get; set; }

    public bool IsClosed { get; set; }

    public bool IsCancelled { get; set; }

    public TaskOptions Options { get; set; }

    /// <inheritdoc />
    public string CollectionName { get; } = "SessionData";

    /// <inheritdoc />
    public Task InitializeIndexesAsync(IClientSessionHandle sessionHandle, IMongoCollection<SessionDataModel> collection)
    {
      var sessionIndex    = Builders<SessionDataModel>.IndexKeys.Text(model => model.SessionId);
      var subSessionIndex = Builders<SessionDataModel>.IndexKeys.Text(model => model.SubSessionId);
      var parentsIndex    = Builders<SessionDataModel>.IndexKeys.Text("ParentsId.Id");
      var sessionSubSessionIndex = Builders<SessionDataModel>.IndexKeys.Combine(sessionIndex,
                                                                                subSessionIndex);
      var sessionParentIndex = Builders<SessionDataModel>.IndexKeys.Combine(sessionIndex,
                                                                            parentsIndex);

      var indexModels = new CreateIndexModel<SessionDataModel>[]
                        {
                          new(sessionIndex,
                              new CreateIndexOptions { Name = nameof(sessionIndex) }),
                          new(sessionSubSessionIndex,
                              new CreateIndexOptions { Name = nameof(sessionSubSessionIndex), Unique = true }),
                          new(sessionParentIndex,
                              new CreateIndexOptions { Name = nameof(sessionParentIndex) }),
                        };

      return collection.Indexes.CreateManyAsync(sessionHandle,
                                                indexModels);
    }

    static SessionDataModel()
    {
      BsonClassMap.RegisterClassMap<SessionDataModel>(cm =>
                                                      {
                                                        cm.MapIdProperty(nameof(SubSessionId)).SetIsRequired(true).SetIdGenerator(new TaggedIdGenerator());
                                                        cm.MapProperty(nameof(SessionId)).SetIsRequired(true);
                                                        cm.MapProperty(nameof(ParentsId)).SetIgnoreIfDefault(true);
                                                        cm.MapProperty(nameof(IsClosed)).SetIsRequired(true);
                                                        cm.MapProperty(nameof(IsCancelled)).SetIsRequired(true);
                                                        cm.MapProperty(nameof(Options)).SetIsRequired(true).SetSerializer(new BsonProtoSerializer<TaskOptions>());
                                                        cm.SetIgnoreExtraElements(true);
                                                      });

    }
    
  }
}
