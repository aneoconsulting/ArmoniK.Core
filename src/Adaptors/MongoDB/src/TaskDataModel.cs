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
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Core.gRPC.V1;

using Google.Protobuf;

using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

using TaskStatus = ArmoniK.Core.gRPC.V1.TaskStatus;

namespace ArmoniK.Adapters.MongoDB
{
  public class TaskDataModel : IMongoDataModel<TaskDataModel>
  {
    [BsonElement]
    [BsonRequired]
    public string SessionId { get; set; }

    [BsonElement]
    [BsonRequired]
    public string SubSessionId { get; set; }

    [BsonId(IdGenerator = typeof(StringCombGuidGenerator))]
    public string TaskId { get; set; }

    [BsonElement]
    [BsonRequired]
    [BsonSerializer(typeof(BsonProtoSerializer<TaskOptions>))]
    public TaskOptions Options { get; set; }

    [BsonElement]
    public TaskStatus Status { get; set; }

    [BsonElement]
    [BsonRequired]
    public int Retries { get; set; }

    [BsonElement]
    [BsonRequired]
    public bool HasPayload { get; set; }

    [BsonElement]
    public byte[] Payload { get; set; }

    [BsonElement]
    public IEnumerable<string> Dependencies { get; set; }

    [BsonElement]
    public IEnumerable<ParentSubSessionRelation> ParentRelations {
      get
      {
        return ParentsSubSessions.Select(s => new ParentSubSessionRelation { ParentSubSession = s, TaskId = TaskId });
      }
      set
      {
        ParentsSubSessions = value.Select(relation => relation.ParentSubSession).ToList();
      }
    }


    [BsonIgnore]
    public IList<string> ParentsSubSessions { get; set; } = Array.Empty<string>();

    /// <inheritdoc />
    public string CollectionName { get; } = "tasks";

    /// <inheritdoc />
    public Task InitializeIndexesAsync(IClientSessionHandle            sessionHandle,
                                       IMongoCollection<TaskDataModel> collection)
    {
      var sessionIndex    = Builders<TaskDataModel>.IndexKeys.Text(model => model.SessionId);
      var subSessionIndex = Builders<TaskDataModel>.IndexKeys.Text(model => model.SubSessionId);
      var taskIndex       = Builders<TaskDataModel>.IndexKeys.Text(model => model.TaskId);
      var statusIndex     = Builders<TaskDataModel>.IndexKeys.Text(model => model.Status);
      var taskIdIndex = Builders<TaskDataModel>.IndexKeys.Combine(sessionIndex,
                                                                  subSessionIndex,
                                                                  taskIndex);
      var sessionStatusIndex = Builders<TaskDataModel>.IndexKeys.Combine(sessionIndex,
                                                                         statusIndex);

      var indexModels = new CreateIndexModel<TaskDataModel>[]
                        {
                          new(sessionIndex,
                              new CreateIndexOptions { Name = nameof(sessionIndex) }),
                          new(taskIdIndex,
                              new CreateIndexOptions { Name = nameof(taskIdIndex), Unique = true }),
                          new(sessionStatusIndex,
                              new CreateIndexOptions { Name = nameof(sessionStatusIndex) }),
                        };

      return collection.Indexes.CreateManyAsync(sessionHandle,
                                                indexModels);
    }

    public TaskData ToTaskData() => new()
                                    {
                                      Id = new TaskId
                                           {
                                             Session    = SessionId,
                                             SubSession = SubSessionId,
                                             Task       = TaskId,
                                           },
                                      IsPayloadAvailable = HasPayload,
                                      Payload            = new Payload { Data = ByteString.CopyFrom(Payload) },
                                      Options            = Options,
                                      Retries            = Retries,
                                      Status             = Status,
                                      Dependencies       = { Dependencies },
                                    };

    public TaskId GetTaskId() => new() { Session = SessionId, SubSession = SubSessionId, Task = TaskId };
  }

  public struct ParentSubSessionRelation
  {
    [BsonElement]
    public string ParentSubSession { get; set; }

    [BsonElement]
    public string TaskId           { get; set; }
  }
}
