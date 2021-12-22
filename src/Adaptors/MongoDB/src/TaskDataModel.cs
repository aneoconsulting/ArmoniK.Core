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
using System.Threading.Tasks;

using ArmoniK.Core.gRPC.V1;

using Google.Protobuf;

using MongoDB.Bson.Serialization;
using MongoDB.Driver;

using TaskStatus = ArmoniK.Core.gRPC.V1.TaskStatus;

namespace ArmoniK.Adapters.MongoDB
{
  public class TaskDataModel : IMongoDataModel<TaskDataModel>, ITaggedId
  {
    static TaskDataModel()
    {
      if (!BsonClassMap.IsClassMapRegistered(typeof(TaskDataModel)))
        BsonClassMap.RegisterClassMap<TaskDataModel>(cm =>
                                                     {
                                                       cm.MapIdProperty(nameof(TaskId)).SetIsRequired(true).SetIdGenerator(new TaggedIdGenerator());
                                                       cm.MapProperty(nameof(SessionId)).SetIsRequired(true);
                                                       cm.MapProperty(nameof(SubSessionId)).SetIsRequired(true);
                                                       cm.MapProperty(nameof(Options)).SetIsRequired(true).SetSerializer(new BsonProtoSerializer<TaskOptions>());
                                                       cm.MapProperty(nameof(Status)).SetIsRequired(true);
                                                       cm.MapProperty(nameof(Retries)).SetIsRequired(true);
                                                       cm.MapProperty(nameof(HasPayload)).SetIsRequired(true);
                                                       cm.MapProperty(nameof(Payload)).SetIgnoreIfDefault(true);
                                                       cm.MapProperty(nameof(Dependencies)).SetIgnoreIfDefault(true).SetDefaultValue(Array.Empty<string>());
                                                       cm.MapProperty(nameof(ParentsSubSessions)).SetIgnoreIfDefault(true).SetDefaultValue(Array.Empty<string>());
                                                       cm.SetIgnoreExtraElements(true);
                                                     });
    }

    public string SessionId { get; set; }

    public string SubSessionId { get; set; }

    public string TaskId { get; set; }

    public TaskOptions Options { get; set; }

    public TaskStatus Status { get; set; }

    public int Retries { get; set; }

    public bool HasPayload { get; set; }

    public byte[] Payload { get; set; }

    public IList<string> Dependencies { get; set; } = Array.Empty<string>();

    public IList<string> ParentsSubSessions { get; set; } = Array.Empty<string>();

    /// <inheritdoc />
    public string CollectionName => "tasks";

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
                              new()
                              {
                                Name = nameof(sessionIndex),
                              }),
                          new(taskIdIndex,
                              new()
                              {
                                Name   = nameof(taskIdIndex),
                                Unique = true,
                              }),
                          new(sessionStatusIndex,
                              new()
                              {
                                Name = nameof(sessionStatusIndex),
                              }),
                        };

      return collection.Indexes.CreateManyAsync(sessionHandle,
                                                indexModels);
    }

    /// <inheritdoc />
    public string IdTag => Options.IdTag;

    public TaskData ToTaskData() => new()
                                    {
                                      Id = new()
                                           {
                                             Session    = SessionId,
                                             SubSession = SubSessionId,
                                             Task       = TaskId,
                                           },
                                      IsPayloadAvailable = HasPayload,
                                      Payload = new()
                                                {
                                                  Data = ByteString.CopyFrom(Payload),
                                                },
                                      Options = Options,
                                      Retries = Retries,
                                      Status  = Status,
                                      Dependencies =
                                      {
                                        Dependencies,
                                      },
                                    };

    public TaskId GetTaskId() => new()
                                 {
                                   Session    = SessionId,
                                   SubSession = SubSessionId,
                                   Task       = TaskId,
                                 };
  }
}
