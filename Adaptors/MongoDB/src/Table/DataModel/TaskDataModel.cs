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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Common.Storage;

using MongoDB.Bson.Serialization;
using MongoDB.Driver;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Adapters.MongoDB.Table.DataModel;

public record TaskDataModel : TaskData, IMongoDataModel<TaskDataModel>
{
  public const string                 Collection = nameof(TaskDataModel);

  static TaskDataModel()
  {
    if (!BsonClassMap.IsClassMapRegistered(typeof(TaskDataModel)))
    {
      BsonClassMap.RegisterClassMap<TaskDataModel>(cm =>
                                                  {
                                                    cm.MapIdProperty(nameof(TaskId)).SetIsRequired(true);
                                                    cm.MapProperty(nameof(SessionId)).SetIsRequired(true);
                                                    cm.MapProperty(nameof(ParentTaskId)).SetIsRequired(true);
                                                    cm.MapProperty(nameof(DispatchId)).SetIsRequired(true);
                                                    cm.MapProperty(nameof(DataDependencies)).SetIgnoreIfDefault(true).SetDefaultValue(Array.Empty<string>());
                                                    cm.MapProperty(nameof(Status)).SetIsRequired(true);
                                                    cm.MapProperty(nameof(Options)).SetIsRequired(true).SetSerializer(new BsonProtoSerializer<TaskOptions>());
                                                    cm.MapProperty(nameof(CreationDate)).SetIsRequired(true);
                                                    cm.MapProperty(nameof(HasPayload)).SetIsRequired(true);
                                                    cm.MapProperty(nameof(Payload)).SetIgnoreIfDefault(true);
                                                    cm.MapProperty(nameof(AncestorDispatchIds)).SetIgnoreIfDefault(true).SetDefaultValue(Array.Empty<string>());
                                                    cm.MapProperty(nameof(ExpectedOutput)).SetIsRequired(true);
                                                    cm.SetIgnoreExtraElements(true);
                                                  });
    }

    if (!BsonClassMap.IsClassMapRegistered(typeof(TaskStatusCount)))
    {
      BsonClassMap.RegisterClassMap<TaskStatusCount>(map =>
                                                     {
                                                       map.MapProperty(nameof(TaskStatusCount.Status)).SetIsRequired(true);
                                                       map.MapProperty(nameof(TaskStatusCount.Count)).SetIsRequired(true);
                                                     });
    }
  }

  public TaskDataModel(string sessionId, string parentTaskId, string dispatchId, string taskId, IList<string> dataDependencies, IList<string> expectedOutput, bool hasPayload, byte[] payload, TaskStatus status, TaskOptions options, IList<string> ancestorDispatchIds) : base(sessionId, parentTaskId, dispatchId, taskId, dataDependencies, expectedOutput, hasPayload, payload, status, options, ancestorDispatchIds)
  {
  }

  public TaskDataModel(string        sessionId,
                       string        parentTaskId,
                       string        dispatchId,
                       string        taskId,
                       IList<string> dataDependencies,
                       IList<string> expectedOutput,
                       bool          hasPayload,
                       byte[]        payload,
                       TaskStatus    status,
                       TaskOptions   options,
                       IList<string> ancestorDispatchIds,
                       DateTime      creationDate)
    : base(sessionId,
           parentTaskId,
           dispatchId,
           taskId,
           dataDependencies,
           expectedOutput,
           hasPayload,
           payload,
           status,
           options,
           ancestorDispatchIds,
           creationDate)
  {
  }

  public TaskDataModel(TaskData original) : base(original)
  {
  }

  public TaskDataModel()
    : base(string.Empty,
           string.Empty,
           string.Empty,
           string.Empty,
           Array.Empty<string>(),
           Array.Empty<string>(),
           false,
           Array.Empty<byte>(),
           TaskStatus.Failed,
           new (),
           Array.Empty<string>(),
           default)
  {
  }

  /// <inheritdoc />
  public string CollectionName => Collection;


  /// <inheritdoc />
  public Task InitializeIndexesAsync(IClientSessionHandle            sessionHandle,
                                     IMongoCollection<TaskDataModel> collection)
  {
    var sessionIndex    = Builders<TaskDataModel>.IndexKeys.Text(model => model.SessionId);
    var statusIndex     = Builders<TaskDataModel>.IndexKeys.Text(model => model.Status);
    var sessionStatusIndex = Builders<TaskDataModel>.IndexKeys.Combine(sessionIndex,
                                                                       statusIndex);

    var indexModels = new CreateIndexModel<TaskDataModel>[]
                      {
                        new(sessionIndex,
                            new()
                            {
                              Name = nameof(sessionIndex),
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
}
