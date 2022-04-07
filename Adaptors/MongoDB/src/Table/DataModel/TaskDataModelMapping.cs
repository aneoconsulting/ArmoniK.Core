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

using System;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Common.Storage;

using MongoDB.Bson.Serialization;
using MongoDB.Driver;

using TaskOptions = ArmoniK.Core.Common.Storage.TaskOptions;


namespace ArmoniK.Core.Adapters.MongoDB.Table.DataModel;


public class TaskDataModelMapping : IMongoDataModelMapping<TaskData>
{
  static TaskDataModelMapping()
  {
    if (!BsonClassMap.IsClassMapRegistered(typeof(TaskData)))
    {
      BsonClassMap.RegisterClassMap<TaskData>(cm =>
                                                   {
                                                     cm.MapIdProperty(nameof(TaskData.TaskId)).SetIsRequired(true);
                                                     cm.MapProperty(nameof(TaskData.SessionId)).SetIsRequired(true);
                                                     cm.MapProperty(nameof(TaskData.ParentTaskId)).SetIsRequired(true);
                                                     cm.MapProperty(nameof(TaskData.DispatchId)).SetIsRequired(true);
                                                     cm.MapProperty(nameof(TaskData.DataDependencies)).SetIgnoreIfDefault(true).SetDefaultValue(Array.Empty<string>());
                                                     cm.MapProperty(nameof(TaskData.Status)).SetIsRequired(true);
                                                     cm.MapProperty(nameof(TaskData.Options)).SetIsRequired(true);
                                                     cm.MapProperty(nameof(TaskData.CreationDate)).SetIsRequired(true);
                                                     cm.MapProperty(nameof(TaskData.HasPayload)).SetIsRequired(true);
                                                     cm.MapProperty(nameof(TaskData.Payload)).SetIgnoreIfDefault(true);
                                                     cm.MapProperty(nameof(TaskData.AncestorDispatchIds)).SetIgnoreIfDefault(true).SetDefaultValue(Array.Empty<string>());
                                                     cm.MapProperty(nameof(TaskData.ExpectedOutput)).SetIsRequired(true);
                                                     cm.MapProperty(nameof(TaskData.Output)).SetIsRequired(true);
                                                     cm.SetIgnoreExtraElements(true);
                                                     cm.MapCreator(model => new(model.SessionId,
                                                                                model.ParentTaskId,
                                                                                model.DispatchId,
                                                                                model.TaskId,
                                                                                model.DataDependencies,
                                                                                model.ExpectedOutput,
                                                                                model.HasPayload,
                                                                                model.Payload,
                                                                                model.Status,
                                                                                model.Options,
                                                                                model.AncestorDispatchIds,
                                                                                model.CreationDate,
                                                                     model.Output));
                                                   });
    }

    if (!BsonClassMap.IsClassMapRegistered(typeof(TaskStatusCount)))
    {
      BsonClassMap.RegisterClassMap<TaskStatusCount>(map =>
                                                     {
                                                       map.MapProperty(nameof(TaskStatusCount.Status)).SetIsRequired(true);
                                                       map.MapProperty(nameof(TaskStatusCount.Count)).SetIsRequired(true);
                                                       map.MapCreator(count => new(count.Status,
                                                                                   count.Count));
                                                     });
    }

    if (!BsonClassMap.IsClassMapRegistered(typeof(TaskOptions)))
    {
      BsonClassMap.RegisterClassMap<TaskOptions>(map =>
                                                 {
                                                   map.MapProperty(nameof(TaskOptions.MaxDuration)).SetIsRequired(true);
                                                   map.MapProperty(nameof(TaskOptions.MaxRetries)).SetIsRequired(true);
                                                   map.MapProperty(nameof(TaskOptions.Options)).SetIsRequired(true);
                                                   map.MapProperty(nameof(TaskOptions.Priority)).SetIsRequired(true);
                                                   map.MapCreator(options => new(options.Options,
                                                                                 options.MaxDuration,
                                                                                 options.MaxRetries,
                                                                                 options.Priority));
                                                 });
    }
  }


  /// <inheritdoc />
  public string CollectionName => nameof(TaskData);


  /// <inheritdoc />
  public async Task InitializeIndexesAsync(IClientSessionHandle       sessionHandle,
                                     IMongoCollection<TaskData> collection)
  {
    var sessionIndex = Builders<TaskData>.IndexKeys.Hashed(model => model.SessionId);
    var statusIndex  = Builders<TaskData>.IndexKeys.Hashed(model => model.Status);
    var sessionStatusIndex = Builders<TaskData>.IndexKeys.Combine(sessionIndex,
                                                                  statusIndex);
    var dispatchIndex = Builders<TaskData>.IndexKeys.Hashed(model => model.DispatchId);
    var taskIndex = Builders<TaskData>.IndexKeys.Hashed(model => model.TaskId);

    var indexModels = new CreateIndexModel<TaskData>[]
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
                        new(dispatchIndex,
                            new()
                            {
                              Name = nameof(dispatchIndex),
                            }),
                        new(taskIndex,
                            new()
                            {
                              Name = nameof(taskIndex),
                            }),
                      };

    await collection.Indexes.CreateManyAsync(sessionHandle,
                                              indexModels);
  }
}
