// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
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

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Common.Storage;

using MongoDB.Bson.Serialization;
using MongoDB.Driver;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Adapters.MongoDB.Table.DataModel;



public record DispatchDataModelMapping : IMongoDataModelMapping<DispatchHandler>
{
  static DispatchDataModelMapping()
  {
    if (!BsonClassMap.IsClassMapRegistered(typeof(DispatchHandler)))
      BsonClassMap.RegisterClassMap<DispatchHandler>(cm =>
                                                     {
                                                       cm.MapIdProperty(nameof(DispatchHandler.Id)).SetIsRequired(true);
                                                       cm.MapProperty(nameof(DispatchHandler.TaskId)).SetIsRequired(true);
                                                       cm.MapProperty(nameof(DispatchHandler.Attempt)).SetIsRequired(true);
                                                       cm.MapProperty(nameof(DispatchHandler.TimeToLive)).SetIsRequired(true);
                                                       cm.MapProperty(nameof(DispatchHandler.Statuses)).SetIgnoreIfDefault(true).SetDefaultValue(Enumerable.Empty<KeyValuePair<TaskStatus, DateTime>>());
                                                       cm.MapProperty(nameof(DispatchHandler.CreationDate)).SetIsRequired(true);
                                                       cm.MapProperty(nameof(DispatchHandler.SessionId)).SetIsRequired(true);
                                                       cm.SetIgnoreExtraElements(true);
                                                     });

    if(!BsonClassMap.IsClassMapRegistered(typeof(StatusTime)))
      BsonClassMap.RegisterClassMap<StatusTime>(cm =>
                                                          {
                                                            cm.MapProperty(nameof(StatusTime.Date)).SetIsRequired(true);
                                                            cm.MapProperty(nameof(StatusTime.Status)).SetIsRequired(true);
                                                            cm.MapProperty(nameof(StatusTime.Details)).SetIgnoreIfDefault(true);
                                                            cm.SetIgnoreExtraElements(true);
                                                          });
  }


  /// <inheritdoc />
  public string CollectionName => nameof(DispatchHandler);

  /// <inheritdoc />
  public Task InitializeIndexesAsync(IClientSessionHandle sessionHandle, IMongoCollection<DispatchHandler> collection)
  {
    var sessionIndex = Builders<DispatchHandler>.IndexKeys.Text(model => model.SessionId);
    var taskIndex    = Builders<DispatchHandler>.IndexKeys.Text(model => model.TaskId);

    var indexModels = new CreateIndexModel<DispatchHandler>[]
                      {
                        new(sessionIndex,
                            new()
                            {
                              Name = nameof(sessionIndex),
                            }),
                        new(taskIndex,
                            new()
                            {
                              Name = nameof(taskIndex),
                            }),
                      };

    return collection.Indexes.CreateManyAsync(sessionHandle,
                                              indexModels);
  }
}
