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

using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;

using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB.Table.DataModel;

public record ResultDataModelMapping : IMongoDataModelMapping<Result>
{
  
  public ResultDataModelMapping()
  {
    if (!BsonClassMap.IsClassMapRegistered(typeof(Core.Common.Storage.Result)))
      BsonClassMap.RegisterClassMap<Core.Common.Storage.Result>(cm =>
                                                                {
                                                                  cm.MapProperty(nameof(Core.Common.Storage.Result.SessionId)).SetIsRequired(true);
                                                                  cm.MapProperty(nameof(Core.Common.Storage.Result.Key)).SetIsRequired(true);
                                                                  cm.MapProperty(nameof(Core.Common.Storage.Result.OwnerTaskId)).SetIsRequired(true);
                                                                  cm.MapProperty(nameof(Core.Common.Storage.Result.OriginDispatchId)).SetIsRequired(true);
                                                                  cm.MapProperty(nameof(Core.Common.Storage.Result.IsResultAvailable)).SetIsRequired(true);
                                                                  cm.MapProperty(nameof(Core.Common.Storage.Result.CreationDate)).SetIsRequired(true);
                                                                  cm.MapProperty(nameof(Core.Common.Storage.Result.Data)).SetIgnoreIfDefault(true).SetDefaultValue(Enumerable.Empty<byte>());
                                                                  cm.MapCreator(model => new(model.SessionId,
                                                                                             model.Key,
                                                                                             model.OwnerTaskId,
                                                                                             model.OriginDispatchId,
                                                                                             model.IsResultAvailable,
                                                                                             model.CreationDate,
                                                                                             model.Data));
                                                                });
    if (!BsonClassMap.IsClassMapRegistered(typeof(Result)))
      BsonClassMap.RegisterClassMap<Result>(cm =>
                                            {

                                              cm.MapIdProperty(nameof(Result.Id));
                                              cm.MapCreator(model => new(model.SessionId,
                                                                         model.Key,
                                                                         model.OwnerTaskId,
                                                                         model.OriginDispatchId,
                                                                         model.IsResultAvailable,
                                                                         model.CreationDate,
                                                                         model.Data));
                                            });
  }


  /// <inheritdoc />
  public string CollectionName => nameof(Result);

  /// <inheritdoc />
  public async Task InitializeIndexesAsync(IClientSessionHandle       sessionHandle,
                                           IMongoCollection<Result> collection)
  {
    var sessionIndex        = Builders<Result>.IndexKeys.Text(model => model.SessionId);
    var ownerTaskIndex      = Builders<Result>.IndexKeys.Text(model => model.OwnerTaskId);
    var originDispatchIndex = Builders<Result>.IndexKeys.Text(model => model.OriginDispatchId);

    var indexModels = new CreateIndexModel<Result>[]
    {
      new(sessionIndex,
          new()
          {
            Name = nameof(sessionIndex),
          }),
      new(ownerTaskIndex,
          new()
          {
            Name = nameof(ownerTaskIndex),
          }),
      new(originDispatchIndex,
          new()
          {
            Name = nameof(originDispatchIndex),
          }),
    };

    await collection.Indexes.CreateManyAsync(sessionHandle,
                                             indexModels);
  }

}
