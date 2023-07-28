// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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

using System;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Common.Auth.Authentication;

using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB.Table.DataModel.Auth;

/// <summary>
///   MongoDB mapping of the User document
/// </summary>
public class UserDataModelMapping : IMongoDataModelMapping<UserData>
{
  static UserDataModelMapping()
  {
    if (!BsonClassMap.IsClassMapRegistered(typeof(UserData)))
    {
      BsonClassMap.RegisterClassMap<UserData>(cm =>
                                              {
                                                cm.MapIdProperty(nameof(UserData.UserId))
                                                  .SetIsRequired(true)
                                                  .SetSerializer(IdSerializer.Instance);
                                                cm.MapProperty(nameof(UserData.Username))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(UserData.Roles))
                                                  .SetIgnoreIfDefault(true)
                                                  .SetSerializer(IdArraySerializer.Instance)
                                                  .SetDefaultValue(Array.Empty<string>());
                                                cm.SetIgnoreExtraElements(true);
                                                cm.MapCreator(model => new UserData(model.UserId,
                                                                                    model.Username,
                                                                                    model.Roles));
                                              });
    }
  }

  /// <inheritdoc />
  public string CollectionName
    => nameof(UserData);

  /// <inheritdoc />
  public async Task InitializeIndexesAsync(IClientSessionHandle       sessionHandle,
                                           IMongoCollection<UserData> collection,
                                           Options.MongoDB            options)
  {
    var indexModels = new[]
                      {
                        IndexHelper.CreateTextIndex<UserData>(model => model.Username,
                                                              true),
                        IndexHelper.CreateHashedIndex<UserData>(model => model.Username),
                      };
    await collection.Indexes.CreateManyAsync(sessionHandle,
                                             indexModels)
                    .ConfigureAwait(false);
  }
}
