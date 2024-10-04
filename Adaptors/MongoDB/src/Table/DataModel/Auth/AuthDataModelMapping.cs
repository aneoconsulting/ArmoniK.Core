// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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

using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Injection.Options.Database;

using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB.Table.DataModel.Auth;

/// <summary>
///   MongoDB object mapping for certificate data
/// </summary>
public class AuthDataModelMapping : IMongoDataModelMapping<AuthData>
{
  static AuthDataModelMapping()
  {
    if (!BsonClassMap.IsClassMapRegistered(typeof(AuthData)))
    {
      BsonClassMap.RegisterClassMap<AuthData>(cm =>
                                              {
                                                cm.MapIdProperty(nameof(AuthData.AuthId))
                                                  .SetIsRequired(true)
                                                  .SetSerializer(IdSerializer.Instance);
                                                cm.MapProperty(nameof(AuthData.UserId))
                                                  .SetIsRequired(true)
                                                  .SetSerializer(IdSerializer.Instance);
                                                cm.MapProperty(nameof(AuthData.Cn))
                                                  .SetIsRequired(true);
                                                cm.MapProperty(nameof(AuthData.Fingerprint))
                                                  .SetDefaultValue(BsonNull.Value);
                                                cm.SetIgnoreExtraElements(true);
                                                cm.MapCreator(model => new AuthData(model.AuthId,
                                                                                    model.UserId,
                                                                                    model.Cn,
                                                                                    model.Fingerprint));
                                              });
    }
  }

  /// <inheritdoc />
  public string CollectionName
    => nameof(AuthData);

  /// <inheritdoc />
  public async Task InitializeIndexesAsync(IClientSessionHandle       sessionHandle,
                                           IMongoCollection<AuthData> collection,
                                           Options.MongoDB            options)
  {
    var indexModels = new[]
                      {
                        IndexHelper.CreateUniqueIndex<AuthData>((IndexType.Descending, model => model.Fingerprint),
                                                                (IndexType.Ascending, model => model.Cn)),
                        IndexHelper.CreateHashedIndex<AuthData>(model => model.Fingerprint),
                        IndexHelper.CreateHashedIndex<AuthData>(model => model.UserId),
                      };

    await collection.Indexes.CreateManyAsync(sessionHandle,
                                             indexModels)
                    .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public Task ShardCollectionAsync(IClientSessionHandle sessionHandle,
                                   Options.MongoDB      options)
    => Task.CompletedTask;

  /// <inheritdoc />
  public async Task InitializeCollectionAsync(IClientSessionHandle       sessionHandle,
                                              IMongoCollection<AuthData> collection,
                                              InitDatabase               initDatabase)
  {
    if (initDatabase.Auths.Any())
    {
      await collection.InsertManyAsync(sessionHandle,
                                       initDatabase.Auths)
                      .ConfigureAwait(false);
    }
  }
}
