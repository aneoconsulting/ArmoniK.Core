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

using System.Collections.Generic;
using System.Threading.Tasks;

using MongoDB.Bson;
using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB.Table.DataModel;

/// <summary>
///   Extension of <see cref="IClientSessionHandle" /> to shard collections
/// </summary>
public static class ShardingExt
{
  /// <summary>
  ///   Shard a collection using its id as a sharding key.
  /// </summary>
  /// <param name="sessionHandle">Handle to the client session.</param>
  /// <param name="options">Options of MongoDB.</param>
  /// <param name="collectionName">Name of the collection to shard.</param>
  public static async Task ShardCollection(this IClientSessionHandle sessionHandle,
                                           Options.MongoDB           options,
                                           string                    collectionName)
  {
    var adminDb = sessionHandle.Client.GetDatabase("admin");
    var shardingCommandDict = new Dictionary<string, object>
                              {
                                {
                                  "shardCollection", $"{options.DatabaseName}.{collectionName}"
                                },
                                {
                                  "key", new Dictionary<string, object>
                                         {
                                           {
                                             "_id", "hashed"
                                           },
                                         }
                                },
                              };

    var shardingCommand = new BsonDocumentCommand<BsonDocument>(new BsonDocument(shardingCommandDict));
    await adminDb.RunCommandAsync(shardingCommand)
                 .ConfigureAwait(false);
  }
}
