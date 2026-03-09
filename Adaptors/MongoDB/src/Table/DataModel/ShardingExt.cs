// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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
using System.Threading;
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
                                             "_id", options.UseHashed
                                                      ? "hashed"
                                                      : 1
                                           },
                                         }
                                },
                              };

    var shardingCommand = new BsonDocumentCommand<BsonDocument>(new BsonDocument(shardingCommandDict));
    await adminDb.RunCommandAsync(shardingCommand)
                 .ConfigureAwait(false);
  }


  /// <summary>
  ///   Determines whether the specified MongoDB collection is sharded.
  /// </summary>
  /// <remarks>
  ///   This method queries the collection's statistics to determine if sharding is enabled. The
  ///   operation requires appropriate database permissions to run the 'collStats' command.
  /// </remarks>
  /// <typeparam name="T">The type of the documents in the collection.</typeparam>
  /// <param name="collection">The MongoDB collection to check for sharding. Cannot be null.</param>
  /// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
  /// <returns>
  ///   A task that represents the asynchronous operation. The task result contains <see langword="true" /> if the
  ///   collection is sharded; otherwise, <see langword="false" />.
  /// </returns>
  public static async Task<bool> IsShardedAsync<T>(this IMongoCollection<T> collection,
                                                   CancellationToken        cancellationToken)
  {
    var stats = await collection.Database.RunCommandAsync<BsonDocument>(new BsonDocument("collStats",
                                                                                         collection.CollectionNamespace.CollectionName),
                                                                        cancellationToken: cancellationToken)
                                .ConfigureAwait(false);
    return stats.GetValue("sharded",
                          false)
                .AsBoolean;
  }
}
