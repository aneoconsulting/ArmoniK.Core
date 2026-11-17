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

using System.Linq;
using System.Threading.Tasks;

using MongoDB.Bson;
using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB.Tests;

/// <summary>
///   Inspects the cursors open on the MongoDB server
/// </summary>
internal static class MongoCursors
{
  /// <summary>
  ///   Count the cursors of the given database still open on the server
  /// </summary>
  /// <param name="database">Database whose cursors are counted</param>
  /// <returns>
  ///   The number of open cursors, idle or running a getMore
  /// </returns>
  public static async Task<int> CountOpen(IMongoDatabase database)
  {
    var admin = database.Client.GetDatabase("admin");
    var pipeline = new[]
                   {
                     new BsonDocument("$currentOp",
                                      new BsonDocument
                                      {
                                        {
                                          "idleCursors", true
                                        },
                                        {
                                          "allUsers", true
                                        },
                                      }),
                     new BsonDocument("$match",
                                      new BsonDocument("ns",
                                                       new BsonRegularExpression($"^{database.DatabaseNamespace.DatabaseName}\\."))),
                   };

    var ops = await admin.AggregateAsync<BsonDocument>(pipeline)
                         .ConfigureAwait(false);
    var cursors = await ops.ToListAsync()
                           .ConfigureAwait(false);
    return cursors.Count(op => op.Contains("cursor"));
  }
}
