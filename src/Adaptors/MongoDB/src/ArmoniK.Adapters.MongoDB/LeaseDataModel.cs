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
using System.Threading.Tasks;

using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace ArmoniK.Adapters.MongoDB
{
  public class LeaseDataModel : IMongoDataModel<LeaseDataModel>
  {
    [BsonId]
    public string Key { get; set; }

    [BsonElement]
    public string Lock { get; set; }

    [BsonElement]
    [BsonDateTimeOptions(Kind = DateTimeKind.Utc,
                         DateOnly = false)]
    public DateTime ExpiresAt { get; set; }

    /// <inheritdoc />
    public string CollectionName { get; } = "Lease";

    /// <inheritdoc />
    public Task InitializeIndexesAsync(
      IClientSessionHandle             sessionHandle,
      IMongoCollection<LeaseDataModel> collection)
    {
      var keyIndex  = Builders<LeaseDataModel>.IndexKeys.Text(model => model.Key);
      var lockIndex = Builders<LeaseDataModel>.IndexKeys.Text(model => model.Lock);
      var wholeIndex = Builders<LeaseDataModel>.IndexKeys.Combine(keyIndex,
                                                                  lockIndex);

      var indexModels = new CreateIndexModel<LeaseDataModel>[]
                        {
                          new(keyIndex,
                              new CreateIndexOptions { Name = nameof(keyIndex), Unique = true }),
                          new(wholeIndex,
                              new CreateIndexOptions { Name = nameof(wholeIndex), Unique = true }),
                        };

      return collection.Indexes.CreateManyAsync(sessionHandle,
                                                indexModels);
    }
  }
}
