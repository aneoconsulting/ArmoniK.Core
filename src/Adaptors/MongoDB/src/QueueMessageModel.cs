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

using ArmoniK.Core.gRPC.V1;

using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace ArmoniK.Adapters.MongoDB
{
  public class QueueMessageModel : IMongoDataModel<QueueMessageModel>
  {
    [BsonId(IdGenerator = typeof(StringCombGuidGenerator))]
    public string MessageId { get; set; }

    [BsonRequired]
    [BsonElement]
    public TaskId TaskId { get; set; }

    [BsonElement]
    public string OwnerId { get; set; }

    [BsonRequired]
    [BsonElement]
    public DateTime SubmissionDate { get; set; }

    [BsonElement]
    public int Priority { get; set; }

    [BsonIgnoreIfDefault]
    [BsonDateTimeOptions(Kind = DateTimeKind.Utc,
                         DateOnly = false)]
    public DateTime OwnedUntil { get; set; }

    /// <inheritdoc />
    public string CollectionName { get; } = "Queue";

    /// <inheritdoc />
    public Task InitializeIndexesAsync(
      IClientSessionHandle                sessionHandle,
      IMongoCollection<QueueMessageModel> collection)
    {
      var messageIdIndex  = Builders<QueueMessageModel>.IndexKeys.Text(model => model.MessageId);
      var ownerIdIndex    = Builders<QueueMessageModel>.IndexKeys.Text(model => model.OwnerId);
      var submissionIndex = Builders<QueueMessageModel>.IndexKeys.Ascending(model => model.SubmissionDate);
      var priorityIndex   = Builders<QueueMessageModel>.IndexKeys.Descending(model => model.Priority);
      var ownedUntilIndex = Builders<QueueMessageModel>.IndexKeys.Text(model => model.OwnedUntil);
      var pullIndex = Builders<QueueMessageModel>.IndexKeys.Combine(priorityIndex,
                                                                    submissionIndex);
      var pullIndex2 = Builders<QueueMessageModel>.IndexKeys.Combine(priorityIndex,
                                                                    submissionIndex,
                                                                    ownedUntilIndex);
      var lockedIndex = Builders<QueueMessageModel>.IndexKeys.Combine(messageIdIndex,
                                                                      ownerIdIndex);


      var indexModels = new CreateIndexModel<QueueMessageModel>[]
                        {
                          new(pullIndex,
                              new CreateIndexOptions { Name = nameof(pullIndex) }),
                          new(pullIndex2,
                              new CreateIndexOptions { Name = nameof(pullIndex2) }),
                          new(lockedIndex,
                              new CreateIndexOptions { Name = nameof(lockedIndex), Unique = true }),
                          new(messageIdIndex,
                              new CreateIndexOptions { Name = nameof(messageIdIndex), Unique = true }),
                        };

      return collection.Indexes.CreateManyAsync(sessionHandle,
                                                indexModels);
    }
  }
}
