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

using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB.Queue;

public class QueueMessageModelMapping : IMongoDataModelMapping<QueueMessageModelMapping>
{
  public const string Collection = "Queue";

  [BsonId(IdGenerator = typeof(StringCombGuidGenerator))]
  public string MessageId { get; set; }

  [BsonRequired]
  [BsonElement]
  public string TaskId { get; set; }

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
  public string CollectionName { get; } = Collection;

  /// <inheritdoc />
  public Task InitializeIndexesAsync(IClientSessionHandle                       sessionHandle,
                                     IMongoCollection<QueueMessageModelMapping> collection)
  {
    var messageIdIndex  = Builders<QueueMessageModelMapping>.IndexKeys.Text(model => model.MessageId);
    var ownerIdIndex    = Builders<QueueMessageModelMapping>.IndexKeys.Text(model => model.OwnerId);
    var submissionIndex = Builders<QueueMessageModelMapping>.IndexKeys.Ascending(model => model.SubmissionDate);
    var priorityIndex   = Builders<QueueMessageModelMapping>.IndexKeys.Descending(model => model.Priority);
    var ownedUntilIndex = Builders<QueueMessageModelMapping>.IndexKeys.Text(model => model.OwnedUntil);
    var pullIndex = Builders<QueueMessageModelMapping>.IndexKeys.Combine(priorityIndex,
                                                                         submissionIndex);
    var pullIndex2 = Builders<QueueMessageModelMapping>.IndexKeys.Combine(priorityIndex,
                                                                          submissionIndex,
                                                                          ownedUntilIndex);
    var lockedIndex = Builders<QueueMessageModelMapping>.IndexKeys.Combine(messageIdIndex,
                                                                           ownerIdIndex);


    var indexModels = new CreateIndexModel<QueueMessageModelMapping>[]
                      {
                        new(pullIndex,
                            new CreateIndexOptions
                            {
                              Name = nameof(pullIndex),
                            }),
                        new(pullIndex2,
                            new CreateIndexOptions
                            {
                              Name = nameof(pullIndex2),
                            }),
                        new(lockedIndex,
                            new CreateIndexOptions
                            {
                              Name   = nameof(lockedIndex),
                              Unique = true,
                            }),
                        new(messageIdIndex,
                            new CreateIndexOptions
                            {
                              Name   = nameof(messageIdIndex),
                              Unique = true,
                            }),
                      };

    return collection.Indexes.CreateManyAsync(sessionHandle,
                                              indexModels);
  }
}
