// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-$CURRENT_YEAR$. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
//   D. Brasseur       <dbrasseur@aneo.fr>
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
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using MongoDB.Bson;
using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB;

public static class ChangeStreamUpdate
{
  public static async Task<IChangeStreamCursor<ChangeStreamDocument<T>>> GetUpdates<T>(IMongoCollection<T>                             collection,
                                                                                       IClientSessionHandle                            sessionHandle,
                                                                                       Expression<Func<ChangeStreamDocument<T>, bool>> filter,
                                                                                       IEnumerable<string>                             fields,
                                                                                       CancellationToken                               cancellationToken = default)
  {
    var match2 = new BsonDocument
                 {
                   {
                     "$addFields", new BsonDocument
                                   {
                                     {
                                       "tmpfields", new BsonDocument
                                                    {
                                                      {
                                                        "$objectToArray", "$updateDescription.updatedFields"
                                                      },
                                                    }
                                     },
                                   }
                   },
                 };
    var match3 = new BsonDocument
                 {
                   {
                     "$match", new BsonDocument
                               {
                                 {
                                   "tmpfields.k", new BsonDocument
                                                  {
                                                    {
                                                      "$in", new BsonArray(fields)
                                                    },
                                                  }
                                 },
                               }
                   },
                 };

    var stage2 = new BsonDocumentPipelineStageDefinition<ChangeStreamDocument<T>, ChangeStreamDocument<T>>(match2);
    var stage3 = new BsonDocumentPipelineStageDefinition<ChangeStreamDocument<T>, ChangeStreamDocument<T>>(match3);

    var pipeline = new EmptyPipelineDefinition<ChangeStreamDocument<T>>().Match(document => document.OperationType == ChangeStreamOperationType.Update)
                                                                         .Match(filter)
                                                                         .AppendStage(stage2)
                                                                         .AppendStage(stage3);

    return await collection.WatchAsync(sessionHandle,
                                       pipeline,
                                       cancellationToken: cancellationToken,
                                       options: new ChangeStreamOptions
                                                {
                                                  FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
                                                })
                           .ConfigureAwait(false);
  }
}
