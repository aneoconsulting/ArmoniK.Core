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
using System.Threading;

using ArmoniK.Core.Common.Injection;

using JetBrains.Annotations;

using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB.Common;

[PublicAPI]
public class MongoCollectionProvider<TData, TModelMapping> : ProviderBase<IMongoCollection<TData>>
  where TModelMapping : IMongoDataModelMapping<TData>, new()
{
  public MongoCollectionProvider(Options.MongoDB   options,
                                 SessionProvider   sessionProvider,
                                 IMongoDatabase    mongoDatabase,
                                 CancellationToken cancellationToken = default)
    : base(async () =>
           {
             var model = new TModelMapping();
             try
             {
               await mongoDatabase.CreateCollectionAsync(model.CollectionName,
                                                         new CreateCollectionOptions<TData>
                                                         {
                                                           ExpireAfter = options.DataRetention,
                                                         },
                                                         cancellationToken)
                                  .ConfigureAwait(false);
             }
             catch (MongoCommandException)
             {
             }

             var output = mongoDatabase.GetCollection<TData>(model.CollectionName);
             var session = sessionProvider.Get();
             try
             {
               await model.InitializeIndexesAsync(session,
                                                  output)
                          .ConfigureAwait(false);
             }
             catch (MongoCommandException)
             {
             }

             return output;
           })
  {
    if (options.DataRetention == TimeSpan.Zero)
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"{nameof(Options.MongoDB.DataRetention)} is not defined.");
    }
  }
}
