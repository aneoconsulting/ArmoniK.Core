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

using System.Threading;

using ArmoniK.Core.Injection;

using JetBrains.Annotations;

using MongoDB.Driver;

namespace ArmoniK.Adapters.MongoDB.Common
{
  [PublicAPI]
  public class MongoCollectionProvider<TDataModel>
    : ProviderBase<IMongoCollection<TDataModel>>
    where TDataModel : IMongoDataModel<TDataModel>, new()
  {
    public MongoCollectionProvider(Options.MongoDB   options,
                                   SessionProvider   sessionProvider,
                                   IMongoDatabase    mongoDatabase,
                                   CancellationToken cancellationToken = default) :
      base(async () =>
      {
        var model = new TDataModel();
        try
        {
          await mongoDatabase.CreateCollectionAsync(model.CollectionName,
                                                    new CreateCollectionOptions<TDataModel>
                                                    {
                                                      ExpireAfter = options.DataRetention,
                                                    },
                                                    cancellationToken);
        }
        catch (MongoCommandException)
        {
        }

        var output  = mongoDatabase.GetCollection<TDataModel>(model.CollectionName);
        var session = await sessionProvider.GetAsync();
        try
        {
          await model.InitializeIndexesAsync(session,
                                             output);
        }
        catch (MongoCommandException)
        {
        }

        return output;
      })
    {
    }
  }
}