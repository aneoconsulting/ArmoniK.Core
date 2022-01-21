// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
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

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Common.Storage;

using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB.Table.DataModel;

public class ResultDataModel : IMongoDataModel<ResultDataModel>, IResult
{
  public const string Collection = nameof(ResultDataModel);

  static ResultDataModel()
  {
    if (!BsonClassMap.IsClassMapRegistered(typeof(ResultDataModel)))
      BsonClassMap.RegisterClassMap<ResultDataModel>(cm =>
                                                     {
                                                       cm.MapIdProperty(nameof(Id)).SetIsRequired(true);
                                                       cm.MapProperty(nameof(SessionId)).SetIsRequired(true);
                                                       cm.MapProperty(nameof(Key)).SetIsRequired(true);
                                                       cm.MapProperty(nameof(Owner)).SetIsRequired(true);
                                                       cm.MapProperty(nameof(IsResultAvailable)).SetIsRequired(true);
                                                       cm.MapProperty(nameof(Data)).SetIgnoreIfDefault(true);
                                                       cm.MapProperty(nameof(CreationDate)).SetIsRequired(true);
                                                       cm.SetIgnoreExtraElements(true);
                                                     });
  }

  /// <inheritdoc />
  public string CollectionName => Collection;

  public string Id => $"{SessionId}.{Key}";

  /// <inheritdoc />
  public string SessionId { get; set; }

  /// <inheritdoc />
  public string Key { get; set; }

  /// <inheritdoc />
  public string Owner { get; set; }

  /// <inheritdoc />
  public bool IsResultAvailable { get; set; }

  /// <inheritdoc />
  public byte[] Data { get; set; }

  /// <inheritdoc />
  public DateTime CreationDate { get; set; } = DateTime.UtcNow;



  /// <inheritdoc />
  public Task InitializeIndexesAsync(IClientSessionHandle sessionHandle, IMongoCollection<ResultDataModel> collection) => throw new NotImplementedException();

}
