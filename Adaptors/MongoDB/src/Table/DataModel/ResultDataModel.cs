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
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Common.Storage;

using JetBrains.Annotations;

using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB.Table.DataModel;

public record ResultDataModel : Result, IMongoDataModel<ResultDataModel>
{
  static ResultDataModel()
  {
    if (!BsonClassMap.IsClassMapRegistered(typeof(ResultDataModel)))
      BsonClassMap.RegisterClassMap<ResultDataModel>(cm =>
                                                     {
                                                       cm.MapIdProperty(nameof(Id)).SetIsRequired(true);
                                                       cm.MapProperty(nameof(SessionId)).SetIsRequired(true);
                                                       cm.MapProperty(nameof(Key)).SetIsRequired(true);
                                                       cm.MapProperty(nameof(OwnerTaskId)).SetIsRequired(true);
                                                       cm.MapProperty(nameof(OriginDispatchId)).SetIsRequired(true);
                                                       cm.MapProperty(nameof(IsResultAvailable)).SetIsRequired(true);
                                                       cm.MapProperty(nameof(CreationDate)).SetIsRequired(true);
                                                       cm.MapProperty(nameof(Data)).SetIgnoreIfDefault(true).SetDefaultValue(Enumerable.Empty<byte>());
                                                       cm.SetIgnoreExtraElements(true);
                                                     });
  }

  public ResultDataModel(string   sessionId,
                         string   key,
                         string   ownerTaskId,
                         string   originDispatchId,
                         bool     isResultAvailable,
                         DateTime creationDate,
                         byte[]   data)
    : base(sessionId,
           key,
           ownerTaskId,
           originDispatchId,
           isResultAvailable,
           creationDate,
           data)
  {
  }

  public ResultDataModel(Result original) : base(original)
  {
  }

  public ResultDataModel()
    : base(string.Empty,
           string.Empty,
           string.Empty,
           string.Empty,
           false,
           default,
           Array.Empty<byte>())
  {
  }

  /// <summary>
  /// Database Id of the object. 
  /// </summary>
  public string Id => $"{SessionId}.{Key}";

  /// <inheritdoc />
  public string CollectionName => nameof(ResultDataModel);

  /// <inheritdoc />
  public Task InitializeIndexesAsync(IClientSessionHandle sessionHandle, IMongoCollection<ResultDataModel> collection) => throw new NotImplementedException();

}
