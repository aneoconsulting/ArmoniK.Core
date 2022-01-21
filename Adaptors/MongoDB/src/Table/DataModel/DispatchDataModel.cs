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
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Common.Storage;

using MongoDB.Bson.Serialization;
using MongoDB.Driver;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Adapters.MongoDB.Table.DataModel;



public class DispatchDataModel : IMongoDataModel<DispatchDataModel>, IDispatch
{

  public const string Collection = nameof(DispatchDataModel);

  static DispatchDataModel()
  {
    if (!BsonClassMap.IsClassMapRegistered(typeof(DispatchDataModel)))
      BsonClassMap.RegisterClassMap<DispatchDataModel>(cm =>
                                                       {
                                                         cm.MapIdProperty(nameof(Id)).SetIsRequired(true);
                                                         cm.MapProperty(nameof(TaskId)).SetIsRequired(true);
                                                         cm.MapProperty(nameof(Attempt)).SetIsRequired(true);
                                                         cm.MapProperty(nameof(TimeToLive)).SetIsRequired(true);
                                                         cm.MapProperty(nameof(Statuses)).SetIgnoreIfDefault(true).SetDefaultValue(Enumerable.Empty<KeyValuePair<TaskStatus, DateTime>>());
                                                         cm.MapProperty(nameof(CreationDate)).SetIsRequired(true);
                                                         cm.SetIgnoreExtraElements(true);
                                                       });

    if(!BsonClassMap.IsClassMapRegistered(typeof(IDispatch.StatusTime)))
      BsonClassMap.RegisterClassMap<IDispatch.StatusTime>(cm =>
                                                          {
                                                            cm.MapProperty(nameof(IDispatch.StatusTime.Date)).SetIsRequired(true);
                                                            cm.MapProperty(nameof(IDispatch.StatusTime.Status)).SetIsRequired(true);
                                                            cm.MapProperty(nameof(IDispatch.StatusTime.Details)).SetIgnoreIfDefault(true);
                                                            cm.SetIgnoreExtraElements(true);
                                                          });
  }

  /// <inheritdoc />
  public string CollectionName { get; set; }

  /// <inheritdoc />
  public string Id { get; set; }

  /// <inheritdoc />
  public string TaskId { get; set; }

  /// <inheritdoc />
  public int Attempt { get; set; }

  /// <inheritdoc />
  public DateTime TimeToLive { get; set; }

  /// <inheritdoc />
  public IEnumerable<IDispatch.StatusTime> Statuses { get; set; }

  /// <inheritdoc />
  public DateTime CreationDate { get; set; } = DateTime.UtcNow;


  /// <inheritdoc />
  public Task InitializeIndexesAsync(IClientSessionHandle sessionHandle, IMongoCollection<DispatchDataModel> collection) => throw new NotImplementedException();
}
