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

using ArmoniK.Api.gRPC.V1;
using Microsoft.AspNetCore.DataProtection.KeyManagement;

using System;

namespace ArmoniK.Core.Adapters.MongoDB.Table.DataModel;

public record Result(string   SessionId,
                     string   Key,
                     string   OwnerTaskId,
                     string   OriginDispatchId,
                     bool     IsResultAvailable,
                     DateTime CreationDate,
                     byte[]   Data) : Core.Common.Storage.Result(SessionId,
                                                                 Key,
                                                                 OwnerTaskId,
                                                                 OriginDispatchId,
                                                                 IsResultAvailable,
                                                                 CreationDate,
                                                                 Data)
{

  public Result(Core.Common.Storage.Result original) : this(original.SessionId,
                                                            original.Key,
                                                            original.OwnerTaskId,
                                                            original.OriginDispatchId,
                                                            original.IsResultAvailable,
                                                            original.CreationDate,
                                                            original.Data)
  {
  }

  /// <summary>
  /// Database Id of the object. 
  /// </summary>
  public string Id => GenerateId(SessionId, Key);

  public static string GenerateId(string SessionId, string Key) =>
    $"{SessionId}.{Key}";
}
