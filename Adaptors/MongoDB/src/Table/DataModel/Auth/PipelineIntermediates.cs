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
//   D. Brasseur       <dbrasseur@aneo.fr>
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

using System.Collections.Generic;

using ArmoniK.Core.Common.Auth.Authentication;

using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace ArmoniK.Core.Adapters.MongoDB.Table.DataModel.Auth;

/// <summary>
///   MongoDB mapping for intermediate object, internally used
/// </summary>
/// <param name="AuthId">Certificate ID</param>
/// <param name="UserId">User ID</param>
/// <param name="CN">Common Name</param>
/// <param name="Fingerprint">Certificate Fingerprint</param>
/// <param name="UserData">List of users that have the id UserId</param>
[BsonIgnoreExtraElements]
public record AuthDataAfterLookup([property: BsonId]
                                  ObjectId AuthId,
                                  ObjectId   UserId,
                                  string     CN,
                                  string     Fingerprint,
                                  UserData[] UserData);

/// <summary>
///   MongoDB mapping for intermediate object, internally used
/// </summary>
/// <param name="UserId">User ID</param>
/// <param name="Username">Username</param>
/// <param name="Roles">List of roles of the user</param>
[BsonIgnoreExtraElements]
public record UserDataAfterLookup([property: BsonId]
                                  ObjectId UserId,
                                  string                Username,
                                  IEnumerable<RoleData> Roles);

/// <summary>
///   Pipeline result using MongoDB syntax
/// </summary>
/// <param name="Id">User Id</param>
/// <param name="Username">Username</param>
/// <param name="Roles">User's roles</param>
/// <param name="Permissions">User's permissions</param>
public record MongoAuthResult([property: BsonId]
                              ObjectId Id,
                              string              Username,
                              IEnumerable<string> Roles,
                              IEnumerable<string> Permissions)
{
  /// <summary>
  ///   Converts this MongoDB pipeline result into a UserAuthenticationResult
  /// </summary>
  /// <returns>UserAuthenticationResult from this object</returns>
  public UserAuthenticationResult ToUserAuthenticationResult()
    => new(IdSerializer.Deserialize(Id),
           Username,
           Roles,
           Permissions);
}
