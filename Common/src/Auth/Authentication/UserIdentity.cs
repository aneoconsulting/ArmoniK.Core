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
using System.Linq;
using System.Security.Claims;

using ArmoniK.Core.Common.Auth.Authorization.Permissions;

namespace ArmoniK.Core.Common.Auth.Authentication;

/// <summary>
///   User identity. Used in the authentication middleware
/// </summary>
public class UserIdentity : ClaimsIdentity
{
  /// <summary>
  ///   Creates a user identity used in authentication
  /// </summary>
  /// <param name="userAuth">Result of the authentication</param>
  /// <param name="authenticationType">Scheme by which the user is authenticated</param>
  public UserIdentity(UserAuthenticationResult userAuth,
                      string?                  authenticationType)
    : base(ClaimsFromUserAuthenticationResult(userAuth),
           authenticationType)
  {
    UserId   = userAuth.Id;
    UserName = userAuth.Username;
    Roles    = new HashSet<string>(userAuth.Roles);
    Permissions = userAuth.Permissions.Select(perm => new Permission(perm))
                          .ToArray();
  }

  /// <summary>
  ///   Username
  /// </summary>
  public string UserName { get; set; }

  /// <summary>
  ///   User Roles
  /// </summary>
  public HashSet<string> Roles { get; set; }

  /// <summary>
  ///   User Permissions
  /// </summary>
  public Permission[] Permissions { get; set; }

  /// <summary>
  ///   User Id
  /// </summary>
  public string UserId { get; set; }

  /// <summary>
  ///   Transforms a UserAuthenticationResult into a list of claims to be used in an ClaimsIdentity
  /// </summary>
  /// <param name="userAuth">UserAuthenticationResult corresponding to the user</param>
  /// <returns>List of claims corresponding to the UserAuthenticationResult</returns>
  public static IEnumerable<Claim> ClaimsFromUserAuthenticationResult(UserAuthenticationResult userAuth)
  {
    // Transform the list of permissions into a list of claims 
    var claims = userAuth.Permissions.Select(perm => new Permission(perm).Claim);

    // Add the roles of the user. This is done by adding claims with type ClaimTypes.Role and with a value corresponding to the role.
    // This claim will be used when ClaimPrincipal.IsInRole is called.
    claims = claims.Concat(userAuth.Roles.Select(r => new Claim(ClaimTypes.Role,
                                                                r)));

    // Add the name of the user. This is done by adding claims with type ClaimTypes.Name and with a value corresponding to the username.
    // This claim will be used when ClaimPrincipal.Name is called.
    claims = claims.Append(new Claim(ClaimTypes.Name,
                                     userAuth.Username));
    return claims.ToList();
  }
}
