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

using ArmoniK.Core.Common.Auth.Authorization;

namespace ArmoniK.Core.Common.Auth.Authentication;

public class UserIdentity : ClaimsPrincipal
{
  public string              UserName { get; set; }
  public IEnumerable<string> Roles    { get; set; }

  public IEnumerable<Permissions.Permission> Permissions { get; set; }

  public string UserId { get; set; }


  public UserIdentity(string                              userId,
                      string                              userName,
                      IEnumerable<string>                 roles,
                      IEnumerable<Permissions.Permission> permissions)
    : base(new ClaimsIdentity(permissions.Select(perm => perm.Claim)))
  {
    UserId      = userId;
    UserName    = userName;
    Roles       = roles;
    Permissions = permissions;
  }


  public override bool IsInRole(string role)
    => Roles.Contains(role);

  public override UserIdentity Clone()
    => new(UserId,
           UserName,
           Roles.ToList(),
           Permissions.Select(p => new Permissions.Permission(p.Prefix,
                                                              p.Name,
                                                              p.Suffix))
                      .ToList());
}
