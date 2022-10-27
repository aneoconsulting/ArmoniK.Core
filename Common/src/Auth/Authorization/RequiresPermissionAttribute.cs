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

using System;

using ArmoniK.Core.Common.Auth.Authorization.Permissions;

using Microsoft.AspNetCore.Authorization;

namespace ArmoniK.Core.Common.Auth.Authorization;

/// <summary>
///   Function attribute defining the authorization policy name for the function
/// </summary>
public class RequiresPermissionAttribute : AuthorizeAttribute
{
  /// <summary>
  ///   Prefix of the authorization policy
  /// </summary>
  public const string PolicyPrefix = "RequiresPermission:";

  private Permission? permission_;

  /// <summary>
  ///   Creates the method attribute for the given category and method
  /// </summary>
  /// <param name="category">Category of the attribute, usually the service</param>
  /// <param name="function">Function guarded by the attribute, usually the method name</param>
  public RequiresPermissionAttribute(Type   category,
                                     string function)
    => Permission = new Permission(ServicesPermissions.FromType(category),
                                   function);

  /// <summary>
  ///   Get or set the permission required by the method
  /// </summary>
  public Permission? Permission
  {
    get => permission_;
    set
    {
      Policy      = $"{PolicyPrefix}{value}";
      permission_ = new Permission(Policy[PolicyPrefix.Length..]);
    }
  }
}
