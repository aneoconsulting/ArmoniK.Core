// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY, without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

namespace ArmoniK.Core.Common.Auth.Authorization.Permissions;

/// <summary>
///   Ownership permission scopes
/// </summary>
public static class PermissionScope
{
  /// <summary>
  ///   Permission Scope when it has access to resources of all owners
  /// </summary>
  public const string AllUsersScope = "all";

  /// <summary>
  ///   Permission Scope when it has only access to resources of the user which created it
  /// </summary>
  public const string SelfScope = "self";

  /// <summary>
  ///   Default permission scope
  /// </summary>
  public const string Default = "";
}
