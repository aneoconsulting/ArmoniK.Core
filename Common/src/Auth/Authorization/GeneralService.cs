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

using ArmoniK.Core.Common.Auth.Authorization.Permissions;

using JetBrains.Annotations;

namespace ArmoniK.Core.Common.Auth.Authorization;

/// <summary>
///   General service
/// </summary>
[PublicAPI]
public static class GeneralService
{
  /// <summary>
  ///   Base permission to allow the user to impersonate
  /// </summary>
  public static readonly Permission Impersonate = new(ServicesPermissions.FromType(typeof(GeneralService)),
                                                      nameof(Impersonate));

  /// <summary>
  ///   Get the impersonate permission
  ///   Used for reflection
  /// </summary>
  /// <returns>Impersonation permission</returns>
  [RequiresPermission(typeof(GeneralService),
                      nameof(Impersonate))]
  public static Permission GetImpersonatePermission()
    => Impersonate;
}
