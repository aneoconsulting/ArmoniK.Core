// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Collections.ObjectModel;
using System.Linq;
using System.Reflection;

using ArmoniK.Core.Common.gRPC.Services;

namespace ArmoniK.Core.Common.Auth.Authorization.Permissions;

/// <summary>
///   Permissions related to each service
/// </summary>
public static class ServicesPermissions
{
  public const string Default = "Default";

  public const string All = "*";

  public static readonly ReadOnlyDictionary<Type, string> Type2NameMapping = new(new Dictionary<Type, string>
                                                                                 {
                                                                                   {
                                                                                     typeof(GrpcSubmitterService), "Submitter"
                                                                                   },
                                                                                   {
                                                                                     typeof(GrpcSessionsService), "Sessions"
                                                                                   },
                                                                                   {
                                                                                     typeof(GrpcTasksService), "Tasks"
                                                                                   },
                                                                                   {
                                                                                     typeof(GrpcResultsService), "Results"
                                                                                   },
                                                                                   {
                                                                                     typeof(GrpcApplicationsService), "Applications"
                                                                                   },
                                                                                   {
                                                                                     typeof(GrpcEventsService), "Events"
                                                                                   },
                                                                                   {
                                                                                     typeof(GeneralService), "General"
                                                                                   },
                                                                                   {
                                                                                     typeof(GrpcAuthService), "Authentication"
                                                                                   },
                                                                                   {
                                                                                     typeof(GrpcPartitionsService), "Partitions"
                                                                                   },
                                                                                   {
                                                                                     typeof(GrpcVersionsService), "Versions"
                                                                                   },
                                                                                 });

  /// <summary>
  ///   Dictionary with the list of permissions for each service
  /// </summary>
  public static readonly ImmutableDictionary<string, ImmutableList<Permission>> PermissionsLists = GetPermissionList();

  /// <summary>
  ///   Determines the service name from the given service type
  /// </summary>
  /// <param name="t">Type of the service</param>
  /// <returns>Name of the service used in permissions</returns>
  public static string FromType(Type t)
    => Type2NameMapping.GetValueOrDefault(t,
                                          Default);

  private static ImmutableDictionary<string, ImmutableList<Permission>> GetPermissionList()
  {
    var servicePermissions = new Dictionary<string, ImmutableList<Permission>>();

    foreach (var (t, name) in Type2NameMapping)
    {
      servicePermissions[name] = t.GetMethods()
                                  .SelectMany(mInfo => mInfo.GetCustomAttributes<RequiresPermissionAttribute>())
                                  .Select(a => a.Permission!)
                                  .ToImmutableList();
    }

    var allPermissions = new List<Permission>();
    foreach (var (_, perms) in servicePermissions)
    {
      allPermissions.AddRange(perms);
    }

    servicePermissions[All] = allPermissions.ToImmutableList();

    return servicePermissions.ToImmutableDictionary();
  }
}
