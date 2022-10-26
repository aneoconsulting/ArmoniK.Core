// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-$CURRENT_YEAR$. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
//   D. Brasseur       <dbrasseur@aneo.fr>
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

namespace ArmoniK.Core.Common.Auth.Authorization.Permissions
{
  public class ServicesPermissions
  {
    private static readonly ReadOnlyDictionary<Type, string> Type2NameMapping = new(new Dictionary<Type, string>
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
                                                                                         typeof(GeneralService), "General"
                                                                                       },
                                                                                     });

    public static readonly ImmutableDictionary<string, ImmutableList<Permission>> PermissionsLists = GetPermissionList();
    public static string FromType(Type t)
    {
      return Type2NameMapping.GetValueOrDefault(t, Default);
    }

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

    public const string Default = "Default";

    public const string All = "*";
  }
}
