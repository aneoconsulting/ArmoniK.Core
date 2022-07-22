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
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Security.Claims;

using ArmoniK.Core.Common.gRPC.Services;

namespace ArmoniK.Core.Common.Auth.Authorization
{
  public static class Permissions
  {
    public class Permission
    {
      public readonly string  Prefix;
      public readonly string  Name;
      public readonly string? Suffix;
      public readonly Claim   Claim;

      public Permission(string actionString)
      {
        var parts = actionString.Split(Separator);
        if (parts.Length is < 2 or > 3)
        {
          throw new ArgumentOutOfRangeException("Wrong number of parts in action policy string " + actionString);
        }

        Prefix = parts[0];
        Name = parts[1];
        Suffix = parts.Length == 3 ? parts[2] : null;
        Claim = new Claim(ToBasePermission(),
                          Suffix ?? Default);
      }

      public Permission(string prefix,
                    string name)
        : this(prefix,
               name,
               null){}

      public Permission(string  prefix,
                    string  name,
                    string? suffix)
      {
        Prefix = prefix;
        Name = name;
        Suffix = suffix;
        Claim = new Claim(ToBasePermission(),
                          Suffix ?? Default);
      }

      public override string ToString()
      {
        var action = ToBasePermission();
        if (Suffix != null)
        {
          action += Separator + Suffix;
        }

        return action;
      }

      public string ToBasePermission()
      {
        return Prefix + Separator + Name;
      }
       
    }

    public static Permission Parse(string actionName)
    {
      return new Permission(actionName);
    }

    public const char   Separator  = ':';
    public const string AdminScope = "*";
    public const string Default    = "";
    public const string General    = "general";
    public const string Session    = "session";
    public const string Task       = "task";
    public const string Result     = "result";

    public static readonly Permission       Impersonate    = new(General, nameof(Impersonate));
    public static readonly Permission       None           = new("", "");
    public static readonly List<Permission> PermissionList = typeof(GrpcSubmitterService).GetMethods()
                                                                                         .SelectMany(mInfo => mInfo.GetCustomAttributes<RequiresPermissionAttribute>())
                                                                                         .Select(a=>a.Permission)
                                                                                         .ToList();
  }
}
