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
using System.Collections.Immutable;
using System.Linq;
using System.Reflection;
using System.Security.Claims;

using ArmoniK.Core.Common.gRPC.Services;

namespace ArmoniK.Core.Common.Auth.Authorization;

public static class Permissions
{
  // Ownership permission scopes
  public const string AllUsersScope = "all";
  public const string SelfScope     = "self";
  public const string Default       = "";

  // Services
  public const string General   = "General";
  public const string Submitter = "Submitter";

  // Constants
  public const char Separator = ':';

  // Base permissions
  public static readonly Permission Impersonate = new(General,
                                                      nameof(Impersonate));

  // Permissions list
  public static readonly ImmutableList<Permission> PermissionList = GetPermissionList();

  private static ImmutableList<Permission> GetPermissionList()
  {
    var permissions = typeof(GrpcSubmitterService).GetMethods()
                                                  .SelectMany(mInfo => mInfo.GetCustomAttributes<RequiresPermissionAttribute>())
                                                  .Select(a => a.Permission!)
                                                  .ToList();
    permissions.Add(Impersonate);
    return permissions.ToImmutableList();
  }

  /// <summary>
  /// Class used to store a permission
  /// </summary>
  public class Permission
  {
    public readonly Claim  Claim;
    public readonly string Name;
    public readonly string Service;
    public readonly string Target;

    /// <summary>
    /// Constructs the permission from a string with format :
    /// Service:Name
    /// or
    /// Service:Name:Target
    /// </summary>
    /// <param name="actionString">String representation of the permission</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if the string is not of the right format</exception>
    /// <exception cref="ArgumentException">Thrown if Service or Name is null or whitespace</exception>
    public Permission(string actionString)
    {
      var parts = actionString.Split(Separator);
      if (parts.Length is < 2 or > 3)
      {
        throw new ArgumentOutOfRangeException("Wrong number of parts in action policy string " + actionString);
      }

      Service = string.IsNullOrWhiteSpace(parts[0])
                  ? throw new ArgumentException("Service of permission is null or whitespace")
                  : parts[0];
      Name = string.IsNullOrWhiteSpace(parts[1])
               ? throw new ArgumentException("Name of permission is null or whitespace")
               : parts[1];
      Target = parts.Length == 3
                 ? parts[2]
                 : Default;
      Claim = new Claim(ToBasePermission(),
                        Target);
    }

    public Permission(string service,
                      string name)
      : this(service,
             name,
             Default)
    {
    }

    public Permission(string  service,
                      string  name,
                      string? target)
    {
      Service = service;
      Name    = name;
      Target  = target ?? Default;
      Claim = new Claim(ToBasePermission(),
                        Target);
    }
    
    public override string ToString()
    {
      var action = ToBasePermission();
      if (!string.IsNullOrEmpty(Target))
      {
        action += Separator + Target;
      }

      return action;
    }

    /// <summary>
    /// Base permission string of the permission (service:name)
    /// </summary>
    /// <returns>The base permission string of this permission, no target</returns>
    public string ToBasePermission()
      => Service + Separator + Name;
  }
}
