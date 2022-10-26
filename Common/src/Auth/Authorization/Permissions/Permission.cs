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
using System.Security.Claims;

namespace ArmoniK.Core.Common.Auth.Authorization.Permissions
{
  /// <summary>
  ///   Class used to store a permission
  /// </summary>

  public class Permission
  {
    /// <summary>
    ///   Separator used in permission strings
    /// </summary>
    public const char Separator = ':';

    /// <summary>
    ///   C# Claim object equivalent to this permission
    /// </summary>
    public readonly Claim Claim;

    /// <summary>
    ///   Name of the permission, usually the affected method
    /// </summary>
    public readonly string Name;

    /// <summary>
    ///   Service targeted by the permission
    /// </summary>
    public readonly string Service;

    /// <summary>
    ///   target scope of the permission (All, self...)
    /// </summary>
    public readonly string Target;

    /// <summary>
    ///   Constructs the permission from a string with format :
    ///   Service:Name
    ///   or
    ///   Service:Name:Target
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
                 : PermissionScope.Default;
      Claim = new Claim(ToBasePermission(),
                        Target);
    }

    /// <summary>
    ///   Creates a permission with the given service and name, target is
    ///   <value cref="PermissionScope.Default">Default</value>
    /// </summary>
    /// <param name="service">Service</param>
    /// <param name="name">Name</param>
    public Permission(string service,
                      string name)
      : this(service,
             name,
             PermissionScope.Default)
    {
    }

    /// <summary>
    ///   Creates a permission with the given service, name and target
    /// </summary>
    /// <param name="service">Service</param>
    /// <param name="name">Name</param>
    /// <param name="target">
    ///   Target, if null defaults to
    ///   <value cref="PermissionScope.Default">Default</value>
    /// </param>
    public Permission(string  service,
                      string  name,
                      string? target)
    {
      Service = service;
      Name    = name;
      Target  = target ?? PermissionScope.Default;
      Claim = new Claim(ToBasePermission(),
                        Target);
    }

    /// <summary>
    ///   String representation of the permission
    /// </summary>
    /// <returns>Returns the full permission string</returns>
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
    ///   Base permission string of the permission (service:name)
    /// </summary>
    /// <returns>The base permission string of this permission, no target</returns>
    public string ToBasePermission()
      => Service + Separator + Name;
  }
}
