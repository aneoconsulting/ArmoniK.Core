// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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
using System.Linq;

namespace ArmoniK.Core.Authorization;

/// <summary>
///   A named group of permissions, granted as a whole
/// </summary>
/// <remarks>
///   Every set is defined by the configuration, none is shipped with ArmoniK: versions running the same configuration
///   store the same sets, whatever their catalogue
/// </remarks>
public sealed class PermissionSet
{
  /// <summary>Name standing for every permission of the catalogue, expanded when the set is read</summary>
  public const string Everything = "*";

  /// <summary>
  ///   Build a permission set
  /// </summary>
  /// <param name="name">Name of the set</param>
  /// <param name="version">Version of the content of the set</param>
  /// <param name="permissions">Permissions of the set</param>
  public PermissionSet(string                  name,
                       int                     version,
                       IEnumerable<Permission> permissions)
    : this(name,
           version,
           permissions.ToHashSet(),
           null)
  {
  }

  private PermissionSet(string               name,
                        int                  version,
                        HashSet<Permission>  permissions,
                        IEnumerable<string>? names)
  {
    if (string.IsNullOrWhiteSpace(name))
    {
      throw new ArgumentException("A permission set needs a name",
                                  nameof(name));
    }

    Name        = name;
    Version     = version;
    Permissions = permissions;
    Names = (names ?? permissions.Select(permission => permission.Name)).Distinct()
                                                                        .Order(StringComparer.Ordinal)
                                                                        .ToList();
  }

  /// <summary>Name of the set</summary>
  public string Name { get; }

  /// <summary>Version of the content of the set</summary>
  public int Version { get; }

  /// <summary>Permissions of the set</summary>
  public IReadOnlySet<Permission> Permissions { get; }

  /// <summary>
  ///   Names the set was defined with, in ordinal order: <see cref="Everything" /> is kept as such, so that the set is
  ///   stored the same by every version, whatever its catalogue
  /// </summary>
  public IReadOnlyList<string> Names { get; }

  /// <summary>
  ///   Build a permission set from permission names
  /// </summary>
  /// <param name="name">Name of the set</param>
  /// <param name="version">Version of the content of the set</param>
  /// <param name="permissions">Canonical names of the permissions, or <see cref="Everything" /></param>
  /// <returns>The permission set</returns>
  /// <exception cref="InvalidPermissionSetException">A name is not in the catalogue</exception>
  public static PermissionSet FromNames(string              name,
                                        int                 version,
                                        IEnumerable<string> permissions)
  {
    var set = FromNames(name,
                        version,
                        permissions,
                        out var unknown);

    if (unknown.Count != 0)
    {
      throw new
        InvalidPermissionSetException($"Permission set '{name}' names permissions that are not in the catalogue: {string.Join(", ", unknown.Select(u => $"'{u}'"))}");
    }

    return set;
  }

  /// <summary>
  ///   Build a permission set from permission names, leaving out the names that are not in the catalogue
  /// </summary>
  /// <remarks>
  ///   For a set as stored: written by a later version, it may name permissions this one does not know, which it could
  ///   not enforce anyway. Leaving them out only withholds them, while leaving the whole set out would refuse every grant
  ///   naming it.
  /// </remarks>
  /// <param name="name">Name of the set</param>
  /// <param name="version">Version of the content of the set</param>
  /// <param name="permissions">Canonical names of the permissions, or <see cref="Everything" /></param>
  /// <param name="unknown">Names left out</param>
  /// <returns>The permission set</returns>
  public static PermissionSet FromNames(string                    name,
                                        int                       version,
                                        IEnumerable<string>       permissions,
                                        out IReadOnlyList<string> unknown)
  {
    var names   = permissions.ToList();
    var parsed  = new HashSet<Permission>();
    var missing = new List<string>();

    foreach (var permission in names)
    {
      if (permission == Everything)
      {
        parsed.UnionWith(Permission.All);
      }
      else if (Permission.TryParse(permission,
                                   out var p))
      {
        parsed.Add(p);
      }
      else
      {
        missing.Add(permission);
      }
    }

    unknown = missing;
    return new PermissionSet(name,
                             version,
                             parsed,
                             names);
  }

  /// <inheritdoc />
  public override string ToString()
    => $"{Name}@{Version}";
}
