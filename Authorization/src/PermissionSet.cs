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
public sealed class PermissionSet
{
  /// <summary>Name of the break-glass set holding every permission</summary>
  public const string ClusterAdminName = "cluster-admin";

  /// <summary>
  ///   Build a permission set
  /// </summary>
  /// <param name="name">Name of the set</param>
  /// <param name="version">Version of the content of the set</param>
  /// <param name="permissions">Permissions of the set</param>
  /// <param name="immutable">Whether the set can be edited or deleted</param>
  public PermissionSet(string                  name,
                       int                     version,
                       IEnumerable<Permission> permissions,
                       bool                    immutable = false)
  {
    if (string.IsNullOrWhiteSpace(name))
    {
      throw new ArgumentException("A permission set needs a name",
                                  nameof(name));
    }

    Name        = name;
    Version     = version;
    Permissions = permissions.ToHashSet();
    Immutable   = immutable;
  }

  /// <summary>Name of the set</summary>
  public string Name { get; }

  /// <summary>Version of the content of the set</summary>
  public int Version { get; }

  /// <summary>Permissions of the set</summary>
  public IReadOnlySet<Permission> Permissions { get; }

  /// <summary>Whether the set can be edited or deleted</summary>
  public bool Immutable { get; }

  /// <summary>
  ///   Build a permission set from permission names
  /// </summary>
  /// <param name="name">Name of the set</param>
  /// <param name="version">Version of the content of the set</param>
  /// <param name="permissions">Canonical names of the permissions</param>
  /// <returns>The permission set</returns>
  /// <exception cref="InvalidPermissionSetException">A name is not in the catalogue</exception>
  public static PermissionSet FromNames(string              name,
                                        int                 version,
                                        IEnumerable<string> permissions)
  {
    var parsed  = new List<Permission>();
    var unknown = new List<string>();

    foreach (var permission in permissions)
    {
      if (Permission.TryParse(permission,
                              out var p))
      {
        parsed.Add(p);
      }
      else
      {
        unknown.Add(permission);
      }
    }

    if (unknown.Count != 0)
    {
      throw new InvalidPermissionSetException($"Permission set '{name}' names permissions that are not in the catalogue: {string.Join(", ", unknown.Select(u => $"'{u}'"))}");
    }

    return new PermissionSet(name,
                             version,
                             parsed);
  }

  /// <summary>Whether two sets hold the same permissions</summary>
  /// <param name="other">The other set</param>
  /// <returns>Whether the contents are equal, whatever the names and versions</returns>
  public bool SameContentAs(PermissionSet other)
    => Permissions.SetEquals(other.Permissions);

  /// <inheritdoc />
  public override string ToString()
    => $"{Name}@{Version}";
}

/// <summary>
///   Permission sets shipped with ArmoniK, as starting points that an organization may redefine, except
///   <see cref="ClusterAdmin" />
/// </summary>
public static class ShippedPermissionSets
{
  /// <summary>The whole of a session, its tasks and its data included, read only</summary>
  public static PermissionSet SessionView { get; } = new("session-view",
                                                         1,
                                                         SessionViewPermissions);

  /// <summary>Creating a session, which is what a partition condition gates</summary>
  public static PermissionSet SessionCreate { get; } = new("session-create",
                                                           1,
                                                           new[]
                                                           {
                                                             Permission.SessionCreate,
                                                             Permission.PartitionRead,
                                                           });

  /// <summary>Everything an owner does with their own work, except <c>data:import</c></summary>
  public static PermissionSet SessionOwn { get; } = new("session-own",
                                                        1,
                                                        SessionViewPermissions.Concat(new[]
                                                                                      {
                                                                                        Permission.TaskSubmit,
                                                                                        Permission.SessionShare,
                                                                                        Permission.SessionDelegate,
                                                                                        Permission.SessionTransfer,
                                                                                        Permission.SessionPause,
                                                                                        Permission.SessionResume,
                                                                                        Permission.SessionStopSubmission,
                                                                                        Permission.SessionCancel,
                                                                                        Permission.SessionClose,
                                                                                        Permission.SessionPurge,
                                                                                        Permission.SessionDelete,
                                                                                        Permission.TaskCancel,
                                                                                        Permission.DataDeclare,
                                                                                        Permission.DataProvide,
                                                                                        Permission.DataDelete,
                                                                                      }));

  /// <summary>Following and stopping work, without any data permission</summary>
  public static PermissionSet SessionSupervise { get; } = new("session-supervise",
                                                              1,
                                                              new[]
                                                              {
                                                                Permission.SessionRead,
                                                                Permission.SessionPause,
                                                                Permission.SessionResume,
                                                                Permission.SessionStopSubmission,
                                                                Permission.SessionCancel,
                                                                Permission.TaskRead,
                                                                Permission.TaskCancel,
                                                              });

  /// <summary>Administration of access</summary>
  public static PermissionSet AccessAdmin { get; } = new("access-admin",
                                                         1,
                                                         new[]
                                                         {
                                                           Permission.GrantRead,
                                                           Permission.GrantManage,
                                                           Permission.PermissionSetManage,
                                                         });

  /// <summary>Every permission. Immutable and non-deletable</summary>
  public static PermissionSet ClusterAdmin { get; } = new(PermissionSet.ClusterAdminName,
                                                          1,
                                                          Permission.All,
                                                          true);

  /// <summary>All shipped sets</summary>
  public static IReadOnlyList<PermissionSet> All { get; } = new[]
                                                            {
                                                              SessionView,
                                                              SessionCreate,
                                                              SessionOwn,
                                                              SessionSupervise,
                                                              AccessAdmin,
                                                              ClusterAdmin,
                                                            };

  private static Permission[] SessionViewPermissions
    => new[]
       {
         Permission.SessionRead,
         Permission.TaskRead,
         Permission.DataRead,
         Permission.DataDownload,
         Permission.EventWatch,
       };
}
