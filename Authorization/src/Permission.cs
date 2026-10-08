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
///   What a permission acts on
/// </summary>
public enum PermissionTarget
{
  /// <summary>A session, or a task or a data object decided on its session</summary>
  Session,

  /// <summary>A partition</summary>
  Partition,

  /// <summary>Grants and permission sets</summary>
  Administration,
}

/// <summary>
///   One action on one kind of resource, from the fixed catalogue of the access model
/// </summary>
public sealed class Permission
{
  private Permission(string           name,
                     PermissionTarget target,
                     bool             putsPartitionInPlay = false,
                     bool             makesSessionVisible = false)
  {
    Name                = name;
    Target              = target;
    PutsPartitionInPlay = putsPartitionInPlay;
    MakesSessionVisible = makesSessionVisible;
  }

  /// <summary>Canonical name, for instance <c>session:read</c></summary>
  public string Name { get; }

  /// <summary>What the permission acts on</summary>
  public PermissionTarget Target { get; }

  /// <summary>Whether a request asking for this permission puts a partition in play</summary>
  public bool PutsPartitionInPlay { get; }

  /// <summary>
  ///   Whether holding this permission on a session makes the session visible, so that a refusal on it is a 403
  ///   rather than a 404
  /// </summary>
  public bool MakesSessionVisible { get; }

  /// <summary>Create a session</summary>
  public static Permission SessionCreate { get; } = new("session:create",
                                                        PermissionTarget.Session,
                                                        putsPartitionInPlay: true);

  /// <summary>View and list sessions</summary>
  public static Permission SessionRead { get; } = Visible("session:read");

  /// <summary>Pause a session</summary>
  public static Permission SessionPause { get; } = Visible("session:pause");

  /// <summary>Resume a paused session</summary>
  public static Permission SessionResume { get; } = Visible("session:resume");

  /// <summary>Stop new tasks being submitted into a session</summary>
  public static Permission SessionStopSubmission { get; } = Visible("session:stop-submission");

  /// <summary>Cancel a session and the tasks still to run</summary>
  public static Permission SessionCancel { get; } = Visible("session:cancel");

  /// <summary>Close a session for good</summary>
  public static Permission SessionClose { get; } = Visible("session:close");

  /// <summary>Add or remove a principal in the session sharedWith list</summary>
  public static Permission SessionShare { get; } = Visible("session:share");

  /// <summary>Add or remove a principal in the session delegatedTo list</summary>
  public static Permission SessionDelegate { get; } = Visible("session:delegate");

  /// <summary>Move ownership of a session</summary>
  public static Permission SessionTransfer { get; } = Visible("session:transfer");

  /// <summary>Delete a session's data</summary>
  public static Permission SessionPurge { get; } = Visible("session:purge");

  /// <summary>Delete a session and its metadata</summary>
  public static Permission SessionDelete { get; } = Visible("session:delete");

  /// <summary>View, list, count tasks</summary>
  public static Permission TaskRead { get; } = Visible("task:read");

  /// <summary>Submit tasks for execution</summary>
  public static Permission TaskSubmit { get; } = Visible("task:submit");

  /// <summary>Cancel tasks</summary>
  public static Permission TaskCancel { get; } = Visible("task:cancel");

  /// <summary>View and list data, never its content</summary>
  public static Permission DataRead { get; } = Visible("data:read");

  /// <summary>Download or stream data</summary>
  public static Permission DataDownload { get; } = Visible("data:download");

  /// <summary>Reserve a result identifier and create its metadata</summary>
  public static Permission DataDeclare { get; } = Visible("data:declare");

  /// <summary>Upload the bytes of a result and register them</summary>
  public static Permission DataProvide { get; } = Visible("data:provide");

  /// <summary>Register a result from a reference the caller supplies</summary>
  public static Permission DataImport { get; } = Visible("data:import");

  /// <summary>Delete data</summary>
  public static Permission DataDelete { get; } = Visible("data:delete");

  /// <summary>View partitions</summary>
  public static Permission PartitionRead { get; } = new("partition:read",
                                                        PermissionTarget.Partition,
                                                        putsPartitionInPlay: true);

  /// <summary>Create a partition</summary>
  public static Permission PartitionCreate { get; } = new("partition:create",
                                                          PermissionTarget.Partition);

  /// <summary>Delete a partition</summary>
  public static Permission PartitionDelete { get; } = new("partition:delete",
                                                          PermissionTarget.Partition);

  /// <summary>Change a partition or its instance count</summary>
  public static Permission PartitionModify { get; } = new("partition:modify",
                                                          PermissionTarget.Partition);

  /// <summary>Subscribe to a session's live events</summary>
  public static Permission EventWatch { get; } = Visible("event:watch");

  /// <summary>View grants, and read another principal's effective permissions</summary>
  public static Permission GrantRead { get; } = new("grant:read",
                                                    PermissionTarget.Administration);

  /// <summary>Create and revoke grants</summary>
  public static Permission GrantManage { get; } = new("grant:manage",
                                                      PermissionTarget.Administration);

  /// <summary>Create, edit and delete permission sets</summary>
  public static Permission PermissionSetManage { get; } = new("permission-set:manage",
                                                              PermissionTarget.Administration);

  /// <summary>The whole catalogue, in the order of the access model</summary>
  public static IReadOnlyList<Permission> All { get; } = new[]
                                                         {
                                                           SessionCreate,
                                                           SessionRead,
                                                           SessionPause,
                                                           SessionResume,
                                                           SessionStopSubmission,
                                                           SessionCancel,
                                                           SessionClose,
                                                           SessionShare,
                                                           SessionDelegate,
                                                           SessionTransfer,
                                                           SessionPurge,
                                                           SessionDelete,
                                                           TaskRead,
                                                           TaskSubmit,
                                                           TaskCancel,
                                                           DataRead,
                                                           DataDownload,
                                                           DataDeclare,
                                                           DataProvide,
                                                           DataImport,
                                                           DataDelete,
                                                           PartitionRead,
                                                           PartitionCreate,
                                                           PartitionDelete,
                                                           PartitionModify,
                                                           EventWatch,
                                                           GrantRead,
                                                           GrantManage,
                                                           PermissionSetManage,
                                                         };

  private static readonly Dictionary<string, Permission> ByName = All.ToDictionary(permission => permission.Name,
                                                                                    StringComparer.Ordinal);

  private static Permission Visible(string name)
    => new(name,
           PermissionTarget.Session,
           makesSessionVisible: true);

  /// <summary>
  ///   Find a permission of the catalogue from its canonical name
  /// </summary>
  /// <param name="name">Canonical name, compared ordinally</param>
  /// <param name="permission">The permission when found</param>
  /// <returns>Whether the name is in the catalogue</returns>
  public static bool TryParse(string                                                      name,
                              [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out Permission? permission)
    => ByName.TryGetValue(name,
                          out permission);

  /// <inheritdoc />
  public override string ToString()
    => Name;
}
