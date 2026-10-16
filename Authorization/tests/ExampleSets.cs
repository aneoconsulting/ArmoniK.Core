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

using System.Collections.Generic;
using System.Linq;

namespace ArmoniK.Core.Authorization.Tests;

/// <summary>
///   The permission sets of the access model, as a deployment defines them in its configuration: none is shipped
/// </summary>
internal static class ExampleSets
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

  /// <summary>Every permission</summary>
  public static PermissionSet ClusterAdmin { get; } = PermissionSet.FromNames("cluster-admin",
                                                                       1,
                                                                       [PermissionSet.Everything]);

  /// <summary>All the example sets</summary>
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
