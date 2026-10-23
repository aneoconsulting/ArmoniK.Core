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

using ArmoniK.Core.Common.Injection.Options;

namespace ArmoniK.Core.Common.Tests.Helpers;

/// <summary>
///   The permission sets of the access model, as a deployment defines them in its configuration: none is shipped
/// </summary>
internal static class ExamplePermissionSets
{
  private static readonly string[] SessionView = ["session:read", "task:read", "data:read", "data:download", "event:watch"];

  /// <summary>The sets, as configuration options</summary>
  public static List<PermissionSetOption> Options
    =>
    [
      Set("session-view",
          SessionView),
      Set("session-create",
          "session:create",
          "partition:read"),
      Set("session-own",
          SessionView.Concat([
                       "task:submit", "session:share", "session:delegate", "session:transfer", "session:pause", "session:resume", "session:stop-submission",
                       "session:cancel", "session:close", "session:purge", "session:delete", "task:cancel", "data:declare", "data:provide", "data:delete",
                     ])
                     .ToArray()),
      Set("session-supervise",
          "session:read",
          "session:pause",
          "session:resume",
          "session:stop-submission",
          "session:cancel",
          "task:read",
          "task:cancel"),
      Set("access-admin",
          "grant:read",
          "grant:manage",
          "permission-set:manage"),
      Set("cluster-admin",
          "*"),
    ];

  /// <summary>
  ///   The sets, followed by others
  /// </summary>
  /// <param name="others">Sets defined next to them</param>
  /// <returns>The sets, as configuration options</returns>
  public static List<PermissionSetOption> With(params IEnumerable<PermissionSetOption> others)
    => Options.Concat(others)
              .ToList();

  private static PermissionSetOption Set(string          name,
                                         params string[] permissions)
    => new()
       {
         Name        = name,
         Permissions = permissions.ToList(),
       };
}
