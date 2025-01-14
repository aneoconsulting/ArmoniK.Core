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

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ArmoniK.Core.Common.Utils;

public static class ActivitySourceExt
{
  /// <summary>
  ///   Starts an activity with the given context while propagating tags and baggage from the parent
  /// </summary>
  /// <param name="source">Activity source from which create the new activity</param>
  /// <param name="context">Context to pass to the created activity</param>
  /// <param name="parent">Activity parent with tags and baggage to propagate</param>
  /// <param name="kind">Activity kind</param>
  /// <param name="name">Name of the created activity, defaults to caller name</param>
  /// <returns>
  ///   The started activity
  /// </returns>
  public static Activity? StartActivityFromParent(this ActivitySource       source,
                                                  ActivityContext           context,
                                                  Activity?                 parent,
                                                  ActivityKind              kind = ActivityKind.Internal,
                                                  [CallerMemberName] string name = "")
  {
    var activity = source.StartActivity(name,
                                        kind,
                                        context,
                                        parent?.TagObjects);
    if (activity is null || parent is null)
    {
      return activity;
    }

    foreach (var pair in parent.Baggage)
    {
      activity.SetBaggage(pair.Key,
                          pair.Value);
    }

    return activity;
  }
}
