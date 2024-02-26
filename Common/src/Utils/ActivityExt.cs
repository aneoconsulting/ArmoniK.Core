// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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

namespace ArmoniK.Core.Common.Utils;

public static class ActivityExt
{
  /// <summary>
  ///   Combines <see cref="Activity.SetTag" /> and <see cref="Activity.SetBaggage" />
  /// </summary>
  /// <param name="activity">Activity to which set tag and baggage</param>
  /// <param name="key">Key of the tag/baggage</param>
  /// <param name="value">Value of the tag/baggage</param>
  /// <returns>
  ///   The given activity for chaining purposes
  /// </returns>
  public static Activity? SetTagAndBaggage(this Activity? activity,
                                           string         key,
                                           string         value)
  {
    activity?.SetBaggage(key,
                         value);
    activity?.SetTag(key,
                     value);
    return activity;
  }
}
