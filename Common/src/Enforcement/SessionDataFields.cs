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

using ArmoniK.Core.Authorization;
using ArmoniK.Core.Common.Storage;

namespace ArmoniK.Core.Common.Enforcement;

/// <summary>
///   Where the session fields read by conditions live in <see cref="SessionData" />
/// </summary>
public static class SessionDataFields
{
  /// <summary>
  ///   Selectors of the session fields read by conditions
  /// </summary>
  public static SessionFields<SessionData> Fields { get; } = new(session => session.Owner,
                                                                 session => session.CreatedUnder,
                                                                 session => session.SharedWith,
                                                                 session => session.DelegatedTo);

  /// <summary>
  ///   The facts a condition reads from a stored session
  /// </summary>
  /// <param name="session">The stored session</param>
  /// <returns>The facts</returns>
  public static SessionFacts Facts(SessionData session)
    => new(session.Owner,
           ReadOnly(session.CreatedUnder),
           ReadOnly(session.SharedWith),
           ReadOnly(session.DelegatedTo));

  private static IReadOnlyCollection<string> ReadOnly(IList<string> list)
    => list as IReadOnlyCollection<string> ?? list.ToList();
}
