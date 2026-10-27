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
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace ArmoniK.Core.Common.Tests.Helpers;

internal static class WatcherStreamExt
{
  /// <summary>
  ///   Yield the given events then stay open until cancellation, like a real watcher does
  /// </summary>
  public static async IAsyncEnumerable<T> UntilCancelled<T>(this                     IEnumerable<T>    events,
                                                            [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    foreach (var e in events)
    {
      yield return e;
    }

    await Task.Delay(Timeout.Infinite,
                     cancellationToken)
              .ConfigureAwait(false);
  }
}
