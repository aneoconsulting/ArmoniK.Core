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
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ArmoniK.Core.Common.Tests.TestBase;

/// <summary>
///   Helpers for the watcher tests.
/// </summary>
internal static class WatchTestHelper
{
  /// <summary>
  ///   Waits until <paramref name="events" /> contains all the <paramref name="expected" /> events, at most
  ///   <paramref name="timeout" />, then cancels the watch after a short delay so that unexpected extra events
  ///   still arrive and fail the assertion.
  /// </summary>
  /// <remarks>
  ///   The watchers deliver the events asynchronously, after a delay that depends on the database
  ///   (change streams, logical replication) and on the machine: cancelling after a fixed delay made the
  ///   tests flaky on slow runners. The watch loop must add to <paramref name="events" /> under a lock on it.
  /// </remarks>
  /// <typeparam name="T">Type of the events</typeparam>
  /// <param name="events">Events received by the watch</param>
  /// <param name="expected">Events the test expects</param>
  /// <param name="cts">Token source that stops the watch</param>
  /// <param name="timeout">Maximum time to wait for the expected events, 10 seconds by default</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  public static async Task StopWhenReceived<T>(List<T>                 events,
                                               IReadOnlyCollection<T>  expected,
                                               CancellationTokenSource cts,
                                               TimeSpan?               timeout = null)
  {
    var watch = Stopwatch.StartNew();
    while (watch.Elapsed < (timeout ?? TimeSpan.FromSeconds(10)))
    {
      lock (events)
      {
        if (expected.All(events.Contains))
        {
          break;
        }
      }

      await Task.Delay(TimeSpan.FromMilliseconds(10))
                .ConfigureAwait(false);
    }

    cts.CancelAfter(TimeSpan.FromMilliseconds(100));
  }
}
