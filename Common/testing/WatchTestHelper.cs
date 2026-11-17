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

using NUnit.Framework;

namespace ArmoniK.Core.Common.Testing;

/// <summary>
///   Helpers for the watcher tests.
/// </summary>
internal static class WatchTestHelper
{
  /// <summary>
  ///   Maximum time a watch may take to end once it is cancelled
  /// </summary>
  public static readonly TimeSpan CancellationTimeout = TimeSpan.FromSeconds(3);

  /// <summary>
  ///   Reads the given stream in the background until it ends or <paramref name="cancellationToken" /> is cancelled.
  /// </summary>
  /// <typeparam name="T">Type of the events</typeparam>
  /// <param name="stream">Stream of events returned by a watcher</param>
  /// <param name="cancellationToken">Token used to cancel the watch</param>
  /// <returns>
  ///   Task completed when the watch ends
  /// </returns>
  public static Task Consume<T>(IAsyncEnumerable<T> stream,
                                CancellationToken   cancellationToken)
    => Task.Run(async () =>
                {
                  await foreach (var _ in stream.WithCancellation(cancellationToken)
                                                .ConfigureAwait(false))
                  {
                  }
                },
                CancellationToken.None);

  /// <summary>
  ///   Cancels the watches after a short delay, then checks that each of them ends with an
  ///   <see cref="OperationCanceledException" /> within <see cref="CancellationTimeout" />.
  /// </summary>
  /// <param name="cts">Token source of the watches</param>
  /// <param name="watches">Watches started with <see cref="Consume{T}" /></param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  public static async Task AssertEndAfterCancellation(CancellationTokenSource cts,
                                                      params Task[]           watches)
  {
    // Let the watches wait for events before cancelling them
    await Task.Delay(TimeSpan.FromMilliseconds(200))
              .ConfigureAwait(false);

    var stopwatch = Stopwatch.StartNew();
    await cts.CancelAsync()
             .ConfigureAwait(false);

    foreach (var watch in watches)
    {
      Assert.That(() => watch,
                  Throws.InstanceOf<OperationCanceledException>());
    }

    Assert.That(stopwatch.Elapsed,
                Is.LessThan(CancellationTimeout));
  }

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
