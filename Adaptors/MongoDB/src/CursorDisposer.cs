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
using System.Threading;
using System.Threading.Tasks;

namespace ArmoniK.Core.Adapters.MongoDB;

/// <summary>
///   Disposes cursors outside of the thread pool.
/// </summary>
/// <remarks>
///   <para>
///     Disposing a cursor that is still open on the server sends a killCursors synchronously: the driver has no
///     asynchronous way to close a change stream. When many subscriptions end together, these blocking calls
///     occupy the threads of the thread pool, which starves the asynchronous operations they wait for, such as
///     opening a connection. The killCursors then time out and are abandoned silently, leaving the cursors open
///     on the server for as long as their session lives.
///   </para>
///   <para>
///     The cursors are therefore disposed on dedicated threads, at most <see cref="MaxConcurrentDisposals" /> at
///     a time, so that they never block the thread pool.
///   </para>
/// </remarks>
internal static class CursorDisposer
{
  /// <summary>
  ///   Maximum number of cursors disposed at the same time
  /// </summary>
  internal const int MaxConcurrentDisposals = 8;

  private static readonly SemaphoreSlim Semaphore = new(MaxConcurrentDisposals,
                                                        MaxConcurrentDisposals);

  /// <summary>
  ///   Dispose the given cursor on a dedicated thread
  /// </summary>
  /// <param name="cursor">Cursor to dispose</param>
  /// <returns>
  ///   Task representing the asynchronous operation, completed once the cursor is disposed
  /// </returns>
  public static async Task DisposeAsync(IDisposable cursor)
  {
    await Semaphore.WaitAsync()
                   .ConfigureAwait(false);
    try
    {
      await Task.Factory.StartNew(cursor.Dispose,
                                  CancellationToken.None,
                                  TaskCreationOptions.LongRunning,
                                  TaskScheduler.Default)
                .ConfigureAwait(false);
    }
    finally
    {
      Semaphore.Release();
    }
  }
}
