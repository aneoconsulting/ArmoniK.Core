// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace ArmoniK.Core.Common.Utils;

/// <summary>
///   Limit the access to a function call to a single thread,
///   all the others waiting the result from the one actually doing the call.
/// </summary>
/// <typeparam name="T">Type of the return object</typeparam>
public class ExecutionSingleizer<T> : IDisposable
{
  private readonly long   tickValidity_;
  private          Handle handle_ = new();


  /// <summary>
  ///   Allow initialization of <see cref="ExecutionSingleizer" />
  /// </summary>
  /// <param name="timeValidity">Results from the execution will be in cache during timeValidity</param>
  public ExecutionSingleizer(TimeSpan timeValidity = default)
    => tickValidity_ = (long)Math.Ceiling(Stopwatch.Frequency * timeValidity.TotalSeconds);

  /// <inheritdoc />
  public void Dispose()
    => handle_.Dispose();

  /// <summary>
  ///   Call the asynchronous function func.
  ///   If another thread is already computing func, just wait for the result.
  ///   The actual call to func is cancelled only when all callers have cancelled it.
  /// </summary>
  /// <param name="func">Function to call</param>
  /// <param name="cancellationToken">Token to cancel the call</param>
  /// <returns>
  ///   Result of func
  /// </returns>
  public async Task<T> Call(Func<CancellationToken, Task<T>> func,
                            CancellationToken                cancellationToken = default)
  {
    // Read the handle_ reference to have a stable view of it
    var currentHandle = handle_;

    // If there is no waiters, the task is complete (success or failed), and no thread is currently running it.
    // We therefore need to call func again if the data has expired.
    if (currentHandle.Waiters == 0 && Stopwatch.GetTimestamp() > currentHandle.ValidUntil)
    {
      // Prepare new handle, with new cancellation token source and new task
      var cts         = new CancellationTokenSource();
      var delayedTask = new Task<Task<T>>(() => func(cts.Token));

      var newHandle = new Handle
                      {
                        CancellationTokenSource = cts,
                        // Unwrap allows the handle to have a single level Task, instead of a Task<Task<...>>
                        InnerTask = delayedTask.Unwrap(),
                        // Current thread is implicitly waiting for the task
                        Waiters = 1,
                      };

      // Try to store new handle replacing the previous one.
      // Only one thread will succeed here.
      var previousHandle = Interlocked.CompareExchange(ref handle_,
                                                       newHandle,
                                                       currentHandle);

      // Check if thread successfully replaced the handle.
      if (ReferenceEquals(previousHandle,
                          currentHandle))
      {
        // Current thread has won, the others will see this new handle.
        // We can now start the task, other threads will just wait on the result.
        delayedTask.Start();
        currentHandle = newHandle;

        // There is no need increment number of waiters as it has been initialized to 1.
      }
      else
      {
        // The handle as been replaced by another thread, so we can just wait for the task
        // in this new handle to get the result.
        // The handle created by the current thread can be destroyed as it is not used by anything.
        newHandle.Dispose();
        currentHandle = previousHandle;

        // Record current thread as waiting for the task
        Interlocked.Increment(ref currentHandle.Waiters);
      }
    }
    else
    {
      // if the task is not complete, we can just wait its result.
      // Record current thread as waiting for the task.
      Interlocked.Increment(ref currentHandle.Waiters);
    }

    var task = currentHandle.InnerTask;
    // Wait for task.
    try
    {
      // Allow for early exit.
      var tcs = new TaskCompletionSource<T>();

      // Early exit if the current cancellationToken is cancelled.
      cancellationToken.Register(() => tcs.SetCanceled(cancellationToken));

      // Wait for either the task to finish, or the cancellation token to be cancelled.
      return await Task.WhenAny(task,
                                tcs.Task)
                       .Unwrap()
                       .ConfigureAwait(false);
    }
    finally
    {
      // Reset the validity of the result once the result is available.
      // This is done by all threads in order to avoid race condition with the Waiters decrement.
      if (!currentHandle.CancellationTokenSource.IsCancellationRequested)
      {
        currentHandle.ValidUntil = Stopwatch.GetTimestamp() + tickValidity_;
      }

      // Remove the current thread from the list of waiters.
      var i = Interlocked.Decrement(ref currentHandle.Waiters);

      // If the current thread was the last, we can cancel the shared token.
      if (i == 0)
      {
        // If we enter here because the task has completed without errors,
        // cancelling is a no op, therefore, we do not need to check why we went here.
        currentHandle.CancellationTokenSource.Cancel();

        // FIXME: There might be a race condition between the dispose and the cancel here.
        // ManyConcurrentExecutionShouldSucceed fails with:
        //   `System.ObjectDisposedException : The CancellationTokenSource has been disposed.`
        // As soon as we understand where it comes from, we can reenable early dispose.

        //// The task might not have finished yet. If that is the case, let the GC do the job.
        //if (currentHandle.InnerTask.IsCompleted)
        //{
        //  // Dispose of the Handle (and therefore the underlying task) here is fine:
        //  // https://devblogs.microsoft.com/pfxteam/do-i-need-to-dispose-of-tasks/
        //  currentHandle.Dispose();
        //}
      }
    }
  }

  /// <summary>
  ///   This handle stores a Task, a cancellationTokenSource, and a counter.
  ///   This needs to be a class to enable an atomic CAS.
  ///   It cannot be inlined into the parent class.
  /// </summary>
  private sealed class Handle : IDisposable
  {
    /// <summary>
    ///   Specify the timestamp until the data is valid
    /// </summary>
    public long ValidUntil = long.MinValue;

    /// <summary>
    ///   Number of threads waiting for the result.
    /// </summary>
    public int Waiters;

    /// <summary>
    ///   Construct an handle that is cancelled.
    /// </summary>
    public Handle()
    {
      // Create a CancellationTokenSource that is already cancelled.
      CancellationTokenSource = new CancellationTokenSource();
      CancellationTokenSource.Cancel();

      // InnerTask is created cancelled.
      // As a cancelled task is completed, the result of this task will never be read,
      // and another task will always be created instead.
      InnerTask = Task.FromCanceled<T>(CancellationTokenSource.Token);
    }

    /// <summary>
    ///   Shared cancellation token for all the threads waiting on the task.
    /// </summary>
    public CancellationTokenSource CancellationTokenSource { get; init; }

    /// <summary>
    ///   Task that creates the result.
    /// </summary>
    public Task<T> InnerTask { get; init; }

    /// <inheritdoc />
    public void Dispose()
    {
      CancellationTokenSource.Dispose();
      InnerTask.Dispose();
    }
  }
}
