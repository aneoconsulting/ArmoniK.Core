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

#define ARMONIK_TEST_QUEUE

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using JetBrains.Annotations;

namespace ArmoniK.Core.Common.Pollster;

public abstract class TaskQueueBase
{
#if ARMONIK_TEST_QUEUE
  [PublicAPI]
  public int MinDelay;

  [PublicAPI]
  public int MaxDelay;
#endif

  private readonly Queue<Exception>                  exceptions_ = new();
  private          TaskCompletionSource<TaskHandler> sendTcs_    = new();
  private          TaskCompletionSource              ackTcs_     = new();

  /// <summary>
  ///   Create an instance
  /// </summary>
  public TaskQueueBase()
  {
  }

  /// <summary>
  ///   Put a handler in the queue
  /// </summary>
  /// <param name="handler">the handler to insert</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  public async Task WriteAsync(TaskHandler       handler,
                               TimeSpan          timeout,
                               CancellationToken cancellationToken)
  {
    var sendTcs = sendTcs_;

    await IntroduceRandomDelayInTestsAsync();

    var ackTcs = ackTcs_;

    await IntroduceRandomDelayInTestsAsync();

    sendTcs.SetResult(handler);

    await IntroduceRandomDelayInTestsAsync();

    try
    {
      await ackTcs.Task.WaitAsync(timeout,
                                  cancellationToken)
                  .ConfigureAwait(false);
    }
    catch (OperationCanceledException)
    {
      // If CAS failed, it means the reader has already reset the TCS
      // So we must wait for it to finish
      var previousTcs = Interlocked.CompareExchange(ref sendTcs_,
                                                    new TaskCompletionSource<TaskHandler>(),
                                                    sendTcs);

      await IntroduceRandomDelayInTestsAsync();

      if (ReferenceEquals(previousTcs,
                          sendTcs))
      {
        throw;
      }

      await ackTcs.Task.ConfigureAwait(false);
    }

    await IntroduceRandomDelayInTestsAsync();

    // If CAS fails, it means that Reader has been closed, so there is no need to replace it
    Interlocked.CompareExchange(ref ackTcs_,
                                new TaskCompletionSource(),
                                ackTcs);
  }

  /// <summary>
  ///   Retrieve an handler
  /// </summary>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  public async Task<TaskHandler> ReadAsync(TimeSpan          timeout,
                                           CancellationToken cancellationToken)
  {
    // Set up a timer to wait for maximum timeout, even if the writer times out just at the wrong moment
    using var cts = timeout.Ticks switch
                    {
                      < 0 => null,
                      0   => new CancellationTokenSource(0),
                      _   => CancellationTokenSource.CreateLinkedTokenSource(cancellationToken),
                    };

    if (timeout.Ticks > 0)
    {
      cts?.CancelAfter(timeout);
    }

    if (cts is not null)
    {
      cancellationToken = cts.Token;
    }

    TaskCompletionSource<TaskHandler> sendTcs,
                                      previousTcs = sendTcs_;
    TaskHandler taskHandler;

    do
    {
      sendTcs = previousTcs;
      await IntroduceRandomDelayInTestsAsync();

      taskHandler = await sendTcs.Task.WaitAsync(cancellationToken)
                                 .ConfigureAwait(false);

      await IntroduceRandomDelayInTestsAsync();

      // If CAS fails, it means that Writer has timed-out
      // So we must continue to wait on the new TCS without resetting the timeout
      previousTcs = Interlocked.CompareExchange(ref sendTcs_,
                                                new TaskCompletionSource<TaskHandler>(),
                                                sendTcs);
    } while (!ReferenceEquals(sendTcs,
                              previousTcs));

    await IntroduceRandomDelayInTestsAsync();

    var ackTcs = ackTcs_;

    await IntroduceRandomDelayInTestsAsync();

    ackTcs.SetResult();

    return taskHandler;
  }

  /// <summary>
  ///   Close Queue
  /// </summary>
  public void CloseReader()
  {
    var ex  = new ChannelClosedException("Writer has been closed");
    var tcs = new TaskCompletionSource();
    tcs.SetException(ex);

    tcs = Interlocked.Exchange(ref ackTcs_,
                               tcs);

    IntroduceRandomDelayInTests();

    tcs.TrySetException(ex);
  }

  /// <summary>
  ///   Close Queue
  /// </summary>
  public void CloseWriter()
  {
    var ex  = new ChannelClosedException("Writer has been closed");
    var tcs = new TaskCompletionSource<TaskHandler>();
    tcs.SetException(ex);

    tcs = Interlocked.Exchange(ref sendTcs_,
                               tcs);

    IntroduceRandomDelayInTests();

    tcs.TrySetException(ex);
  }

  /// <summary>
  ///   Add an exception in the internal exception list
  /// </summary>
  /// <param name="e">the exception to add</param>
  public void AddException(Exception e)
    => exceptions_.Enqueue(e);

  /// <summary>
  ///   Get and remove an exception from the internal list of exception
  /// </summary>
  /// <param name="e">the exception to return</param>
  /// <returns>
  ///   Whether there is an exception in the internal list
  /// </returns>
  public bool RemoveException([MaybeNullWhen(false)] out Exception e)
  {
    var r = exceptions_.Count > 0;

    e = r
          ? exceptions_.Dequeue()
          : null;

    return r;
  }

  [MethodImpl(MethodImplOptions.AggressiveInlining)]
  private ConfiguredValueTaskAwaitable IntroduceRandomDelayInTestsAsync()
#if ARMONIK_TEST_QUEUE
    => new ValueTask(Task.Delay(Random.Shared.Next(MinDelay,
                                                   MaxDelay))).ConfigureAwait(false);
#else
  => new ValueTask().ConfigureAwait(false);
#endif

  [MethodImpl(MethodImplOptions.AggressiveInlining)]
  private void IntroduceRandomDelayInTests()
  {
#if ARMONIK_TEST_QUEUE
    Thread.Sleep(Random.Shared.Next(MinDelay,
                                    MaxDelay));
#endif
  }
}
