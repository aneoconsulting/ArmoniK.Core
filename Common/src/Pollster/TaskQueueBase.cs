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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

[assembly: InternalsVisibleTo("ArmoniK.Core.Common.Tests")]

namespace ArmoniK.Core.Common.Pollster;

internal interface IRandomDelayForTest
{
  public ConfiguredValueTaskAwaitable WaitAsync();

  public void Wait();
}

internal struct RandomDelayForTest : IRandomDelayForTest
{
  [MethodImpl(MethodImplOptions.AggressiveInlining)]
  public ConfiguredValueTaskAwaitable WaitAsync()
    => new ValueTask().ConfigureAwait(false);

  [MethodImpl(MethodImplOptions.AggressiveInlining)]
  public void Wait()
  {
  }
}

internal class TaskQueueBase<T>
  where T : IRandomDelayForTest, new()
{
  private readonly SemaphoreSlim readerSem_;
  private readonly SemaphoreSlim writerSem_;
  internal         T             RandomDelay = new();
  private          bool          readerClosed_;
  private          TaskHandler?  taskHandler_;
  private          bool          writerClosed_;

  /// <summary>
  ///   Create an instance
  /// </summary>
  public TaskQueueBase()
  {
    readerSem_ = new SemaphoreSlim(0);
    writerSem_ = new SemaphoreSlim(0);
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
    if (writerClosed_)
    {
      throw new ChannelClosedException("Writer has been closed");
    }

    taskHandler_ = handler;

    await RandomDelay.WaitAsync();

    readerSem_.Release();

    await RandomDelay.WaitAsync();

    try
    {
      if (!await writerSem_.WaitAsync(timeout,
                                      cancellationToken)
                           .ConfigureAwait(false))
      {
        throw new TimeoutException("No consumer in the allotted time");
      }
    }
    catch
    {
      // Try to acquire the reader semaphore to reset the task handler
      // If the acquire fails, it means the reader has acquired it after all
      if (await readerSem_.WaitAsync(0,
                                     CancellationToken.None)
                          .ConfigureAwait(false))
      {
        // Writer semaphore will be released soon, if not already released
        await writerSem_.WaitAsync(CancellationToken.None)
                        .ConfigureAwait(false);
        taskHandler_ = null;
        throw;
      }
    }

    if (readerClosed_)
    {
      writerSem_.Release();
      throw new ChannelClosedException("Reader has been closed");
    }

    // Acknowledge that write is complete
    readerSem_.Release();
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
    if (readerClosed_)
    {
      throw new ChannelClosedException("Reader has been closed");
    }

    if (!await readerSem_.WaitAsync(timeout,
                                    cancellationToken)
                         .ConfigureAwait(false))
    {
      throw new TimeoutException("No producer in the allotted time");
    }

    await RandomDelay.WaitAsync();

    var handler = taskHandler_!;
    taskHandler_ = null;

    await RandomDelay.WaitAsync();

    if (writerClosed_)
    {
      readerSem_.Release();
      throw new ChannelClosedException("Writer has been closed");
    }

    writerSem_.Release();

    await RandomDelay.WaitAsync();

    // Without this wait, the reader thread could close the channel before
    // the writer being notified that a read has occured.
    // Reader semaphore will be released soon, if not already released
    await readerSem_.WaitAsync(CancellationToken.None)
                    .ConfigureAwait(false);

    return handler;
  }

  /// <summary>
  ///   Close Queue
  /// </summary>
  public void CloseReader()
  {
    Debug.Assert(!readerClosed_);
    readerClosed_ = true;
    writerSem_.Release();
  }

  /// <summary>
  ///   Close Queue
  /// </summary>
  public void CloseWriter()
  {
    Debug.Assert(!writerClosed_);
    writerClosed_ = true;
    readerSem_.Release();
  }
}

public abstract class TaskQueueBase
{
  private readonly TaskQueueBase<RandomDelayForTest> base_ = new();

  private readonly Queue<Exception> exceptions_ = new();

  /// <inheritdoc cref="TaskQueueBase{RandomDelayForTest}.WriteAsync" />
  public Task WriteAsync(TaskHandler       handler,
                         TimeSpan          timeout,
                         CancellationToken cancellationToken)
    => base_.WriteAsync(handler,
                        timeout,
                        cancellationToken);

  /// <inheritdoc cref="TaskQueueBase{RandomDelayForTest}.ReadAsync" />
  public Task<TaskHandler> ReadAsync(TimeSpan          timeout,
                                     CancellationToken cancellationToken)
    => base_.ReadAsync(timeout,
                       cancellationToken);

  /// <inheritdoc cref="TaskQueueBase{RandomDelayForTest}.CloseReader" />
  public void CloseReader()
    => base_.CloseReader();

  /// <inheritdoc cref="TaskQueueBase{RandomDelayForTest}.CloseWriter" />
  public void CloseWriter()
    => base_.CloseWriter();


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
}
