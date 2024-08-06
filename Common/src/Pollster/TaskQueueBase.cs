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
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace ArmoniK.Core.Common.Pollster;

public abstract class TaskQueueBase
{
  private readonly Queue<Exception>                                           exceptions_ = new();
  private          TaskCompletionSource<(TaskHandler, TaskCompletionSource)>? tcs_        = new();

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
    var tcs = tcs_;
    if (tcs is null)
    {
      throw new ChannelClosedException("Reader has been closed");
    }

    var returnTcs = new TaskCompletionSource();
    tcs.SetResult((handler, returnTcs));

    if (tcs_ is null)
    {
      throw new ChannelClosedException("Reader has been closed");
    }

    try
    {
      await returnTcs.Task.WaitAsync(timeout,
                                     cancellationToken)
                     .ConfigureAwait(false);
    }
    catch (OperationCanceledException)
    {
      var oldTcs = Interlocked.CompareExchange(ref tcs_,
                                               new TaskCompletionSource<(TaskHandler, TaskCompletionSource)>(),
                                               tcs);
      if (ReferenceEquals(oldTcs,
                          tcs))
      {
        throw;
      }

      await returnTcs.Task.ConfigureAwait(false);
    }
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
    var tcs = tcs_;
    if (tcs is null)
    {
      throw new ChannelClosedException("Writer has been closed");
    }

    var (handler, returnTcs) = await tcs.Task.WaitAsync(timeout,
                                                        cancellationToken)
                                        .ConfigureAwait(false);

    Interlocked.CompareExchange(ref tcs_,
                                new TaskCompletionSource<(TaskHandler, TaskCompletionSource)>(),
                                tcs);

    returnTcs.SetResult();

    return handler;
  }

  /// <summary>
  ///   Close Queue
  /// </summary>
  public void CloseReader()
  {
    var task = Interlocked.Exchange(ref tcs_,
                                    null)
                          ?.Task;

    if (task is not null && task.IsCompletedSuccessfully)
    {
      var (_, returnTcs) = task.GetAwaiter()
                               .GetResult();

      returnTcs.SetException(new ChannelClosedException("Writer has been closed"));
    }
  }

  /// <summary>
  ///   Close Queue
  /// </summary>
  public void CloseWriter()
  {
    var tcs = Interlocked.Exchange(ref tcs_,
                                   null);

    tcs?.SetException(new ChannelClosedException("Writer has been closed"));
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
}
