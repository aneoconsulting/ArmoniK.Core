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
  private readonly Channel<TaskHandler> channel_;


  private readonly Queue<Exception> exceptions_ = new();
  private readonly bool             singleReader_;

  /// <summary>
  ///   Create an instance
  /// </summary>
  /// <param name="singleReader">whether only one reader can retrieve item from the queue</param>
  public TaskQueueBase(bool singleReader)
  {
    singleReader_ = singleReader;
    channel_ = Channel.CreateBounded<TaskHandler>(new BoundedChannelOptions(1)
                                                  {
                                                    Capacity     = 1,
                                                    FullMode     = BoundedChannelFullMode.Wait,
                                                    SingleReader = singleReader,
                                                    SingleWriter = true,
                                                  });
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
                               CancellationToken cancellationToken)
    => await channel_.Writer.WriteAsync(handler,
                                        cancellationToken)
                     .ConfigureAwait(false);

  /// <summary>
  ///   Wait for the availability of the next write.
  ///   Remove and dispose the current handler if timeout expires.
  /// </summary>
  /// <param name="timeout">Timeout before the handler is removed and disposed</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  /// <exception cref="InvalidOperationException">if this method is used when the queue is in single mode</exception>
  public async Task WaitForNextWriteAsync(TimeSpan          timeout,
                                          CancellationToken cancellationToken)
  {
    if (singleReader_)
    {
      throw new InvalidOperationException("Cannot use this method in single reader mode");
    }

    using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    cts.CancelAfter(timeout);

    try
    {
      // block until handler is consumed or token is cancelled
      await channel_.Writer.WaitToWriteAsync(cts.Token)
                    .ConfigureAwait(false);
    }
    catch (Exception e)
    {
      // Consumer took too long to retrieve the handler or there was an unrecoverable error
      // so we remove the handler from the channel and dispose it.
      if (channel_.Reader.TryRead(out var handler))
      {
        await handler.DisposeAsync()
                     .ConfigureAwait(false);
      }

      // if the wait was cancelled because it reached the timeout, we ignore the error
      if (e is not OperationCanceledException || !cts.IsCancellationRequested)
      {
        throw;
      }
    }
  }

  /// <summary>
  ///   Retrieve an handler
  /// </summary>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  public async Task<TaskHandler> ReadAsync(CancellationToken cancellationToken)
    => await channel_.Reader.ReadAsync(cancellationToken)
                     .ConfigureAwait(false);

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
