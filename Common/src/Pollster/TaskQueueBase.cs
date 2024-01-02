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

  public TaskQueueBase(bool singleReader)
    => channel_ = Channel.CreateBounded<TaskHandler>(new BoundedChannelOptions(1)
                                                     {
                                                       Capacity     = 1,
                                                       FullMode     = BoundedChannelFullMode.Wait,
                                                       SingleReader = singleReader,
                                                       SingleWriter = true,
                                                     });

  public async Task WriteAsync(TaskHandler       handler,
                               CancellationToken cancellationToken)
    => await channel_.Writer.WriteAsync(handler,
                                        cancellationToken)
                     .ConfigureAwait(false);

  public async Task WaitForNextWriteAsync(TimeSpan          timeout,
                                          CancellationToken cancellationToken)
  {
    using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    cts.CancelAfter(timeout);

    await channel_.Writer.WaitToWriteAsync(cts.Token)
                  .ConfigureAwait(false);

    if (channel_.Reader.TryRead(out var handler))
    {
      await handler.DisposeAsync()
                   .ConfigureAwait(false);
    }
  }

  public async Task<TaskHandler> ReadAsync(CancellationToken cancellationToken)
    => await channel_.Reader.ReadAsync(cancellationToken)
                     .ConfigureAwait(false);

  public void AddException(Exception e)
    => exceptions_.Enqueue(e);

  public bool RemoveException([MaybeNullWhen(false)] out Exception e)
  {
    var r = exceptions_.Count > 0;

    e = r
          ? exceptions_.Dequeue()
          : null;

    return r;
  }
}
