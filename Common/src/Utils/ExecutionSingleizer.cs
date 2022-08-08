// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-$CURRENT_YEAR$. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
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

namespace ArmoniK.Core.Common.Utils;

public class ExecutionSingleizer<T> : IDisposable
{
  private sealed class Handle : IDisposable
  {
    public CancellationTokenSource CancellationTokenSource { get; init; }
    public Task<T>                 InnerTask               { get; init; }
    public int                     Waiters;

    public Handle()
    {
      CancellationTokenSource = new CancellationTokenSource();
      CancellationTokenSource.Cancel();

      InnerTask = Task.FromCanceled<T>(CancellationTokenSource.Token);
    }

    public void Dispose()
    {
      CancellationTokenSource.Dispose();
      InnerTask.Dispose();
    }
  }

  private Handle handle_ = new();


  public async Task<T> Call(Func<CancellationToken, Task<T>> func,
                            CancellationToken                cancellationToken = default)
  {
    var currentHandle = handle_;
    var task          = currentHandle.InnerTask;
    if (task.IsCompleted)
    {
      var cts         = new CancellationTokenSource();
      var delayedTask = new Task<Task<T>>(() => func(cts.Token));

      var newHandle = new Handle
                      {
                        CancellationTokenSource = cts,
                        InnerTask               = delayedTask.Unwrap(),
                      };

      var previousHandle = Interlocked.CompareExchange(ref handle_,
                                                       newHandle,
                                                       currentHandle);
      if (ReferenceEquals(previousHandle,
                          currentHandle))
      {
        delayedTask.Start();
        currentHandle = newHandle;
      }
      else
      {
        newHandle.Dispose();
        currentHandle = previousHandle;
      }

      task = currentHandle.InnerTask;
    }

    Interlocked.Increment(ref currentHandle.Waiters);
    try
    {
      var tcs = new TaskCompletionSource<T>();
      cancellationToken.Register(() => tcs.SetCanceled(cancellationToken));
      return await Task.WhenAny(task,
                                tcs.Task)
                       .Unwrap()
                       .ConfigureAwait(false);
    }
    finally
    {
      var i = Interlocked.Decrement(ref currentHandle.Waiters);
      if (i == 0)
      {
        currentHandle.CancellationTokenSource.Cancel();

        // The task might not have finished yet. If that is the case, let the GC do the job
        if (currentHandle.InnerTask.IsCompleted)
        {
          // Dispose of the Handle (and therefore the underlying task) here is fine:
          // https://devblogs.microsoft.com/pfxteam/do-i-need-to-dispose-of-tasks/
          currentHandle.Dispose();
        }
      }
    }
  }

  public void Dispose()
    => handle_.Dispose();
}
