// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ArmoniK.Core.Utils
{
  public static class TaskExt
  {
    public static Task<T[]> WhenAll<T>(this IEnumerable<Task<T>> enumerable) => Task.WhenAll(enumerable);
    public static Task WhenAll(this IEnumerable<Task> enumerable) => Task.WhenAll(enumerable);
  }

  public static class DisposableExt
  {
    public static IAsyncDisposable Merge(this IEnumerable<IAsyncDisposable> disposables)
    {
      return AsyncDisposable.Create(async () => await disposables.Select(async disposable => await disposable.DisposeAsync())
                                                                 .WhenAll());
    }
  }

  public static class Disposable
  {
    private class DisposableImpl : IDisposable
    {
      private readonly Action action_;

      public DisposableImpl(Action action) => action_ = action;

      /// <inheritdoc />
      public void Dispose()
      {
        action_();
      }
    }

    public static IDisposable Create(Action action) => new DisposableImpl(action);
  }

  public static class AsyncDisposable
  {
    private class AsyncDisposableImpl : IAsyncDisposable
    {
      private readonly Func<ValueTask> action_;

      public AsyncDisposableImpl(Func<ValueTask> action) => action_ = action;

      /// <inheritdoc />
      public ValueTask DisposeAsync() => action_();
    }

    public static IAsyncDisposable Create(Func<ValueTask> action) => new AsyncDisposableImpl(action);
  }
}