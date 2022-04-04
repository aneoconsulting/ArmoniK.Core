// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
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
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ArmoniK.Core.Common.Utils;

public static class TaskExt
{
  public static Task<T[]> WhenAll<T>(this IEnumerable<Task<T>> enumerable) => Task.WhenAll(enumerable);
  public static Task      WhenAll(this    IEnumerable<Task>    enumerable) => Task.WhenAll(enumerable);

  public static async Task<List<T>> ToListAsync<T>(this Task<IEnumerable<T>> enumerableTask) => (await enumerableTask).ToList();
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
  public static IDisposable Create(Action action) => new DisposableImpl(action);

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
}

public static class AsyncDisposable
{
  public static IAsyncDisposable Create(Func<ValueTask> action) => new AsyncDisposableImpl(action);

  private class AsyncDisposableImpl : IAsyncDisposable
  {
    private readonly Func<ValueTask> action_;

    public AsyncDisposableImpl(Func<ValueTask> action) => action_ = action;

    /// <inheritdoc />
    public ValueTask DisposeAsync() => action_();
  }
}