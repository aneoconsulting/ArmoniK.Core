// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
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

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using MongoDB.Driver;

namespace ArmoniK.Adapters.MongoDB.Common
{
  public static class AsyncCursorSourceExt
  {
    public static IAsyncEnumerable<T> ToAsyncEnumerable<T>(
      this IAsyncCursorSource<T> asyncCursorSource)
      => new AsyncEnumerableAdapter<T>(asyncCursorSource);

    private class AsyncEnumerableAdapter<T> : IAsyncEnumerable<T>
    {
      private readonly IAsyncCursorSource<T> asyncCursorSource_;

      public AsyncEnumerableAdapter(IAsyncCursorSource<T> asyncCursorSource) => asyncCursorSource_ = asyncCursorSource;

      public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default) =>
        new AsyncEnumeratorAdapter<T>(asyncCursorSource_,
                                      cancellationToken);
    }

    private class AsyncEnumeratorAdapter<T> : IAsyncEnumerator<T>
    {
      private readonly IAsyncCursorSource<T> asyncCursorSource_;
      private readonly CancellationToken     cancellationToken_;
      private          IAsyncCursor<T>       asyncCursor_;
      private          IEnumerator<T>        batchEnumerator_;

      public AsyncEnumeratorAdapter(IAsyncCursorSource<T> asyncCursorSource, CancellationToken cancellationToken = default)
      {
        asyncCursorSource_ = asyncCursorSource;
        cancellationToken_ = cancellationToken;
      }

      public T Current => batchEnumerator_.Current;

      public async ValueTask<bool> MoveNextAsync()
      {
        if (asyncCursor_ == null)
          asyncCursor_ = await asyncCursorSource_.ToCursorAsync(cancellationToken_);

        if (batchEnumerator_ != null &&
            batchEnumerator_.MoveNext())
          return true;

        if (asyncCursor_ != null &&
            await asyncCursor_.MoveNextAsync(cancellationToken_))
        {
          batchEnumerator_?.Dispose();
          batchEnumerator_ = asyncCursor_.Current.GetEnumerator();
          return batchEnumerator_.MoveNext();
        }

        return false;
      }

      public ValueTask DisposeAsync()
      {
        asyncCursor_?.Dispose();
        asyncCursor_ = null;
        return ValueTask.CompletedTask;
      }
    }
  }
}
