using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using MongoDB.Driver;

namespace ArmoniK.Adapters.MongoDB
{
  public static class AsyncCursorSourceExtensions
  {
    public static IAsyncEnumerable<T> ToAsyncEnumerable<T>(
      this IAsyncCursorSource<T> asyncCursorSource) =>
      new AsyncEnumerableAdapter<T>(asyncCursorSource);

    private class AsyncEnumerableAdapter<T> : IAsyncEnumerable<T>
    {
      private readonly IAsyncCursorSource<T> asyncCursorSource_;

      public AsyncEnumerableAdapter(IAsyncCursorSource<T> asyncCursorSource)
      {
        asyncCursorSource_ = asyncCursorSource;
      }

      public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default) =>
        new AsyncEnumeratorAdapter<T>(asyncCursorSource_, cancellationToken);
    }

    private class AsyncEnumeratorAdapter<T> : IAsyncEnumerator<T>
    {
      private readonly IAsyncCursorSource<T> asyncCursorSource_;
      private          IAsyncCursor<T>       asyncCursor_;
      private          IEnumerator<T>        batchEnumerator_;
      private readonly CancellationToken     cancellationToken_;

      public T Current => batchEnumerator_.Current;

      public AsyncEnumeratorAdapter(IAsyncCursorSource<T> asyncCursorSource, CancellationToken cancellationToken = default)
      {
        asyncCursorSource_      = asyncCursorSource;
        this.cancellationToken_ = cancellationToken;
      }

      public async ValueTask<bool> MoveNextAsync()
      {
        if (asyncCursor_ == null)
        {
          asyncCursor_ = await asyncCursorSource_.ToCursorAsync(cancellationToken_);
        }

        if (batchEnumerator_ != null &&
            batchEnumerator_.MoveNext())
        {
          return true;
        }

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