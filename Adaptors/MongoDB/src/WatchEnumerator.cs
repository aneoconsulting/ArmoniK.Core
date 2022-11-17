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
//   D. Brasseur       <dbrasseur@aneo.fr>
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
using System.Threading;

using ArmoniK.Core.Common.Storage;

using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB;

public sealed class WatchEnumerator<TOutput, TInput> : IWatchEnumerator<TOutput>
{
  private readonly IChangeStreamCursor<TInput> cursor_;
  private readonly Func<TInput, TOutput>       converter_;
  private readonly CancellationToken           cancellationToken_;
  private          IEnumerator<TInput>?        currentEnumerable_;

  public WatchEnumerator(IChangeStreamCursor<TInput> cursor,
                         Func<TInput, TOutput>       converter,
                         CancellationToken           cancellationToken)
  {
    cursor_            = cursor;
    converter_         = converter;
    cancellationToken_ = cancellationToken;
  }

  public void Dispose()
  {
    cursor_.Dispose();
    currentEnumerable_?.Dispose();
  }


  public bool MoveNext(CancellationToken cancellationToken)
  {
    if (cancellationToken_.IsCancellationRequested)
    {
      return false;
    }

    if (currentEnumerable_ is not null)
    {
      if (currentEnumerable_.MoveNext())
      {
        return true;
      }

      currentEnumerable_ = null;
    }

    while (currentEnumerable_ is null)
    {
      if (cancellationToken_.IsCancellationRequested)
      {
        return false;
      }

      cancellationToken.ThrowIfCancellationRequested();

      if (!cursor_.MoveNext(cancellationToken))
      {
        return false;
      }

      var enumerator = cursor_.Current.GetEnumerator();
      if (enumerator.MoveNext())
      {
        currentEnumerable_ = enumerator;
        return true;
      }
    }

    return false;
  }

  public TOutput Current
  {
    get
    {
      if (currentEnumerable_ is null)
      {
        throw new InvalidOperationException();
      }

      return converter_(currentEnumerable_!.Current);
    }
  }
}
