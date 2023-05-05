// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;

using MongoDB.Driver;
using MongoDB.Driver.Linq;

namespace ArmoniK.Core.Adapters.MongoDB;

internal static class IMongoQueryableExt
{
  public static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(this                     IMongoQueryable<T> queryable,
                                                               [EnumeratorCancellation] CancellationToken  cancellationToken = default)
  {
    var cursor = await queryable.ToCursorAsync(cancellationToken)
                                .ConfigureAwait(false);
    while (await cursor.MoveNextAsync(cancellationToken)
                       .ConfigureAwait(false))
    {
      foreach (var item in cursor.Current)
      {
        cancellationToken.ThrowIfCancellationRequested();
        yield return item;
      }
    }
  }


  public static async IAsyncEnumerable<U> ToAsyncEnumerable<T,U>(this                     IFindFluent<T, U>    queryable,
                                                               [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    var cursor = await queryable.ToCursorAsync(cancellationToken)
                                .ConfigureAwait(false);
    while (await cursor.MoveNextAsync(cancellationToken)
                       .ConfigureAwait(false))
    {
      foreach (var item in cursor.Current)
      {
        cancellationToken.ThrowIfCancellationRequested();
        yield return item;
      }
    }
  }


  public static IOrderedMongoQueryable<T> OrderByList<T>(this IMongoQueryable<T>                   queryable,
                                                         ICollection<Expression<Func<T, object?>>> orderFields,
                                                         bool                                      ascOrder = true)
  {
    if (ascOrder)
    {
      var ordered = queryable.OrderBy(orderFields.First());
      if (orderFields.Count > 1)
      {
        foreach (var expression in orderFields.Skip(1))
        {
          ordered = ordered.ThenBy(expression);
        }
      }

      return ordered;
    }
    else
    {
      var ordered = queryable.OrderByDescending(orderFields.First());
      if (orderFields.Count > 1)
      {
        foreach (var expression in orderFields.Skip(1))
        {
          ordered = ordered.ThenByDescending(expression);
        }
      }

      return ordered;
    }
  }
}
