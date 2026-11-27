// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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

namespace ArmoniK.Core.Adapters.Memory;

/// <summary>
///   Helpers to query in-memory collections with the semantics of a database
/// </summary>
internal static class EnumerableExt
{
  /// <summary>
  ///   Sort elements by a key, treating a key that cannot be evaluated as null
  /// </summary>
  /// <remarks>
  ///   A database sorts documents whose sort field is missing as if the field was null. In memory, evaluating such a key
  ///   throws, for instance when the key reads a dictionary entry that does not exist (sort on a generic task option).
  /// </remarks>
  /// <typeparam name="T">Type of the elements</typeparam>
  /// <param name="source">Elements to sort</param>
  /// <param name="key">Expression of the sort key</param>
  /// <param name="ascending">Whether to sort in ascending order</param>
  /// <returns>
  ///   The sorted elements
  /// </returns>
  public static IOrderedEnumerable<T> OrderByKey<T>(this IEnumerable<T>          source,
                                                    Expression<Func<T, object?>> key,
                                                    bool                         ascending)
  {
    var compiled = key.Compile();

    object? SafeKey(T element)
    {
      try
      {
        return compiled(element);
      }
      catch (Exception e) when (e is KeyNotFoundException or NullReferenceException)
      {
        return null;
      }
    }

    return ascending
             ? source.OrderBy(SafeKey)
             : source.OrderByDescending(SafeKey);
  }
}
