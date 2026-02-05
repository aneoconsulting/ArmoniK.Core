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
using System.Linq.Expressions;

using FluentValidation.Internal;

using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB.Table.DataModel;

/// <summary>
///   Helpers to created indexes for MongoDB
/// </summary>
public class IndexHelper
{
  /// <summary>
  ///   Creates an hashed index model from expression
  /// </summary>
  /// <typeparam name="T">Type stored in database</typeparam>
  /// <param name="expr">Expression to select the field for the index</param>
  /// <param name="useHashed">Whether to use Hashed indexes</param>
  /// <returns>
  ///   The hashed index model
  /// </returns>
  public static CreateIndexModel<T> CreateHashedOrAscendingIndex<T>(Expression<Func<T, object?>> expr,
                                                                    bool                         useHashed)
    => useHashed
         ? CreateHashedIndex(expr)
         : CreateAscendingIndex(expr);

  /// <summary>
  ///   Creates an hashed index model from expression
  /// </summary>
  /// <typeparam name="T">Type stored in database</typeparam>
  /// <param name="expr">Expression to select the field for the index</param>
  /// <returns>
  ///   The hashed index model
  /// </returns>
  private static CreateIndexModel<T> CreateHashedIndex<T>(Expression<Func<T, object?>> expr)
    => new(Builders<T>.IndexKeys.Hashed(new ExpressionFieldDefinition<T>(expr)),
           new CreateIndexOptions
           {
             Name = $"{expr.GetMember().Name}_h",
           });

  /// <summary>
  ///   Creates an ascending index model from expression
  /// </summary>
  /// <typeparam name="T">Type stored in database</typeparam>
  /// <param name="expr">Expression to select the field for the index</param>
  /// <param name="unique">Unicity constraint, default to false</param>
  /// <param name="expireAfter">Setup document should expire</param>
  /// <returns>
  ///   The ascending index model
  /// </returns>
  public static CreateIndexModel<T> CreateAscendingIndex<T>(Expression<Func<T, object?>> expr,
                                                            bool                         unique      = false,
                                                            TimeSpan?                    expireAfter = null)
    => new(Builders<T>.IndexKeys.Ascending(new ExpressionFieldDefinition<T>(expr)),
           new CreateIndexOptions
           {
             Name   = $"{expr.GetMember().Name}_1",
             Unique = unique,
             ExpireAfter = expireAfter == TimeSpan.MaxValue
                             ? null
                             : expireAfter,
           });

  /// <summary>
  ///   Creates a combined index model from expression
  /// </summary>
  /// <typeparam name="T">Type stored in database</typeparam>
  /// <param name="first">Expression to select the field for the first index</param>
  /// <param name="second">Expression to select the field for the second index</param>
  /// <param name="unique">Unicity constraint, default to false</param>
  /// <returns>
  ///   The combined index model
  /// </returns>
  public static CreateIndexModel<T> CreateCombinedIndex<T>(Expression<Func<T, object?>> first,
                                                           Expression<Func<T, object?>> second,
                                                           bool                         unique = false)
    => new(Builders<T>.IndexKeys.Combine(Builders<T>.IndexKeys.Ascending(new ExpressionFieldDefinition<T>(first)),
                                         Builders<T>.IndexKeys.Ascending(new ExpressionFieldDefinition<T>(second))),
           new CreateIndexOptions
           {
             Name   = $"{first.GetMember().Name}_1_{second.GetMember().Name}_1",
             Unique = unique,
           });
}
