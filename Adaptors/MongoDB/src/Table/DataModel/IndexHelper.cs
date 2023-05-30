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
  /// <returns>
  ///   The hashed index model
  /// </returns>
  public static CreateIndexModel<T> CreateHashedIndex<T>(Expression<Func<T, object?>> expr)
    => new(Builders<T>.IndexKeys.Hashed(new ExpressionFieldDefinition<T>(expr)),
           new CreateIndexOptions
           {
             Name = expr.Name,
           });

  /// <summary>
  ///   Creates an ascending index model from expression
  /// </summary>
  /// <typeparam name="T">Type stored in database</typeparam>
  /// <param name="expr">Expression to select the field for the index</param>
  /// <returns>
  ///   The ascending index model
  /// </returns>
  public static CreateIndexModel<T> CreateAscendingIndex<T>(Expression<Func<T, object?>> expr)
    => new(Builders<T>.IndexKeys.Ascending(new ExpressionFieldDefinition<T>(expr)),
           new CreateIndexOptions
           {
             Name = expr.Name,
           });

  /// <summary>
  ///   Creates a combined index model (hashed + ascending) from expression
  /// </summary>
  /// <typeparam name="T">Type stored in database</typeparam>
  /// <param name="hashed">Expression to select the field for the hashed index</param>
  /// <param name="ascending">Expression to select the field for the ascending index</param>
  /// <returns>
  ///   The combined index model
  /// </returns>
  public static CreateIndexModel<T> CreateCombinedIndex<T>(Expression<Func<T, object?>> hashed,
                                                           Expression<Func<T, object?>> ascending)
    => new(Builders<T>.IndexKeys.Combine(Builders<T>.IndexKeys.Hashed(new ExpressionFieldDefinition<T>(hashed)),
                                         Builders<T>.IndexKeys.Ascending(new ExpressionFieldDefinition<T>(ascending))),
           new CreateIndexOptions
           {
             Name = $"{hashed.GetMember().Name}_hashed_{ascending.GetMember().Name}_1",
           });
}
