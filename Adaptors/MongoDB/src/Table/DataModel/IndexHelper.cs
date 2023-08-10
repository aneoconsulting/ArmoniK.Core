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
using System.Linq;
using System.Linq.Expressions;

using FluentValidation.Internal;

using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB.Table.DataModel;

/// <summary>
///   Index Type
/// </summary>
public enum IndexType
{
  Ascending,
  Descending,
  Hashed,
  Text,
}

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
             Name = $"{expr.GetMember().Name}_h",
           });

  /// <summary>
  ///   Creates an text index model from expression
  /// </summary>
  /// <typeparam name="T">Type stored in database</typeparam>
  /// <param name="expr">Expression to select the field for the index</param>
  /// <param name="unique">Unicity constraint, default to false</param>
  /// <returns>
  ///   The text index model
  /// </returns>
  public static CreateIndexModel<T> CreateTextIndex<T>(Expression<Func<T, object?>> expr,
                                                       bool                         unique = false)
    => new(Builders<T>.IndexKeys.Text(new ExpressionFieldDefinition<T>(expr)),
           new CreateIndexOptions
           {
             Name   = $"{expr.GetMember().Name}_t",
             Unique = unique,
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
  ///   Creates an descending index model from expression
  /// </summary>
  /// <typeparam name="T">Type stored in database</typeparam>
  /// <param name="expr">Expression to select the field for the index</param>
  /// <param name="unique">Unicity constraint, default to false</param>
  /// <returns>
  ///   The descending index model
  /// </returns>
  public static CreateIndexModel<T> CreateDescendingIndex<T>(Expression<Func<T, object?>> expr,
                                                             bool                         unique = false)
    => new(Builders<T>.IndexKeys.Descending(new ExpressionFieldDefinition<T>(expr)),
           new CreateIndexOptions
           {
             Name   = $"{expr.GetMember().Name}_-1",
             Unique = unique,
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
             Name = $"{hashed.GetMember().Name}_h_{ascending.GetMember().Name}_1",
           });

  /// <summary>
  ///   Creates a generic index from a list of indices type and expressions, with no unicity constraint
  /// </summary>
  /// <typeparam name="T">Type stored in database</typeparam>
  /// <param name="field">Type and Expressions to select the fields for the index</param>
  /// <returns> The corresponding generated index</returns>
  /// <exception cref="ArgumentException">Thrown when fields has no expression</exception>
  /// <exception cref="ArgumentOutOfRangeException">Thrown for invalid IndexType, or hashed index related issues</exception>
  public static CreateIndexModel<T> CreateIndex<T>(params (IndexType type, Expression<Func<T, object?>> expression)[] field)
    => CreateIndex(false,
                   field);

  /// <summary>
  ///   Creates an index from an index type and expression, with no unicity constraint
  /// </summary>
  /// <typeparam name="T">Type stored in database</typeparam>
  /// <param name="type">Type of index</param>
  /// <param name="expression">Expression to select the field for the index</param>
  /// <returns> The corresponding generated index</returns>
  /// <exception cref="ArgumentOutOfRangeException">Thrown for invalid IndexType, or hashed index related issues</exception>
  public static CreateIndexModel<T> CreateIndex<T>(IndexType                    type,
                                                   Expression<Func<T, object?>> expression)
    => CreateIndex(false,
                   (type, expression));

  /// <summary>
  ///   Creates a generic index from a list of indices type and expressions with a unicity constraint
  /// </summary>
  /// <typeparam name="T">Type stored in database</typeparam>
  /// <param name="field">Expressions to select the fields for the index</param>
  /// <returns> The corresponding generated index</returns>
  /// <exception cref="ArgumentException">Thrown when fields has no expression</exception>
  /// <exception cref="ArgumentOutOfRangeException">Thrown for invalid IndexType, or hashed index related issues</exception>
  public static CreateIndexModel<T> CreateUniqueIndex<T>(params (IndexType type, Expression<Func<T, object?>> expression)[] field)
    => CreateIndex(true,
                   field);

  /// <summary>
  ///   Creates an index from an index type and expression with a unicity constraint
  /// </summary>
  /// <typeparam name="T">Type stored in database</typeparam>
  /// <param name="type">Type of index</param>
  /// <param name="expression">Expression to select the field for the index</param>
  /// <returns> The corresponding generated index</returns>
  /// <exception cref="ArgumentOutOfRangeException">Thrown for invalid IndexType, or hashed index related issues</exception>
  public static CreateIndexModel<T> CreateUniqueIndex<T>(IndexType                    type,
                                                         Expression<Func<T, object?>> expression)
    => CreateIndex(true,
                   (type, expression));


  /// <summary>
  ///   Creates a generic index from a list of indices type and expressions
  /// </summary>
  /// <typeparam name="T">Type stored in database</typeparam>
  /// <param name="unique">Unicity constraint</param>
  /// <param name="field">Expressions to select the fields for the index</param>
  /// <returns> The corresponding generated index</returns>
  /// <exception cref="ArgumentException">Thrown when fields has no expression</exception>
  /// <exception cref="ArgumentOutOfRangeException">Thrown for invalid IndexType, or hashed index related issues</exception>
  public static CreateIndexModel<T> CreateIndex<T>(bool                                                               unique,
                                                   params (IndexType type, Expression<Func<T, object?>> expression)[] field)
  {
    if (field.Length == 0)
    {
      throw new ArgumentException("CreateIndex should have at least one argument");
    }

    if (field.Length == 1)
    {
      if (unique && field[0]
            .type == IndexType.Hashed)
      {
        throw new ArgumentOutOfRangeException(nameof(unique),
                                              "A hashed index cannot be constrained to be unique");
      }

      return field[0]
               .type switch
             {
               IndexType.Ascending => CreateAscendingIndex(field[0]
                                                             .expression,
                                                           unique),
               IndexType.Descending => CreateAscendingIndex(field[0]
                                                              .expression,
                                                            unique),
               IndexType.Hashed => CreateHashedIndex(field[0]
                                                       .expression),
               IndexType.Text => CreateTextIndex(field[0]
                                                   .expression,
                                                 unique),
               _ => throw new ArgumentOutOfRangeException(nameof(field),
                                                          "Invalid IndexType"),
             };
    }

    if (field.Count(f => f.type == IndexType.Hashed) is var hashedCount && hashedCount > 1)
    {
      throw new ArgumentOutOfRangeException(nameof(field),
                                            "At most one Hashed index is supported in a compound index");
    }

    if (unique && hashedCount > 0)
    {
      throw new ArgumentOutOfRangeException(nameof(unique),
                                            "A hashed index cannot be constrained to be unique");
    }

    return new CreateIndexModel<T>(Builders<T>.IndexKeys.Combine(field.Select(f => f.type switch
                                                                                   {
                                                                                     IndexType.Ascending =>
                                                                                       Builders<T>.IndexKeys.Ascending(new ExpressionFieldDefinition<T>(f.expression)),
                                                                                     IndexType.Descending =>
                                                                                       Builders<T>.IndexKeys.Descending(new ExpressionFieldDefinition<T>(f.expression)),
                                                                                     IndexType.Hashed =>
                                                                                       Builders<T>.IndexKeys.Hashed(new ExpressionFieldDefinition<T>(f.expression)),
                                                                                     IndexType.Text =>
                                                                                       Builders<T>.IndexKeys.Text(new ExpressionFieldDefinition<T>(f.expression)),
                                                                                     _ => throw new ArgumentOutOfRangeException(nameof(field),
                                                                                                                                "Invalid IndexType"),
                                                                                   })
                                                                      .ToArray()),
                                   new CreateIndexOptions
                                   {
                                     Name = string.Join('_',
                                                        field.SelectMany(f => new[]
                                                                              {
                                                                                f.expression.GetMember()
                                                                                 .Name,
                                                                                f.type switch
                                                                                {
                                                                                  IndexType.Ascending  => "1",
                                                                                  IndexType.Descending => "-1",
                                                                                  IndexType.Hashed     => "h",
                                                                                  IndexType.Text       => "t",
                                                                                  _ => throw new ArgumentOutOfRangeException(nameof(field),
                                                                                                                             "Invalid IndexType"),
                                                                                },
                                                                              })),
                                     Unique = unique,
                                   });
  }
}
