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
using System.Threading;
using System.Threading.Tasks;

using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB;

/// <summary>
///   Helper class to get stream updates (events produced when there is an action on the db used by replication)
///   from MongoDB collection
/// </summary>
public static class ChangeStreamUpdate
{
  /// <summary>
  ///   Maximum time the server waits for new events before answering a getMore of a change stream with an empty batch.
  ///   It bounds how long a <see cref="WatchEnumerator{TOutput,TInput}" /> takes to notice its cancellation without
  ///   interrupting the pending getMore.
  /// </summary>
  internal static readonly TimeSpan MaxAwaitTime = TimeSpan.FromSeconds(1);

  /// <summary>
  ///   Options used to open the change streams of the watchers
  /// </summary>
  /// <returns>
  ///   New <see cref="ChangeStreamOptions" /> that look up the full document and use <see cref="MaxAwaitTime" />
  /// </returns>
  internal static ChangeStreamOptions CreateOptions()
    => new()
       {
         FullDocument = ChangeStreamFullDocumentOption.UpdateLookup,
         MaxAwaitTime = MaxAwaitTime,
       };

  /// <summary>
  ///   Listen to the change stream with filtering and returns the matching events
  /// </summary>
  /// <typeparam name="T">Type of Document retrieved from the database</typeparam>
  /// <param name="collection">Collection containing the data</param>
  /// <param name="sessionHandle">MongoDB session</param>
  /// <param name="filter">Filter to select the updates that the function will return</param>
  /// <param name="fields">List of fields to watch for modifications</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   A <see cref="IChangeStreamCursor{ChangeStreamDocument}" /> containing the filtered modification events
  /// </returns>
  public static async Task<IChangeStreamCursor<ChangeStreamDocument<T>>> GetUpdates<T>(IMongoCollection<T>       collection,
                                                                                       IClientSessionHandle      sessionHandle,
                                                                                       Expression<Func<T, bool>> filter,
                                                                                       IEnumerable<string>       fields,
                                                                                       CancellationToken         cancellationToken = default)
  {
    var matchUpdatedFields =
      new FilterDefinitionBuilder<ChangeStreamDocument<T>>().Or(fields.Select(f => new FilterDefinitionBuilder<ChangeStreamDocument<T>>()
                                                                                .Exists("updateDescription.updatedFields." + f)));

    var pipeline = new EmptyPipelineDefinition<ChangeStreamDocument<T>>().Match(document => document.OperationType == ChangeStreamOperationType.Update)
                                                                         .Match(filter.ToChangeStreamDocumentExpression())
                                                                         .Match(matchUpdatedFields);

    return await collection.WatchAsync(sessionHandle,
                                       pipeline,
                                       cancellationToken: cancellationToken,
                                       options: CreateOptions())
                           .ConfigureAwait(false);
  }

  internal static Expression<Func<ChangeStreamDocument<T>, bool>> ToChangeStreamDocumentExpression<T>(this Expression<Func<T, bool>> expression)
  {
    var type = typeof(ChangeStreamDocument<T>);
    var parameter = Expression.Parameter(type,
                                         nameof(ChangeStreamDocument<T>));

    var propertyInfo = type.GetProperties()
                           .Single(info => info.Name == nameof(ChangeStreamDocument<T>.FullDocument));

    var access = Expression.MakeMemberAccess(parameter,
                                             propertyInfo);

    var visitor = new ReplaceExpressionVisitor(expression.Parameters[0],
                                               access);

    var newExpression = visitor.Visit(expression.Body);

    return Expression.Lambda<Func<ChangeStreamDocument<T>, bool>>(newExpression!,
                                                                  parameter);
  }


  private class ReplaceExpressionVisitor : ExpressionVisitor
  {
    private readonly Expression newValue_;
    private readonly Expression oldValue_;

    public ReplaceExpressionVisitor(Expression oldValue,
                                    Expression newValue)
    {
      oldValue_ = oldValue;
      newValue_ = newValue;
    }

    public override Expression? Visit(Expression? node)
      => node == oldValue_
           ? newValue_
           : base.Visit(node);
  }
}
