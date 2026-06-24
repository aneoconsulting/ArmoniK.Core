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
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.PostgresSQL.Common;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Storage.Events;

using Microsoft.Extensions.Diagnostics.HealthChecks;

using Npgsql.Replication.PgOutput.Messages;

namespace ArmoniK.Core.Adapters.PostgresSQL;

/// <inheritdoc cref="IResultWatcher" />
public class ResultWatcher : IResultWatcher, IDisposable
{
  private readonly NpgsqlConnectionProvider     connectionProvider_;
  private readonly WalBroadcaster<Result> insertBroadcaster_;

  // Shared between GetResultStatusUpdates and GetResultOwnerUpdates — both consume
  // UpdateMessage on the results table with identical WAL handling, so one connection suffices.
  private readonly WalBroadcaster<Result> updateBroadcaster_;

  /// <summary>
  ///   Creates a new ResultWatcher
  /// </summary>
  public ResultWatcher(NpgsqlConnectionProvider connectionProvider)
  {
    connectionProvider_ = connectionProvider;

    insertBroadcaster_ = new WalBroadcaster<Result>(connectionProvider,
                                                     async (message, ct) =>
                                                     {
                                                       if (message is InsertMessage insert && insert.Relation.RelationName == "results")
                                                         return await WalHelpers.ReadResult(insert.NewRow,
                                                                                            ct)
                                                                                .ConfigureAwait(false);
                                                       await WalHelpers.ConsumeMessage(message,
                                                                                       ct)
                                                                       .ConfigureAwait(false);
                                                       return null;
                                                     });

    updateBroadcaster_ = new WalBroadcaster<Result>(connectionProvider,
                                                     async (message, ct) =>
                                                     {
                                                       if (message is UpdateMessage update && update.Relation.RelationName == "results")
                                                       {
                                                         // DEFAULT replica identity: the old owner_task_id is not present in the WAL.
                                                         // previousOwner is always "" — the same limitation the MongoDB adaptor has
                                                         // (its change stream also does not surface the pre-update field value).
                                                         // Every result UPDATE reaches this watcher; consumers must be idempotent.
                                                         await WalHelpers.ConsumeOldTuple(update,
                                                                                          ct)
                                                                         .ConfigureAwait(false);
                                                         return await WalHelpers.ReadResult(update.NewRow,
                                                                                            ct)
                                                                                .ConfigureAwait(false);
                                                       }

                                                       await WalHelpers.ConsumeMessage(message,
                                                                                       ct)
                                                                       .ConfigureAwait(false);
                                                       return null;
                                                     });
  }

  /// <inheritdoc />
  public void Dispose()
  {
    insertBroadcaster_.Dispose();
    updateBroadcaster_.Dispose();
    GC.SuppressFinalize(this);
  }

  /// <inheritdoc />
  public async Task<IAsyncEnumerable<NewResult>> GetNewResults(Expression<Func<Result, bool>> filter,
                                                               CancellationToken              cancellationToken = default)
  {
    var compiled  = filter.Compile();
    var rawStream = await insertBroadcaster_.SubscribeAsync(cancellationToken)
                                            .ConfigureAwait(false);
    return FilterNewResults(rawStream,
                            compiled,
                            cancellationToken);
  }

  /// <inheritdoc />
  public async Task<IAsyncEnumerable<ResultOwnerUpdate>> GetResultOwnerUpdates(Expression<Func<Result, bool>> filter,
                                                                               CancellationToken              cancellationToken = default)
  {
    var compiled  = filter.Compile();
    var rawStream = await updateBroadcaster_.SubscribeAsync(cancellationToken)
                                            .ConfigureAwait(false);
    return FilterResultOwnerUpdates(rawStream,
                                    compiled,
                                    cancellationToken);
  }

  /// <inheritdoc />
  public async Task<IAsyncEnumerable<ResultStatusUpdate>> GetResultStatusUpdates(Expression<Func<Result, bool>> filter,
                                                                                 CancellationToken              cancellationToken = default)
  {
    // DEFAULT replica identity: same caveat as GetResultOwnerUpdates — no old row values
    // in the WAL, so status comparisons are impossible. Every result UPDATE fires here.
    // Consumers must tolerate duplicate status notifications.
    var compiled  = filter.Compile();
    var rawStream = await updateBroadcaster_.SubscribeAsync(cancellationToken)
                                            .ConfigureAwait(false);
    return FilterResultStatusUpdates(rawStream,
                                     compiled,
                                     cancellationToken);
  }

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
    => connectionProvider_.Init(cancellationToken);

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => connectionProvider_.Check(tag);

  private static async IAsyncEnumerable<NewResult> FilterNewResults(IAsyncEnumerable<Result>                      source,
                                                                      Func<Result, bool>                            compiled,
                                                                      [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    await foreach (var result in source.WithCancellation(cancellationToken)
                                       .ConfigureAwait(false))
    {
      if (!compiled(result))
        continue;

      yield return new NewResult(result.SessionId,
                                 result.ResultId,
                                 result.OwnerTaskId,
                                 result.Status);
    }
  }

  private static async IAsyncEnumerable<ResultOwnerUpdate> FilterResultOwnerUpdates(IAsyncEnumerable<Result>                      source,
                                                                                      Func<Result, bool>                            compiled,
                                                                                      [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    await foreach (var result in source.WithCancellation(cancellationToken)
                                       .ConfigureAwait(false))
    {
      if (!compiled(result))
        continue;

      yield return new ResultOwnerUpdate(result.SessionId,
                                         result.ResultId,
                                         PreviousOwnerId: "",
                                         result.OwnerTaskId);
    }
  }

  private static async IAsyncEnumerable<ResultStatusUpdate> FilterResultStatusUpdates(IAsyncEnumerable<Result>                      source,
                                                                                        Func<Result, bool>                            compiled,
                                                                                        [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    await foreach (var result in source.WithCancellation(cancellationToken)
                                       .ConfigureAwait(false))
    {
      if (!compiled(result))
        continue;

      yield return new ResultStatusUpdate(result.SessionId,
                                          result.ResultId,
                                          result.Status);
    }
  }
}
