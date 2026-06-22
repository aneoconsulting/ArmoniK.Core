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

using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;

namespace ArmoniK.Core.Adapters.PostgresSQL;

/// <inheritdoc cref="IResultWatcher" />
public class ResultWatcher : IResultWatcher
{
  private readonly NpgsqlConnectionProvider connectionProvider_;

  /// <summary>
  ///   Creates a new ResultWatcher
  /// </summary>
  public ResultWatcher(NpgsqlConnectionProvider connectionProvider)
    => connectionProvider_ = connectionProvider;

  /// <inheritdoc />
  public async Task<IAsyncEnumerable<NewResult>> GetNewResults(Expression<Func<Result, bool>> filter,
                                                               CancellationToken              cancellationToken = default)
    => WatchNewResults(filter,
                       cancellationToken);

  /// <inheritdoc />
  public async Task<IAsyncEnumerable<ResultOwnerUpdate>> GetResultOwnerUpdates(Expression<Func<Result, bool>> filter,
                                                                               CancellationToken              cancellationToken = default)
    => WatchResultOwnerUpdates(filter,
                               cancellationToken);

  /// <inheritdoc />
  public async Task<IAsyncEnumerable<ResultStatusUpdate>> GetResultStatusUpdates(Expression<Func<Result, bool>> filter,
                                                                                 CancellationToken              cancellationToken = default)
    => WatchResultStatusUpdates(filter,
                                cancellationToken);

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
    => connectionProvider_.Init(cancellationToken);

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => connectionProvider_.Check(tag);

  private async IAsyncEnumerable<NewResult> WatchNewResults(Expression<Func<Result, bool>>                filter,
                                                            [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    var compiled = filter.Compile();
    var slotName = $"armonik_{Guid.NewGuid():N}";
    var options  = new PgOutputReplicationOptions("armonik_pub", PgOutputProtocolVersion.V1, null, null, null, null);

    await using var replConn = connectionProvider_.CreateReplicationConnection();
    await replConn.Open(cancellationToken)
                  .ConfigureAwait(false);

    var slot = await replConn.CreatePgOutputReplicationSlot(slotName,
                                                            temporarySlot: true,
                                                            cancellationToken: cancellationToken)
                             .ConfigureAwait(false);

    await foreach (var message in replConn.StartReplication(slot,
                                                            options,
                                                            cancellationToken)
                                          .ConfigureAwait(false))
    {
      replConn.SetReplicationStatus(message.WalEnd);

      if (message is not InsertMessage insert || insert.Relation.RelationName != "results")
      {
        await WalHelpers.ConsumeMessage(message,
                                        cancellationToken)
                        .ConfigureAwait(false);
        continue;
      }

      var cols   = await WalHelpers.ReadAllTextColumns(insert.NewRow, cancellationToken).ConfigureAwait(false);
      var result = RowMapper.MapToResultFromWal(cols);

      if (!compiled(result))
      {
        continue;
      }

      yield return new NewResult(result.SessionId,
                                 result.ResultId,
                                 result.OwnerTaskId,
                                 result.Status);
    }
  }

  private async IAsyncEnumerable<ResultOwnerUpdate> WatchResultOwnerUpdates(Expression<Func<Result, bool>>                filter,
                                                                            [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    var compiled = filter.Compile();
    var slotName = $"armonik_{Guid.NewGuid():N}";
    var options  = new PgOutputReplicationOptions("armonik_pub", PgOutputProtocolVersion.V1, null, null, null, null);

    await using var replConn = connectionProvider_.CreateReplicationConnection();
    await replConn.Open(cancellationToken)
                  .ConfigureAwait(false);

    var slot = await replConn.CreatePgOutputReplicationSlot(slotName,
                                                            temporarySlot: true,
                                                            cancellationToken: cancellationToken)
                             .ConfigureAwait(false);

    await foreach (var message in replConn.StartReplication(slot,
                                                            options,
                                                            cancellationToken)
                                          .ConfigureAwait(false))
    {
      replConn.SetReplicationStatus(message.WalEnd);

      // DEFAULT replica identity: the old owner_task_id is not present in the WAL.
      // previousOwner is always "" — the same limitation the MongoDB adaptor has
      // (its change stream also does not surface the pre-update field value).
      // Every result UPDATE reaches this watcher; consumers must be idempotent.
      if (message is not UpdateMessage update || update.Relation.RelationName != "results")
      {
        await WalHelpers.ConsumeMessage(message,
                                        cancellationToken)
                        .ConfigureAwait(false);
        continue;
      }

      // Consume the old-key tuple before reading NewRow.
      await WalHelpers.ConsumeOldTuple(update,
                                       cancellationToken)
                      .ConfigureAwait(false);

      var cols   = await WalHelpers.ReadAllTextColumns(update.NewRow, cancellationToken).ConfigureAwait(false);
      var result = RowMapper.MapToResultFromWal(cols);

      if (!compiled(result))
      {
        continue;
      }

      yield return new ResultOwnerUpdate(result.SessionId,
                                         result.ResultId,
                                         PreviousOwnerId: "",
                                         result.OwnerTaskId);
    }
  }

  private async IAsyncEnumerable<ResultStatusUpdate> WatchResultStatusUpdates(Expression<Func<Result, bool>>                filter,
                                                                              [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    var compiled = filter.Compile();
    var slotName = $"armonik_{Guid.NewGuid():N}";
    var options  = new PgOutputReplicationOptions("armonik_pub", PgOutputProtocolVersion.V1, null, null, null, null);

    await using var replConn = connectionProvider_.CreateReplicationConnection();
    await replConn.Open(cancellationToken)
                  .ConfigureAwait(false);

    var slot = await replConn.CreatePgOutputReplicationSlot(slotName,
                                                            temporarySlot: true,
                                                            cancellationToken: cancellationToken)
                             .ConfigureAwait(false);

    await foreach (var message in replConn.StartReplication(slot,
                                                            options,
                                                            cancellationToken)
                                          .ConfigureAwait(false))
    {
      replConn.SetReplicationStatus(message.WalEnd);

      // DEFAULT replica identity: same caveat as WatchResultOwnerUpdates — no old row values
      // in the WAL, so status comparisons are impossible. Every result UPDATE fires here.
      // Consumers must tolerate duplicate status notifications.
      if (message is not UpdateMessage update || update.Relation.RelationName != "results")
      {
        await WalHelpers.ConsumeMessage(message,
                                        cancellationToken)
                        .ConfigureAwait(false);
        continue;
      }

      // Consume the old-key tuple before reading NewRow.
      await WalHelpers.ConsumeOldTuple(update,
                                       cancellationToken)
                      .ConfigureAwait(false);

      var cols   = await WalHelpers.ReadAllTextColumns(update.NewRow, cancellationToken).ConfigureAwait(false);
      var result = RowMapper.MapToResultFromWal(cols);

      if (!compiled(result))
      {
        continue;
      }

      yield return new ResultStatusUpdate(result.SessionId,
                                          result.ResultId,
                                          result.Status);
    }
  }

}
