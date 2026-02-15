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
// but WITHOUT ANY WARRANTY; without even the implied warranty of
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
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.PostgresSQL.Common;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Storage.Events;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using Npgsql;

namespace ArmoniK.Core.Adapters.PostgresSQL;

/// <inheritdoc cref="IResultWatcher" />
public class ResultWatcher : IResultWatcher
{
  private readonly NpgsqlConnectionProvider connectionProvider_;
  private readonly ILogger<ResultWatcher>   logger_;

  /// <summary>
  ///   Creates a new ResultWatcher
  /// </summary>
  public ResultWatcher(NpgsqlConnectionProvider connectionProvider,
                       ILogger<ResultWatcher>   logger)
  {
    connectionProvider_ = connectionProvider;
    logger_             = logger;
  }

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
    var channel  = Channel.CreateUnbounded<string>();

    await using var listenConn = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    listenConn.Notification += (_, args) => channel.Writer.TryWrite(args.Payload);

    await using var listenCmd = listenConn.CreateCommand();
    listenCmd.CommandText = "LISTEN result_insert";
    await listenCmd.ExecuteNonQueryAsync(cancellationToken)
                   .ConfigureAwait(false);

    _ = WaitForNotifications(listenConn,
                             cancellationToken);

    await foreach (var payload in channel.Reader.ReadAllAsync(cancellationToken)
                                         .ConfigureAwait(false))
    {
      // Payload format: result_id|session_id
      var parts = payload.Split('|');
      if (parts.Length < 2)
      {
        continue;
      }

      var resultId = parts[0];

      Result? result;
      try
      {
        await using var fetchConn = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                              .ConfigureAwait(false);
        await using var fetchCmd = fetchConn.CreateCommand();
        fetchCmd.CommandText = "SELECT * FROM results WHERE result_id = @result_id";
        fetchCmd.Parameters.AddWithValue("result_id",
                                         resultId);
        await using var reader = await fetchCmd.ExecuteReaderAsync(cancellationToken)
                                               .ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken)
                         .ConfigureAwait(false))
        {
          continue;
        }

        result = RowMapper.MapToResult(reader);
      }
      catch (Exception e)
      {
        logger_.LogWarning(e,
                           "Failed to fetch result {resultId} for watcher",
                           resultId);
        continue;
      }

      if (!compiled.Invoke(result))
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
    var channel  = Channel.CreateUnbounded<string>();

    await using var listenConn = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    listenConn.Notification += (_, args) => channel.Writer.TryWrite(args.Payload);

    await using var listenCmd = listenConn.CreateCommand();
    listenCmd.CommandText = "LISTEN result_owner";
    await listenCmd.ExecuteNonQueryAsync(cancellationToken)
                   .ConfigureAwait(false);

    _ = WaitForNotifications(listenConn,
                             cancellationToken);

    await foreach (var payload in channel.Reader.ReadAllAsync(cancellationToken)
                                         .ConfigureAwait(false))
    {
      // Payload format: result_id|session_id|old_owner_task_id|new_owner_task_id
      var parts = payload.Split('|');
      if (parts.Length < 4)
      {
        continue;
      }

      var resultId      = parts[0];
      var sessionId     = parts[1];
      var previousOwner = parts[2];
      var newOwner      = parts[3];

      Result? result;
      try
      {
        await using var fetchConn = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                              .ConfigureAwait(false);
        await using var fetchCmd = fetchConn.CreateCommand();
        fetchCmd.CommandText = "SELECT * FROM results WHERE result_id = @result_id";
        fetchCmd.Parameters.AddWithValue("result_id",
                                         resultId);
        await using var reader = await fetchCmd.ExecuteReaderAsync(cancellationToken)
                                               .ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken)
                         .ConfigureAwait(false))
        {
          continue;
        }

        result = RowMapper.MapToResult(reader);
      }
      catch (Exception e)
      {
        logger_.LogWarning(e,
                           "Failed to fetch result {resultId} for watcher",
                           resultId);
        continue;
      }

      if (!compiled.Invoke(result))
      {
        continue;
      }

      yield return new ResultOwnerUpdate(sessionId,
                                         resultId,
                                         previousOwner,
                                         newOwner);
    }
  }

  private async IAsyncEnumerable<ResultStatusUpdate> WatchResultStatusUpdates(Expression<Func<Result, bool>>                filter,
                                                                              [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    var compiled = filter.Compile();
    var channel  = Channel.CreateUnbounded<string>();

    await using var listenConn = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    listenConn.Notification += (_, args) => channel.Writer.TryWrite(args.Payload);

    await using var listenCmd = listenConn.CreateCommand();
    listenCmd.CommandText = "LISTEN result_status";
    await listenCmd.ExecuteNonQueryAsync(cancellationToken)
                   .ConfigureAwait(false);

    _ = WaitForNotifications(listenConn,
                             cancellationToken);

    await foreach (var payload in channel.Reader.ReadAllAsync(cancellationToken)
                                         .ConfigureAwait(false))
    {
      // Payload format: result_id|session_id|status
      var parts = payload.Split('|');
      if (parts.Length < 3)
      {
        continue;
      }

      var resultId  = parts[0];
      var sessionId = parts[1];

      if (!int.TryParse(parts[2],
                        out var statusInt))
      {
        continue;
      }

      var status = (ResultStatus)statusInt;

      Result? result;
      try
      {
        await using var fetchConn = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                              .ConfigureAwait(false);
        await using var fetchCmd = fetchConn.CreateCommand();
        fetchCmd.CommandText = "SELECT * FROM results WHERE result_id = @result_id";
        fetchCmd.Parameters.AddWithValue("result_id",
                                         resultId);
        await using var reader = await fetchCmd.ExecuteReaderAsync(cancellationToken)
                                               .ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken)
                         .ConfigureAwait(false))
        {
          continue;
        }

        result = RowMapper.MapToResult(reader);
      }
      catch (Exception e)
      {
        logger_.LogWarning(e,
                           "Failed to fetch result {resultId} for watcher",
                           resultId);
        continue;
      }

      if (!compiled.Invoke(result))
      {
        continue;
      }

      yield return new ResultStatusUpdate(sessionId,
                                          resultId,
                                          status);
    }
  }

  private static async Task WaitForNotifications(NpgsqlConnection  connection,
                                                  CancellationToken cancellationToken)
  {
    try
    {
      while (!cancellationToken.IsCancellationRequested)
      {
        await connection.WaitAsync(cancellationToken)
                        .ConfigureAwait(false);
      }
    }
    catch (OperationCanceledException)
    {
    }
  }
}
