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
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.PostgresSQL.Common;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using Npgsql;

using NpgsqlTypes;

namespace ArmoniK.Core.Adapters.PostgresSQL;

/// <inheritdoc cref="ISessionTable" />
public class SessionTable : ISessionTable
{
  private readonly NpgsqlConnectionProvider connectionProvider_;

  /// <summary>
  ///   Creates a new SessionTable
  /// </summary>
  public SessionTable(NpgsqlConnectionProvider connectionProvider,
                      ILogger<SessionTable>    logger)
  {
    connectionProvider_ = connectionProvider;
    Logger              = logger;
  }

  /// <inheritdoc />
  public ILogger Logger { get; }

  /// <inheritdoc />
  public async Task<string> SetSessionDataAsync(IEnumerable<string> partitionIds,
                                                TaskOptions         defaultOptions,
                                                CancellationToken   cancellationToken = default)
  {
    var sessionId = Guid.NewGuid()
                        .ToString();

    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    await using var cmd = connection.CreateCommand();
    cmd.CommandText = @"
INSERT INTO sessions (
  session_id, status, client_submission, worker_submission, creation_date,
  partition_ids, options_options, options_max_duration, options_max_retries,
  options_priority, options_partition_id, options_app_name, options_app_version,
  options_app_namespace, options_app_service, options_engine_type
) VALUES (
  @session_id, @status, @client_submission, @worker_submission, @creation_date,
  @partition_ids, @options_options, @options_max_duration, @options_max_retries,
  @options_priority, @options_partition_id, @options_app_name, @options_app_version,
  @options_app_namespace, @options_app_service, @options_engine_type
)";
    cmd.Parameters.AddWithValue("session_id",
                                sessionId);
    cmd.Parameters.AddWithValue("status",
                                (int)SessionStatus.Running);
    cmd.Parameters.AddWithValue("client_submission",
                                true);
    cmd.Parameters.AddWithValue("worker_submission",
                                true);
    cmd.Parameters.AddWithValue("creation_date",
                                DateTime.UtcNow);
    cmd.Parameters.AddWithValue("partition_ids",
                                NpgsqlDbType.Array | NpgsqlDbType.Text,
                                partitionIds.ToArray());
    cmd.Parameters.AddWithValue("options_options",
                                NpgsqlDbType.Jsonb,
                                JsonSerializer.Serialize(defaultOptions.Options));
    cmd.Parameters.AddWithValue("options_max_duration",
                                defaultOptions.MaxDuration.Ticks);
    cmd.Parameters.AddWithValue("options_max_retries",
                                defaultOptions.MaxRetries);
    cmd.Parameters.AddWithValue("options_priority",
                                defaultOptions.Priority);
    cmd.Parameters.AddWithValue("options_partition_id",
                                defaultOptions.PartitionId);
    cmd.Parameters.AddWithValue("options_app_name",
                                defaultOptions.ApplicationName);
    cmd.Parameters.AddWithValue("options_app_version",
                                defaultOptions.ApplicationVersion);
    cmd.Parameters.AddWithValue("options_app_namespace",
                                defaultOptions.ApplicationNamespace);
    cmd.Parameters.AddWithValue("options_app_service",
                                defaultOptions.ApplicationService);
    cmd.Parameters.AddWithValue("options_engine_type",
                                defaultOptions.EngineType);

    await cmd.ExecuteNonQueryAsync(cancellationToken)
             .ConfigureAwait(false);

    return sessionId;
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<T> FindSessionsAsync<T>(Expression<Func<SessionData, bool>>         filter,
                                                        Expression<Func<SessionData, T>>             selector,
                                                        [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    var (whereSql, whereParams) = ExpressionToSql<SessionData>.Translate(filter);

    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    await using var cmd = connection.CreateCommand();
    cmd.CommandText = $"SELECT * FROM sessions WHERE {whereSql}";
    SqlHelper.AddExpressionParameters(cmd,
                                      whereParams);

    await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
                                      .ConfigureAwait(false);

    var compiled = selector.Compile();
    while (await reader.ReadAsync(cancellationToken)
                       .ConfigureAwait(false))
    {
      yield return compiled.Invoke(RowMapper.MapToSessionData(reader));
    }
  }

  /// <inheritdoc />
  public async Task DeleteSessionAsync(string            sessionId,
                                       CancellationToken cancellationToken = default)
  {
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    await using var cmd = connection.CreateCommand();
    cmd.CommandText = "DELETE FROM sessions WHERE session_id = @session_id";
    cmd.Parameters.AddWithValue("session_id",
                                sessionId);

    var deleted = await cmd.ExecuteNonQueryAsync(cancellationToken)
                           .ConfigureAwait(false);

    if (deleted > 0)
    {
      Logger.LogInformation("Deleted {sessionId}",
                            sessionId);
    }
    else
    {
      Logger.LogInformation("Tried to delete {sessionId} but not found",
                            sessionId);
    }
  }

  /// <inheritdoc />
  public async Task<(IEnumerable<SessionData> sessions, long totalCount)> ListSessionsAsync(Expression<Func<SessionData, bool>>    filter,
                                                                                            Expression<Func<SessionData, object?>> orderField,
                                                                                            bool                                   ascOrder,
                                                                                            int                                    page,
                                                                                            int                                    pageSize,
                                                                                            CancellationToken                      cancellationToken = default)
  {
    var (whereSql, whereParams) = ExpressionToSql<SessionData>.Translate(filter);
    var orderColumn             = ExpressionToSql<SessionData>.TranslateOrderBy(orderField);
    var orderDir                = ascOrder ? "ASC" : "DESC";

    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    // Count query
    await using var countCmd = connection.CreateCommand();
    countCmd.CommandText = $"SELECT COUNT(*) FROM sessions WHERE {whereSql}";
    SqlHelper.AddExpressionParameters(countCmd,
                                      whereParams);
    var totalCount = Convert.ToInt64(await countCmd.ExecuteScalarAsync(cancellationToken)
                                                   .ConfigureAwait(false));

    if (pageSize <= 0)
    {
      return (Enumerable.Empty<SessionData>(), totalCount);
    }

    // Data query
    await using var dataCmd = connection.CreateCommand();
    dataCmd.CommandText = $"SELECT * FROM sessions WHERE {whereSql} ORDER BY {orderColumn} {orderDir} LIMIT @limit OFFSET @offset";
    SqlHelper.AddExpressionParameters(dataCmd,
                                      whereParams);
    dataCmd.Parameters.AddWithValue("limit",
                                    pageSize);
    dataCmd.Parameters.AddWithValue("offset",
                                    page * pageSize);

    await using var reader = await dataCmd.ExecuteReaderAsync(cancellationToken)
                                          .ConfigureAwait(false);

    var results = new List<SessionData>();
    while (await reader.ReadAsync(cancellationToken)
                       .ConfigureAwait(false))
    {
      results.Add(RowMapper.MapToSessionData(reader));
    }

    return (results, totalCount);
  }

  /// <inheritdoc />
  public async Task<SessionData?> UpdateOneSessionAsync(string                               sessionId,
                                                        Expression<Func<SessionData, bool>>? filter,
                                                        UpdateDefinition<SessionData>        updates,
                                                        bool                                 before,
                                                        CancellationToken                    cancellationToken = default)
  {
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);
    await using var transaction = await connection.BeginTransactionAsync(cancellationToken)
                                                  .ConfigureAwait(false);

    var whereClause = "session_id = @sessionId";
    var filterParams = new Dictionary<string, object?>
                       {
                         { "@sessionId", sessionId },
                       };

    if (filter is not null)
    {
      var (filterSql, fParams) = ExpressionToSql<SessionData>.Translate(filter);
      whereClause              = $"{whereClause} AND {filterSql}";
      foreach (var p in fParams)
      {
        filterParams[p.Key] = p.Value;
      }
    }

    // Read before if requested
    SessionData? result = null;
    if (before)
    {
      result = await ReadSessionByWhere(connection,
                                        transaction,
                                        whereClause,
                                        filterParams,
                                        cancellationToken)
                 .ConfigureAwait(false);
    }

    // Update
    await using var updateCmd = connection.CreateCommand();
    updateCmd.Transaction = transaction;
    var setClauses = SqlHelper.BuildSetClauses(updates,
                                              updateCmd);
    updateCmd.CommandText = $"UPDATE sessions SET {setClauses} WHERE {whereClause}";
    SqlHelper.AddExpressionParameters(updateCmd,
                                      filterParams);

    var matched = await updateCmd.ExecuteNonQueryAsync(cancellationToken)
                                 .ConfigureAwait(false);

    if (matched == 0)
    {
      await transaction.RollbackAsync(cancellationToken)
                       .ConfigureAwait(false);
      return null;
    }

    if (!before)
    {
      result = await ReadSessionByWhere(connection,
                                        transaction,
                                        "session_id = @sessionId",
                                        new Dictionary<string, object?>
                                        {
                                          { "@sessionId", sessionId },
                                        },
                                        cancellationToken)
                 .ConfigureAwait(false);
    }

    await transaction.CommitAsync(cancellationToken)
                     .ConfigureAwait(false);

    return result;
  }

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
    => connectionProvider_.Init(cancellationToken);

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => connectionProvider_.Check(tag);

  private static async Task<SessionData?> ReadSessionByWhere(NpgsqlConnection             connection,
                                                             NpgsqlTransaction            transaction,
                                                             string                       whereClause,
                                                             Dictionary<string, object?> parameters,
                                                             CancellationToken            cancellationToken)
  {
    await using var cmd = connection.CreateCommand();
    cmd.Transaction = transaction;
    cmd.CommandText = $"SELECT * FROM sessions WHERE {whereClause}";
    SqlHelper.AddExpressionParameters(cmd,
                                      parameters);

    await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
                                      .ConfigureAwait(false);

    if (!await reader.ReadAsync(cancellationToken)
                     .ConfigureAwait(false))
    {
      return null;
    }

    return RowMapper.MapToSessionData(reader);
  }
}
