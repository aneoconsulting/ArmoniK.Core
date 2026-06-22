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
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.PostgresSQL.Common;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Base.Exceptions;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using Npgsql;

using NpgsqlTypes;

namespace ArmoniK.Core.Adapters.PostgresSQL;

/// <inheritdoc cref="IResultTable" />
public class ResultTable : IResultTable
{
  private readonly NpgsqlConnectionProvider connectionProvider_;

  /// <summary>
  ///   Creates a new ResultTable
  /// </summary>
  public ResultTable(NpgsqlConnectionProvider connectionProvider,
                     ILogger<ResultTable>     logger)
  {
    connectionProvider_ = connectionProvider;
    Logger              = logger;
  }

  /// <inheritdoc />
  public ILogger Logger { get; }

  /// <inheritdoc />
  public async Task Create(ICollection<Result> results,
                            CancellationToken   cancellationToken = default)
  {
    if (results.Count == 0)
    {
      return;
    }

    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);
    await using var transaction = await connection.BeginTransactionAsync(cancellationToken)
                                                  .ConfigureAwait(false);

    try
    {
      await using var batch = new NpgsqlBatch(connection,
                                              transaction);

      foreach (var result in results)
      {
        var cmd = new NpgsqlBatchCommand(@"
INSERT INTO results (
  session_id, result_id, name, created_by, completed_by, owner_task_id,
  status, dependent_tasks, creation_date, completion_date, size, opaque_id, manual_deletion
) VALUES (
  @session_id, @result_id, @name, @created_by, @completed_by, @owner_task_id,
  @status, @dependent_tasks, @creation_date, @completion_date, @size, @opaque_id, @manual_deletion
)");
        AddResultInsertParameters(cmd.Parameters,
                                  result);
        batch.BatchCommands.Add(cmd);
      }

      await batch.ExecuteNonQueryAsync(cancellationToken)
                 .ConfigureAwait(false);
    }
    catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.UniqueViolation)
    {
      throw new ArmoniKException("Key already exists",
                                 e);
    }

    await transaction.CommitAsync(cancellationToken)
                     .ConfigureAwait(false);

    Logger.LogDebug("Created {count} results",
                    results.Count);
  }

  /// <inheritdoc />
  public async Task AddTaskDependencies(IDictionary<string, ICollection<string>> dependencies,
                                        CancellationToken                        cancellationToken = default)
  {
    if (dependencies.Count == 0)
    {
      return;
    }

    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);
    await using var transaction = await connection.BeginTransactionAsync(cancellationToken)
                                                  .ConfigureAwait(false);

    long totalMatched = 0;
    foreach (var (resultId, taskIds) in dependencies)
    {
      await using var cmd = connection.CreateCommand();
      cmd.Transaction = transaction;
      cmd.CommandText = @"
UPDATE results
SET dependent_tasks = (
  SELECT ARRAY(SELECT DISTINCT unnest(dependent_tasks || @new_tasks))
)
WHERE result_id = @result_id";
      cmd.Parameters.AddWithValue("result_id",
                                  resultId);
      cmd.Parameters.AddWithValue("new_tasks",
                                  NpgsqlDbType.Array | NpgsqlDbType.Text,
                                  taskIds.ToArray());

      totalMatched += await cmd.ExecuteNonQueryAsync(cancellationToken)
                               .ConfigureAwait(false);
    }

    if (totalMatched != dependencies.Count)
    {
      await transaction.RollbackAsync(cancellationToken)
                       .ConfigureAwait(false);
      throw new ResultNotFoundException($"One of the input results was not found: expected: {dependencies.Count}, found: {totalMatched}");
    }

    await transaction.CommitAsync(cancellationToken)
                     .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task SetTaskOwnership(ICollection<(string resultId, string taskId)> requests,
                                     CancellationToken                             cancellationToken = default)
  {
    if (requests.Count == 0)
    {
      return;
    }

    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);
    await using var transaction = await connection.BeginTransactionAsync(cancellationToken)
                                                  .ConfigureAwait(false);

    long totalModified = 0;
    foreach (var (resultId, taskId) in requests)
    {
      await using var cmd = connection.CreateCommand();
      cmd.Transaction = transaction;
      cmd.CommandText = "UPDATE results SET owner_task_id = @task_id WHERE result_id = @result_id";
      cmd.Parameters.AddWithValue("task_id",
                                  taskId);
      cmd.Parameters.AddWithValue("result_id",
                                  resultId);

      totalModified += await cmd.ExecuteNonQueryAsync(cancellationToken)
                                .ConfigureAwait(false);
    }

    if (totalModified != requests.Count)
    {
      await transaction.RollbackAsync(cancellationToken)
                       .ConfigureAwait(false);
      throw new ResultNotFoundException("One of the requested results was not found");
    }

    await transaction.CommitAsync(cancellationToken)
                     .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task ChangeResultOwnership(string                                                 oldTaskId,
                                          IEnumerable<IResultTable.ChangeResultOwnershipRequest> requests,
                                          CancellationToken                                      cancellationToken)
  {
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);
    await using var transaction = await connection.BeginTransactionAsync(cancellationToken)
                                                  .ConfigureAwait(false);

    foreach (var request in requests)
    {
      await using var cmd = connection.CreateCommand();
      cmd.Transaction = transaction;
      cmd.CommandText = @"
UPDATE results SET owner_task_id = @new_task_id
WHERE result_id = ANY(@keys) AND owner_task_id = @old_task_id";
      cmd.Parameters.AddWithValue("new_task_id",
                                  request.NewTaskId);
      cmd.Parameters.AddWithValue("keys",
                                  NpgsqlDbType.Array | NpgsqlDbType.Text,
                                  request.Keys.ToArray());
      cmd.Parameters.AddWithValue("old_task_id",
                                  oldTaskId);

      await cmd.ExecuteNonQueryAsync(cancellationToken)
               .ConfigureAwait(false);
    }

    await transaction.CommitAsync(cancellationToken)
                     .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task<Result> GetResult(string            key,
                                      CancellationToken cancellationToken = default)
  {
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    await using var cmd = connection.CreateCommand();
    cmd.CommandText = "SELECT * FROM results WHERE result_id = @result_id";
    cmd.Parameters.AddWithValue("result_id",
                                key);

    await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
                                      .ConfigureAwait(false);

    if (!await reader.ReadAsync(cancellationToken)
                     .ConfigureAwait(false))
    {
      throw new ResultNotFoundException($"Key '{key}' not found");
    }

    return RowMapper.MapToResult(reader);
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<T> GetResults<T>(Expression<Func<Result, bool>>                filter,
                                                  Expression<Func<Result, T>>                   convertor,
                                                  [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    var (whereSql, whereParams) = ExpressionToSql<Result>.Translate(filter);

    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    await using var cmd = connection.CreateCommand();
    cmd.CommandText = $"SELECT * FROM results WHERE {whereSql}";
    SqlHelper.AddExpressionParameters(cmd,
                                      whereParams);

    await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
                                      .ConfigureAwait(false);

    var compiled = convertor.Compile();
    while (await reader.ReadAsync(cancellationToken)
                       .ConfigureAwait(false))
    {
      yield return compiled.Invoke(RowMapper.MapToResult(reader));
    }
  }

  /// <inheritdoc />
  public async Task<(IEnumerable<Result> results, int totalCount)> ListResultsAsync(Expression<Func<Result, bool>>    filter,
                                                                                     Expression<Func<Result, object?>> orderField,
                                                                                     bool                              ascOrder,
                                                                                     int                               page,
                                                                                     int                               pageSize,
                                                                                     CancellationToken                 cancellationToken = default)
  {
    var (whereSql, whereParams) = ExpressionToSql<Result>.Translate(filter);
    var orderColumn             = ExpressionToSql<Result>.TranslateOrderBy(orderField);
    var orderDir                = ascOrder ? "ASC" : "DESC";

    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    // Count query
    await using var countCmd = connection.CreateCommand();
    countCmd.CommandText = $"SELECT COUNT(*) FROM results WHERE {whereSql}";
    SqlHelper.AddExpressionParameters(countCmd,
                                      whereParams);
    var totalCount = Convert.ToInt32(await countCmd.ExecuteScalarAsync(cancellationToken)
                                                   .ConfigureAwait(false));

    if (pageSize <= 0)
    {
      return (Enumerable.Empty<Result>(), totalCount);
    }

    // Data query
    await using var dataCmd = connection.CreateCommand();
    dataCmd.CommandText = $"SELECT * FROM results WHERE {whereSql} ORDER BY {orderColumn} {orderDir} LIMIT @limit OFFSET @offset";
    SqlHelper.AddExpressionParameters(dataCmd,
                                      whereParams);
    dataCmd.Parameters.AddWithValue("limit",
                                    pageSize);
    dataCmd.Parameters.AddWithValue("offset",
                                    page * pageSize);

    await using var reader = await dataCmd.ExecuteReaderAsync(cancellationToken)
                                          .ConfigureAwait(false);

    var results = new List<Result>();
    while (await reader.ReadAsync(cancellationToken)
                       .ConfigureAwait(false))
    {
      results.Add(RowMapper.MapToResult(reader));
    }

    return (results, totalCount);
  }

  /// <inheritdoc />
  public async Task DeleteResult(string            key,
                                 CancellationToken cancellationToken = default)
  {
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    await using var cmd = connection.CreateCommand();
    cmd.CommandText = "DELETE FROM results WHERE result_id = @result_id";
    cmd.Parameters.AddWithValue("result_id",
                                key);

    var deleted = await cmd.ExecuteNonQueryAsync(cancellationToken)
                           .ConfigureAwait(false);

    if (deleted == 0)
    {
      throw new ResultNotFoundException($"Result '{key}' not found.");
    }
  }

  /// <inheritdoc />
  public async Task DeleteResults(string            sessionId,
                                  CancellationToken cancellationToken = default)
  {
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    await using var cmd = connection.CreateCommand();
    cmd.CommandText = "DELETE FROM results WHERE session_id = @session_id";
    cmd.Parameters.AddWithValue("session_id",
                                sessionId);

    await cmd.ExecuteNonQueryAsync(cancellationToken)
             .ConfigureAwait(false);

    Logger.LogInformation("Deleted results from {sessionId}",
                          sessionId);
  }

  /// <inheritdoc />
  public async Task DeleteResults(ICollection<string> results,
                                  CancellationToken   cancellationToken = default)
  {
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    await using var cmd = connection.CreateCommand();
    cmd.CommandText = "DELETE FROM results WHERE result_id = ANY(@result_ids)";
    cmd.Parameters.AddWithValue("result_ids",
                                NpgsqlDbType.Array | NpgsqlDbType.Text,
                                results.ToArray());

    await cmd.ExecuteNonQueryAsync(cancellationToken)
             .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task<Result> UpdateOneResult(string                   resultId,
                                            UpdateDefinition<Result> updates,
                                            CancellationToken        cancellationToken = default)
  {
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);
    await using var transaction = await connection.BeginTransactionAsync(cancellationToken)
                                                  .ConfigureAwait(false);

    // Read before update
    await using var readCmd = connection.CreateCommand();
    readCmd.Transaction = transaction;
    readCmd.CommandText = "SELECT * FROM results WHERE result_id = @result_id";
    readCmd.Parameters.AddWithValue("result_id",
                                    resultId);

    await using var reader = await readCmd.ExecuteReaderAsync(cancellationToken)
                                          .ConfigureAwait(false);

    if (!await reader.ReadAsync(cancellationToken)
                     .ConfigureAwait(false))
    {
      await reader.CloseAsync()
                  .ConfigureAwait(false);
      await transaction.RollbackAsync(cancellationToken)
                       .ConfigureAwait(false);
      throw new ResultNotFoundException($"Result not found {resultId}");
    }

    var before = RowMapper.MapToResult(reader);
    await reader.CloseAsync()
                .ConfigureAwait(false);

    // Update
    await using var updateCmd = connection.CreateCommand();
    updateCmd.Transaction = transaction;
    var setClauses = SqlHelper.BuildSetClauses(updates,
                                              updateCmd);
    updateCmd.CommandText = $"UPDATE results SET {setClauses} WHERE result_id = @result_id";
    updateCmd.Parameters.AddWithValue("result_id",
                                      resultId);

    await updateCmd.ExecuteNonQueryAsync(cancellationToken)
                   .ConfigureAwait(false);

    await transaction.CommitAsync(cancellationToken)
                     .ConfigureAwait(false);

    return before;
  }

  /// <inheritdoc />
  public async Task<long> UpdateManyResults(Expression<Func<Result, bool>> filter,
                                            UpdateDefinition<Result>       updates,
                                            CancellationToken              cancellationToken = default)
  {
    var (whereSql, whereParams) = ExpressionToSql<Result>.Translate(filter);

    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    await using var cmd = connection.CreateCommand();
    var setClauses = SqlHelper.BuildSetClauses(updates,
                                              cmd);
    cmd.CommandText = $"UPDATE results SET {setClauses} WHERE {whereSql}";
    SqlHelper.AddExpressionParameters(cmd,
                                      whereParams);

    var matched = await cmd.ExecuteNonQueryAsync(cancellationToken)
                           .ConfigureAwait(false);

    return matched;
  }

  /// <inheritdoc />
  public async Task<long> BulkUpdateResults(IEnumerable<(Expression<Func<Result, bool>> filter, UpdateDefinition<Result> updates)> bulkUpdates,
                                            CancellationToken                                                                      cancellationToken)
  {
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);
    await using var transaction = await connection.BeginTransactionAsync(cancellationToken)
                                                  .ConfigureAwait(false);

    long totalMatched = 0;
    foreach (var (filter, updates) in bulkUpdates)
    {
      var (whereSql, whereParams) = ExpressionToSql<Result>.Translate(filter);

      await using var cmd = connection.CreateCommand();
      cmd.Transaction = transaction;
      var setClauses = SqlHelper.BuildSetClauses(updates,
                                                cmd);
      cmd.CommandText = $"UPDATE results SET {setClauses} WHERE {whereSql}";
      SqlHelper.AddExpressionParameters(cmd,
                                        whereParams);

      totalMatched += await cmd.ExecuteNonQueryAsync(cancellationToken)
                               .ConfigureAwait(false);
    }

    await transaction.CommitAsync(cancellationToken)
                     .ConfigureAwait(false);

    return totalMatched;
  }

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
    => connectionProvider_.Init(cancellationToken);

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => connectionProvider_.Check(tag);

  private static void AddResultInsertParameters(NpgsqlParameterCollection parameters,
                                                Result                    result)
  {
    parameters.AddWithValue("session_id",
                            result.SessionId);
    parameters.AddWithValue("result_id",
                            result.ResultId);
    parameters.AddWithValue("name",
                            result.Name);
    parameters.AddWithValue("created_by",
                            result.CreatedBy);
    parameters.AddWithValue("completed_by",
                            result.CompletedBy);
    parameters.AddWithValue("owner_task_id",
                            result.OwnerTaskId);
    parameters.AddWithValue("status",
                            (int)result.Status);
    parameters.AddWithValue("dependent_tasks",
                            NpgsqlDbType.Array | NpgsqlDbType.Text,
                            result.DependentTasks.ToArray());
    parameters.AddWithValue("creation_date",
                            result.CreationDate);
    parameters.AddWithValue("completion_date",
                            (object?)result.CompletionDate ?? DBNull.Value);
    parameters.AddWithValue("size",
                            result.Size);
    parameters.AddWithValue("opaque_id",
                            NpgsqlDbType.Bytea,
                            result.OpaqueId);
    parameters.AddWithValue("manual_deletion",
                            result.ManualDeletion);
  }
}
