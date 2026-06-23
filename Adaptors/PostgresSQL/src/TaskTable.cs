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

using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

namespace ArmoniK.Core.Adapters.PostgresSQL;

/// <inheritdoc cref="ITaskTable" />
public class TaskTable : ITaskTable
{
  private readonly NpgsqlConnectionProvider connectionProvider_;
  private readonly Options.TableStorage     tableStorageOptions_;

  /// <summary>
  ///   Creates a new TaskTable
  /// </summary>
  public TaskTable(NpgsqlConnectionProvider connectionProvider,
                   Options.TableStorage     tableStorageOptions,
                   ILogger<TaskTable>       logger)
  {
    connectionProvider_  = connectionProvider;
    tableStorageOptions_ = tableStorageOptions;
    Logger               = logger;
  }

  /// <inheritdoc />
  public TimeSpan PollingDelayMin
    => tableStorageOptions_.PollingDelayMin;

  /// <inheritdoc />
  public TimeSpan PollingDelayMax
    => tableStorageOptions_.PollingDelayMax;

  /// <inheritdoc />
  public ILogger Logger { get; }

  /// <inheritdoc />
  public async Task CreateTasks(IEnumerable<TaskData> tasks,
                                CancellationToken     cancellationToken = default)
  {
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);
    await using var transaction = await connection.BeginTransactionAsync(cancellationToken)
                                                  .ConfigureAwait(false);

    try
    {
      await using var batch = new NpgsqlBatch(connection,
                                              transaction);

      foreach (var task in tasks)
      {
        var insertCmd = new NpgsqlBatchCommand(@"
INSERT INTO tasks (
  session_id, task_id, owner_pod_id, owner_pod_name, payload_id,
  parent_task_ids, data_dependencies, expected_output_ids,
  initial_task_id, created_by, retry_of_ids, status, status_message,
  options_options, options_max_duration, options_max_retries, options_priority,
  options_partition_id, options_app_name, options_app_version, options_app_namespace,
  options_app_service, options_engine_type,
  creation_date, submitted_date, start_date, end_date, reception_date,
  acquisition_date, processed_date, fetched_date, pod_ttl,
  processing_to_end_duration, creation_to_end_duration, received_to_end_duration,
  output_status, output_error
) VALUES (
  @session_id, @task_id, @owner_pod_id, @owner_pod_name, @payload_id,
  @parent_task_ids, @data_dependencies, @expected_output_ids,
  @initial_task_id, @created_by, @retry_of_ids, @status, @status_message,
  @options_options, @options_max_duration, @options_max_retries, @options_priority,
  @options_partition_id, @options_app_name, @options_app_version, @options_app_namespace,
  @options_app_service, @options_engine_type,
  @creation_date, @submitted_date, @start_date, @end_date, @reception_date,
  @acquisition_date, @processed_date, @fetched_date, @pod_ttl,
  @processing_to_end_duration, @creation_to_end_duration, @received_to_end_duration,
  @output_status, @output_error
)");
        SqlHelper.AddTaskInsertParameters(insertCmd.Parameters,
                                          task);
        batch.BatchCommands.Add(insertCmd);

        if (task.RemainingDataDependencies.Count > 0)
        {
          var depCmd = new NpgsqlBatchCommand(@"
INSERT INTO task_remaining_dependencies (task_id, dependency_id)
SELECT @dep_task_id, unnest(@dep_ids)");
          depCmd.Parameters.AddWithValue("dep_task_id",
                                         task.TaskId);
          depCmd.Parameters.AddWithValue("dep_ids",
                                         NpgsqlDbType.Array | NpgsqlDbType.Text,
                                         task.RemainingDataDependencies.Keys.ToArray());
          batch.BatchCommands.Add(depCmd);
        }
      }

      await batch.ExecuteNonQueryAsync(cancellationToken)
                 .ConfigureAwait(false);
    }
    catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.UniqueViolation)
    {
      throw new TaskAlreadyExistsException("A task already exists",
                                           e);
    }

    await transaction.CommitAsync(cancellationToken)
                     .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task<T> ReadTaskAsync<T>(string                        taskId,
                                        Expression<Func<TaskData, T>> selector,
                                        CancellationToken             cancellationToken = default)
  {
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    await using var cmd = connection.CreateCommand();
    cmd.CommandText = "SELECT * FROM tasks WHERE task_id = @task_id";
    cmd.Parameters.AddWithValue("task_id",
                                taskId);

    await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
                                      .ConfigureAwait(false);

    if (!await reader.ReadAsync(cancellationToken)
                     .ConfigureAwait(false))
    {
      throw new TaskNotFoundException($"Task '{taskId}' not found.");
    }

    var taskData = RowMapper.MapToTaskData(reader);
    await reader.CloseAsync()
                .ConfigureAwait(false);

    // Load remaining dependencies
    taskData = await LoadRemainingDependencies(connection,
                                               taskData,
                                               cancellationToken)
                 .ConfigureAwait(false);

    return selector.Compile()
                   .Invoke(taskData);
  }

  /// <inheritdoc />
  public async Task<IEnumerable<TaskStatusCount>> CountTasksAsync(Expression<Func<TaskData, bool>> filter,
                                                                  CancellationToken                cancellationToken = default)
  {
    var (whereSql, whereParams) = ExpressionToSql<TaskData>.Translate(filter);

    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    await using var cmd = connection.CreateCommand();
    cmd.CommandText = $"SELECT status, COUNT(*) as cnt FROM tasks WHERE {whereSql} GROUP BY status";
    SqlHelper.AddExpressionParameters(cmd,
                                      whereParams);

    await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
                                      .ConfigureAwait(false);

    var results = new List<TaskStatusCount>();
    while (await reader.ReadAsync(cancellationToken)
                       .ConfigureAwait(false))
    {
      results.Add(new TaskStatusCount((TaskStatus)reader.GetInt32(0),
                                      (int)reader.GetInt64(1)));
    }

    return results;
  }

  /// <inheritdoc />
  public async Task<IEnumerable<PartitionTaskStatusCount>> CountPartitionTasksAsync(CancellationToken cancellationToken = default)
  {
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    await using var cmd = connection.CreateCommand();
    cmd.CommandText = "SELECT options_partition_id, status, COUNT(*) as cnt FROM tasks GROUP BY options_partition_id, status";

    await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
                                      .ConfigureAwait(false);

    var results = new List<PartitionTaskStatusCount>();
    while (await reader.ReadAsync(cancellationToken)
                       .ConfigureAwait(false))
    {
      results.Add(new PartitionTaskStatusCount(reader.GetString(0),
                                               (TaskStatus)reader.GetInt32(1),
                                               (int)reader.GetInt64(2)));
    }

    return results;
  }

  /// <inheritdoc />
  public async Task<int> CountAllTasksAsync(TaskStatus        status,
                                            CancellationToken cancellationToken = default)
  {
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    await using var cmd = connection.CreateCommand();
    cmd.CommandText = "SELECT COUNT(*) FROM tasks WHERE status = @status";
    cmd.Parameters.AddWithValue("status",
                                (int)status);

    var result = await cmd.ExecuteScalarAsync(cancellationToken)
                          .ConfigureAwait(false);
    return Convert.ToInt32(result);
  }

  /// <inheritdoc />
  public async Task DeleteTaskAsync(string            id,
                                    CancellationToken cancellationToken = default)
  {
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    await using var cmd = connection.CreateCommand();
    cmd.CommandText = "DELETE FROM tasks WHERE task_id = @task_id";
    cmd.Parameters.AddWithValue("task_id",
                                id);

    var deleted = await cmd.ExecuteNonQueryAsync(cancellationToken)
                           .ConfigureAwait(false);

    if (deleted == 0)
    {
      throw new TaskNotFoundException($"Task '{id}' not found.");
    }
  }

  /// <inheritdoc />
  public async Task DeleteTasksAsync(string            sessionId,
                                     CancellationToken cancellationToken = default)
  {
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    await using var cmd = connection.CreateCommand();
    cmd.CommandText = "DELETE FROM tasks WHERE session_id = @session_id";
    cmd.Parameters.AddWithValue("session_id",
                                sessionId);

    var deleted = await cmd.ExecuteNonQueryAsync(cancellationToken)
                           .ConfigureAwait(false);

    if (deleted > 0)
    {
      Logger.LogDebug("Deleted Tasks from {sessionId}",
                      sessionId);
    }
  }

  /// <inheritdoc />
  public async Task DeleteTasksAsync(ICollection<string> taskIds,
                                     CancellationToken   cancellationToken = default)
  {
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    await using var cmd = connection.CreateCommand();
    cmd.CommandText = "DELETE FROM tasks WHERE task_id = ANY(@task_ids)";
    cmd.Parameters.AddWithValue("task_ids",
                                NpgsqlDbType.Array | NpgsqlDbType.Text,
                                taskIds.ToArray());

    await cmd.ExecuteNonQueryAsync(cancellationToken)
             .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task<(IEnumerable<T> tasks, long totalCount)> ListTasksAsync<T>(Expression<Func<TaskData, bool>>    filter,
                                                                               Expression<Func<TaskData, object?>> orderField,
                                                                               Expression<Func<TaskData, T>>       selector,
                                                                               bool                                ascOrder,
                                                                               int                                 page,
                                                                               int                                 pageSize,
                                                                               CancellationToken                   cancellationToken = default)
  {
    var (whereSql, whereParams) = ExpressionToSql<TaskData>.Translate(filter);
    var orderColumn             = ExpressionToSql<TaskData>.TranslateOrderBy(orderField);
    var orderDir                = ascOrder ? "ASC" : "DESC";

    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    // Count query
    await using var countCmd = connection.CreateCommand();
    countCmd.CommandText = $"SELECT COUNT(*) FROM tasks WHERE {whereSql}";
    SqlHelper.AddExpressionParameters(countCmd,
                                      whereParams);
    var totalCount = Convert.ToInt64(await countCmd.ExecuteScalarAsync(cancellationToken)
                                                   .ConfigureAwait(false));

    if (pageSize <= 0)
    {
      return (Enumerable.Empty<T>(), totalCount);
    }

    // Data query
    await using var dataCmd = connection.CreateCommand();
    dataCmd.CommandText = $"SELECT * FROM tasks WHERE {whereSql} ORDER BY {orderColumn} {orderDir} LIMIT @limit OFFSET @offset";
    SqlHelper.AddExpressionParameters(dataCmd,
                                      whereParams);
    dataCmd.Parameters.AddWithValue("limit",
                                    pageSize);
    dataCmd.Parameters.AddWithValue("offset",
                                    page * pageSize);

    await using var reader = await dataCmd.ExecuteReaderAsync(cancellationToken)
                                          .ConfigureAwait(false);

    var compiled = selector.Compile();
    var results  = new List<T>();
    while (await reader.ReadAsync(cancellationToken)
                       .ConfigureAwait(false))
    {
      var taskData = RowMapper.MapToTaskData(reader);
      results.Add(compiled.Invoke(taskData));
    }

    return (results, totalCount);
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<T> FindTasksAsync<T>(Expression<Func<TaskData, bool>>                filter,
                                                     Expression<Func<TaskData, T>>                   selector,
                                                     [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    var (whereSql, whereParams) = ExpressionToSql<TaskData>.Translate(filter);

    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    await using var cmd = connection.CreateCommand();
    cmd.CommandText = $"SELECT * FROM tasks WHERE {whereSql}";
    SqlHelper.AddExpressionParameters(cmd,
                                      whereParams);

    await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
                                      .ConfigureAwait(false);

    var compiled = selector.Compile();
    while (await reader.ReadAsync(cancellationToken)
                       .ConfigureAwait(false))
    {
      var taskData = RowMapper.MapToTaskData(reader);
      yield return compiled.Invoke(taskData);
    }
  }

  /// <inheritdoc />
  public async Task<TaskData?> UpdateOneTask(string                            taskId,
                                             Expression<Func<TaskData, bool>>? filter,
                                             UpdateDefinition<TaskData>        updates,
                                             bool                              before,
                                             CancellationToken                 cancellationToken = default)
  {
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);
    await using var transaction = await connection.BeginTransactionAsync(cancellationToken)
                                                  .ConfigureAwait(false);

    // Build WHERE clause
    var whereClause = "task_id = @taskId";
    var filterParams = new Dictionary<string, object?>
                       {
                         { "@taskId", taskId },
                       };

    if (filter is not null)
    {
      var (filterSql, fParams) = ExpressionToSql<TaskData>.Translate(filter);
      whereClause              = $"{whereClause} AND {filterSql}";
      foreach (var p in fParams)
      {
        filterParams[p.Key] = p.Value;
      }
    }

    // Read before if requested (FOR UPDATE locks the row to prevent TOCTOU between read and update)
    TaskData? result = null;
    if (before)
    {
      result = await ReadTaskByWhereClause(connection,
                                           transaction,
                                           whereClause,
                                           filterParams,
                                           cancellationToken,
                                           forUpdate: true)
                 .ConfigureAwait(false);
    }

    // Build SET clause
    await using var updateCmd = connection.CreateCommand();
    updateCmd.Transaction = transaction;
    var setClauses = SqlHelper.BuildSetClauses(updates,
                                              updateCmd);
    updateCmd.CommandText = $"UPDATE tasks SET {setClauses} WHERE {whereClause}";
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

    // Read after if not before
    if (!before)
    {
      result = await ReadTaskByWhereClause(connection,
                                           transaction,
                                           "task_id = @taskId",
                                           new Dictionary<string, object?>
                                           {
                                             { "@taskId", taskId },
                                           },
                                           cancellationToken)
                 .ConfigureAwait(false);
    }

    await transaction.CommitAsync(cancellationToken)
                     .ConfigureAwait(false);

    return result;
  }

  /// <inheritdoc />
  public async Task<long> UpdateManyTasks(Expression<Func<TaskData, bool>> filter,
                                          UpdateDefinition<TaskData>       updates,
                                          CancellationToken                cancellationToken = default)
  {
    var (whereSql, whereParams) = ExpressionToSql<TaskData>.Translate(filter);

    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    await using var cmd = connection.CreateCommand();
    var setClauses = SqlHelper.BuildSetClauses(updates,
                                              cmd);
    cmd.CommandText = $"UPDATE tasks SET {setClauses} WHERE {whereSql}";
    SqlHelper.AddExpressionParameters(cmd,
                                      whereParams);

    var matched = await cmd.ExecuteNonQueryAsync(cancellationToken)
                           .ConfigureAwait(false);

    return matched;
  }

  /// <inheritdoc />
  public async Task<(IEnumerable<Application> applications, int totalCount)> ListApplicationsAsync(Expression<Func<TaskData, bool>>                    filter,
                                                                                                   ICollection<Expression<Func<Application, object?>>> orderFields,
                                                                                                   bool                                                ascOrder,
                                                                                                   int                                                 page,
                                                                                                   int                                                 pageSize,
                                                                                                   CancellationToken                                   cancellationToken = default)
  {
    var (whereSql, whereParams) = ExpressionToSql<TaskData>.Translate(filter);
    var orderDir                = ascOrder ? "ASC" : "DESC";

    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    // Count distinct applications
    await using var countCmd = connection.CreateCommand();
    countCmd.CommandText = $@"
SELECT COUNT(*) FROM (
  SELECT DISTINCT options_app_name, options_app_namespace, options_app_version, options_app_service
  FROM tasks WHERE {whereSql}
) sub";
    SqlHelper.AddExpressionParameters(countCmd,
                                      whereParams);
    var totalCount = Convert.ToInt32(await countCmd.ExecuteScalarAsync(cancellationToken)
                                                   .ConfigureAwait(false));

    // Get paginated applications
    var orderClauses = orderFields.Select(f => $"{ExpressionToSql<Application>.TranslateOrderBy(f)} {orderDir}");
    var orderBySql   = string.Join(", ",
                                   orderClauses);

    await using var dataCmd = connection.CreateCommand();
    dataCmd.CommandText = $@"
SELECT DISTINCT options_app_name, options_app_namespace, options_app_version, options_app_service
FROM tasks WHERE {whereSql}
ORDER BY {orderBySql}
LIMIT @limit OFFSET @offset";
    SqlHelper.AddExpressionParameters(dataCmd,
                                      whereParams);
    dataCmd.Parameters.AddWithValue("limit",
                                    pageSize);
    dataCmd.Parameters.AddWithValue("offset",
                                    page * pageSize);

    await using var reader = await dataCmd.ExecuteReaderAsync(cancellationToken)
                                          .ConfigureAwait(false);

    var results = new List<Application>();
    while (await reader.ReadAsync(cancellationToken)
                       .ConfigureAwait(false))
    {
      results.Add(new Application(reader.GetString(0),
                                  reader.GetString(1),
                                  reader.GetString(2),
                                  reader.GetString(3)));
    }

    return (results, totalCount);
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<T> RemoveRemainingDataDependenciesAsync<T>(ICollection<string>                        taskIds,
                                                                           ICollection<string>                        dependenciesToRemove,
                                                                           Expression<Func<TaskData, T>>              selector,
                                                                           [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    if (dependenciesToRemove.Count == 0)
    {
      yield break;
    }

    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    // Remove dependencies
    await using var deleteCmd = connection.CreateCommand();
    deleteCmd.CommandText = "DELETE FROM task_remaining_dependencies WHERE task_id = ANY(@task_ids) AND dependency_id = ANY(@dep_ids)";
    deleteCmd.Parameters.AddWithValue("task_ids",
                                      NpgsqlDbType.Array | NpgsqlDbType.Text,
                                      taskIds.ToArray());
    deleteCmd.Parameters.AddWithValue("dep_ids",
                                      NpgsqlDbType.Array | NpgsqlDbType.Text,
                                      dependenciesToRemove.ToArray());

    await deleteCmd.ExecuteNonQueryAsync(cancellationToken)
                   .ConfigureAwait(false);

    // Find tasks that are now ready (no remaining dependencies)
    await using var readyCmd = connection.CreateCommand();
    readyCmd.CommandText = @"
SELECT t.* FROM tasks t
WHERE t.task_id = ANY(@ready_task_ids)
  AND t.status IN (@status_creating, @status_pending)
  AND NOT EXISTS (
    SELECT 1 FROM task_remaining_dependencies d WHERE d.task_id = t.task_id
  )";
    readyCmd.Parameters.AddWithValue("ready_task_ids",
                                     NpgsqlDbType.Array | NpgsqlDbType.Text,
                                     taskIds.ToArray());
    readyCmd.Parameters.AddWithValue("status_creating",
                                     (int)TaskStatus.Creating);
    readyCmd.Parameters.AddWithValue("status_pending",
                                     (int)TaskStatus.Pending);

    await using var reader = await readyCmd.ExecuteReaderAsync(cancellationToken)
                                           .ConfigureAwait(false);

    var compiled = selector.Compile();
    while (await reader.ReadAsync(cancellationToken)
                       .ConfigureAwait(false))
    {
      var taskData = RowMapper.MapToTaskData(reader);
      yield return compiled.Invoke(taskData);
    }
  }

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
    => connectionProvider_.Init(cancellationToken);

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => connectionProvider_.Check(tag);

  private async Task<TaskData?> ReadTaskByWhereClause(NpgsqlConnection             connection,
                                                      NpgsqlTransaction            transaction,
                                                      string                       whereClause,
                                                      Dictionary<string, object?> parameters,
                                                      CancellationToken            cancellationToken,
                                                      bool                         forUpdate = false)
  {
    await using var cmd = connection.CreateCommand();
    cmd.Transaction = transaction;
    cmd.CommandText = $"SELECT * FROM tasks WHERE {whereClause}{(forUpdate ? " FOR UPDATE" : string.Empty)}";
    SqlHelper.AddExpressionParameters(cmd,
                                      parameters);

    await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
                                      .ConfigureAwait(false);

    if (!await reader.ReadAsync(cancellationToken)
                     .ConfigureAwait(false))
    {
      return null;
    }

    var taskData = RowMapper.MapToTaskData(reader);
    await reader.CloseAsync()
                .ConfigureAwait(false);

    return await LoadRemainingDependencies(connection,
                                           taskData,
                                           cancellationToken,
                                           transaction)
             .ConfigureAwait(false);
  }

  private static async Task<TaskData> LoadRemainingDependencies(NpgsqlConnection   connection,
                                                                TaskData           taskData,
                                                                CancellationToken  cancellationToken,
                                                                NpgsqlTransaction? transaction = null)
  {
    await using var depCmd = connection.CreateCommand();
    if (transaction is not null)
    {
      depCmd.Transaction = transaction;
    }

    depCmd.CommandText = "SELECT dependency_id FROM task_remaining_dependencies WHERE task_id = @task_id";
    depCmd.Parameters.AddWithValue("task_id",
                                   taskData.TaskId);

    await using var depReader = await depCmd.ExecuteReaderAsync(cancellationToken)
                                            .ConfigureAwait(false);

    var deps = new Dictionary<string, bool>();
    while (await depReader.ReadAsync(cancellationToken)
                          .ConfigureAwait(false))
    {
      deps[depReader.GetString(0)] = true;
    }

    return taskData with
           {
             RemainingDataDependencies = deps,
           };
  }
}
