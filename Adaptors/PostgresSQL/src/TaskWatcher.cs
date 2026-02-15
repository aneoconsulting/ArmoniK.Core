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

using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

namespace ArmoniK.Core.Adapters.PostgresSQL;

/// <inheritdoc cref="ITaskWatcher" />
public class TaskWatcher : ITaskWatcher
{
  private readonly NpgsqlConnectionProvider connectionProvider_;
  private readonly ILogger<TaskWatcher>     logger_;

  /// <summary>
  ///   Creates a new TaskWatcher
  /// </summary>
  public TaskWatcher(NpgsqlConnectionProvider connectionProvider,
                     ILogger<TaskWatcher>     logger)
  {
    connectionProvider_ = connectionProvider;
    logger_             = logger;
  }

  /// <inheritdoc />
  public async Task<IAsyncEnumerable<NewTask>> GetNewTasks(Expression<Func<TaskData, bool>> filter,
                                                           CancellationToken                cancellationToken = default)
    => WatchNewTasks(filter,
                     cancellationToken);

  /// <inheritdoc />
  public async Task<IAsyncEnumerable<TaskStatusUpdate>> GetTaskStatusUpdates(Expression<Func<TaskData, bool>> filter,
                                                                             CancellationToken                cancellationToken = default)
    => WatchTaskStatusUpdates(filter,
                              cancellationToken);

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
    => connectionProvider_.Init(cancellationToken);

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => connectionProvider_.Check(tag);

  private async IAsyncEnumerable<NewTask> WatchNewTasks(Expression<Func<TaskData, bool>>              filter,
                                                        [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    var compiled = filter.Compile();
    var channel  = Channel.CreateUnbounded<string>();

    await using var listenConn = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    listenConn.Notification += (_, args) => channel.Writer.TryWrite(args.Payload);

    await using var listenCmd = listenConn.CreateCommand();
    listenCmd.CommandText = "LISTEN task_insert";
    await listenCmd.ExecuteNonQueryAsync(cancellationToken)
                   .ConfigureAwait(false);

    // Start background task to keep waiting for notifications
    _ = WaitForNotifications(listenConn,
                             cancellationToken);

    await foreach (var payload in channel.Reader.ReadAllAsync(cancellationToken)
                                         .ConfigureAwait(false))
    {
      // Payload format: task_id|session_id
      var parts = payload.Split('|');
      if (parts.Length < 2)
      {
        continue;
      }

      var taskId = parts[0];

      TaskData? taskData;
      try
      {
        await using var fetchConn = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                              .ConfigureAwait(false);
        await using var fetchCmd = fetchConn.CreateCommand();
        fetchCmd.CommandText = "SELECT * FROM tasks WHERE task_id = @task_id";
        fetchCmd.Parameters.AddWithValue("task_id",
                                         taskId);
        await using var reader = await fetchCmd.ExecuteReaderAsync(cancellationToken)
                                               .ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken)
                         .ConfigureAwait(false))
        {
          continue;
        }

        taskData = RowMapper.MapToTaskData(reader);
      }
      catch (Exception e)
      {
        logger_.LogWarning(e,
                           "Failed to fetch task {taskId} for watcher",
                           taskId);
        continue;
      }

      if (!compiled.Invoke(taskData))
      {
        continue;
      }

      yield return new NewTask(taskData.SessionId,
                               taskData.TaskId,
                               taskData.InitialTaskId,
                               taskData.PayloadId,
                               taskData.ParentTaskIds,
                               taskData.ExpectedOutputIds,
                               taskData.DataDependencies,
                               taskData.RetryOfIds,
                               taskData.Status);
    }
  }

  private async IAsyncEnumerable<TaskStatusUpdate> WatchTaskStatusUpdates(Expression<Func<TaskData, bool>>              filter,
                                                                          [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    var compiled = filter.Compile();
    var channel  = Channel.CreateUnbounded<string>();

    await using var listenConn = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    listenConn.Notification += (_, args) => channel.Writer.TryWrite(args.Payload);

    await using var listenCmd = listenConn.CreateCommand();
    listenCmd.CommandText = "LISTEN task_status";
    await listenCmd.ExecuteNonQueryAsync(cancellationToken)
                   .ConfigureAwait(false);

    _ = WaitForNotifications(listenConn,
                             cancellationToken);

    await foreach (var payload in channel.Reader.ReadAllAsync(cancellationToken)
                                         .ConfigureAwait(false))
    {
      // Payload format: task_id|session_id|status
      var parts = payload.Split('|');
      if (parts.Length < 3)
      {
        continue;
      }

      var taskId    = parts[0];
      var sessionId = parts[1];

      if (!int.TryParse(parts[2],
                        out var statusInt))
      {
        continue;
      }

      var status = (TaskStatus)statusInt;

      TaskData? taskData;
      try
      {
        await using var fetchConn = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                              .ConfigureAwait(false);
        await using var fetchCmd = fetchConn.CreateCommand();
        fetchCmd.CommandText = "SELECT * FROM tasks WHERE task_id = @task_id";
        fetchCmd.Parameters.AddWithValue("task_id",
                                         taskId);
        await using var reader = await fetchCmd.ExecuteReaderAsync(cancellationToken)
                                               .ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken)
                         .ConfigureAwait(false))
        {
          continue;
        }

        taskData = RowMapper.MapToTaskData(reader);
      }
      catch (Exception e)
      {
        logger_.LogWarning(e,
                           "Failed to fetch task {taskId} for watcher",
                           taskId);
        continue;
      }

      if (!compiled.Invoke(taskData))
      {
        continue;
      }

      yield return new TaskStatusUpdate(sessionId,
                                        taskId,
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
