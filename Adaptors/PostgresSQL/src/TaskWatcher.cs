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

using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

namespace ArmoniK.Core.Adapters.PostgresSQL;

/// <inheritdoc cref="ITaskWatcher" />
public class TaskWatcher : ITaskWatcher, IDisposable
{
  private readonly NpgsqlConnectionProvider      connectionProvider_;
  private readonly WalBroadcaster<TaskData> insertBroadcaster_;
  private readonly WalBroadcaster<TaskData> updateBroadcaster_;

  /// <summary>
  ///   Creates a new TaskWatcher
  /// </summary>
  public TaskWatcher(NpgsqlConnectionProvider connectionProvider)
  {
    connectionProvider_ = connectionProvider;

    insertBroadcaster_ = new WalBroadcaster<TaskData>(connectionProvider,
                                                       async (message, ct) =>
                                                       {
                                                         if (message is InsertMessage insert && insert.Relation.RelationName == "tasks")
                                                           return await WalHelpers.ReadTaskData(insert.NewRow,
                                                                                                ct)
                                                                                  .ConfigureAwait(false);
                                                         await WalHelpers.ConsumeMessage(message,
                                                                                         ct)
                                                                         .ConfigureAwait(false);
                                                         return null;
                                                       });

    updateBroadcaster_ = new WalBroadcaster<TaskData>(connectionProvider,
                                                       async (message, ct) =>
                                                       {
                                                         if (message is UpdateMessage update && update.Relation.RelationName == "tasks")
                                                         {
                                                           await WalHelpers.ConsumeOldTuple(update,
                                                                                            ct)
                                                                           .ConfigureAwait(false);
                                                           return await WalHelpers.ReadTaskData(update.NewRow,
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
  public async Task<IAsyncEnumerable<NewTask>> GetNewTasks(Expression<Func<TaskData, bool>> filter,
                                                           CancellationToken                cancellationToken = default)
  {
    var compiled  = filter.Compile();
    var rawStream = await insertBroadcaster_.SubscribeAsync(cancellationToken)
                                            .ConfigureAwait(false);
    return FilterNewTasks(rawStream,
                          compiled,
                          cancellationToken);
  }

  /// <inheritdoc />
  public async Task<IAsyncEnumerable<TaskStatusUpdate>> GetTaskStatusUpdates(Expression<Func<TaskData, bool>> filter,
                                                                             CancellationToken                cancellationToken = default)
  {
    // DEFAULT replica identity: the WAL does not carry old row values, so we cannot
    // compare old vs new status to suppress no-op events. Every task UPDATE reaches
    // this watcher — including timestamp-only changes (e.g. acquisition_date).
    // Trade-off: consumers must tolerate receiving the same status twice. This matches
    // the MongoDB adaptor behaviour, which also cannot filter on updated fields server-side.
    var compiled  = filter.Compile();
    var rawStream = await updateBroadcaster_.SubscribeAsync(cancellationToken)
                                            .ConfigureAwait(false);
    return FilterTaskStatusUpdates(rawStream,
                                   compiled,
                                   cancellationToken);
  }

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
    => connectionProvider_.Init(cancellationToken);

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => connectionProvider_.Check(tag);

  private static async IAsyncEnumerable<NewTask> FilterNewTasks(IAsyncEnumerable<TaskData>                    source,
                                                                  Func<TaskData, bool>                          compiled,
                                                                  [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    await foreach (var taskData in source.WithCancellation(cancellationToken)
                                         .ConfigureAwait(false))
    {
      if (!compiled(taskData))
        continue;

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

  private static async IAsyncEnumerable<TaskStatusUpdate> FilterTaskStatusUpdates(IAsyncEnumerable<TaskData>                    source,
                                                                                    Func<TaskData, bool>                          compiled,
                                                                                    [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    await foreach (var taskData in source.WithCancellation(cancellationToken)
                                         .ConfigureAwait(false))
    {
      if (!compiled(taskData))
        continue;

      yield return new TaskStatusUpdate(taskData.SessionId,
                                        taskData.TaskId,
                                        taskData.Status);
    }
  }
}
