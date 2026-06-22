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

using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

namespace ArmoniK.Core.Adapters.PostgresSQL;

/// <inheritdoc cref="ITaskWatcher" />
public class TaskWatcher : ITaskWatcher
{
  private readonly NpgsqlConnectionProvider connectionProvider_;

  /// <summary>
  ///   Creates a new TaskWatcher
  /// </summary>
  public TaskWatcher(NpgsqlConnectionProvider connectionProvider)
    => connectionProvider_ = connectionProvider;

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
    var slotName = $"armonik_{Guid.NewGuid():N}";
    var options  = new PgOutputReplicationOptions("armonik_pub", PgOutputProtocolVersion.V1, binary: true);

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

      if (message is not InsertMessage insert || insert.Relation.RelationName != "tasks")
      {
        await WalHelpers.ConsumeMessage(message,
                                        cancellationToken)
                        .ConfigureAwait(false);
        continue;
      }

      var taskData = await WalHelpers.ReadTaskData(insert.NewRow, cancellationToken).ConfigureAwait(false);

      if (!compiled(taskData))
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
    var slotName = $"armonik_{Guid.NewGuid():N}";
    var options  = new PgOutputReplicationOptions("armonik_pub", PgOutputProtocolVersion.V1, binary: true);

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

      // DEFAULT replica identity: the WAL does not carry old row values, so we cannot
      // compare old vs new status to suppress no-op events. Every task UPDATE reaches
      // this watcher — including timestamp-only changes (e.g. acquisition_date).
      // Trade-off: consumers must tolerate receiving the same status twice. This matches
      // the MongoDB adaptor behaviour, which also cannot filter on updated fields server-side.
      if (message is not UpdateMessage update || update.Relation.RelationName != "tasks")
      {
        await WalHelpers.ConsumeMessage(message,
                                        cancellationToken)
                        .ConfigureAwait(false);
        continue;
      }

      // With DEFAULT replica identity, Npgsql surfaces the old PK as an IndexUpdateMessage.Key
      // tuple that must be consumed before NewRow can be read.
      await WalHelpers.ConsumeOldTuple(update,
                                       cancellationToken)
                      .ConfigureAwait(false);

      var taskData = await WalHelpers.ReadTaskData(update.NewRow, cancellationToken).ConfigureAwait(false);

      if (!compiled(taskData))
      {
        continue;
      }

      yield return new TaskStatusUpdate(taskData.SessionId,
                                        taskData.TaskId,
                                        taskData.Status);
    }
  }

}
