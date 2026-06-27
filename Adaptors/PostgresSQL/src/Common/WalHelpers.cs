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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Storage;

using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;

namespace ArmoniK.Core.Adapters.PostgresSQL.Common;

internal static class WalHelpers
{
  /// <summary>
  ///   Reads a WAL tuple directly into a <see cref="TaskData" /> in a single pass.
  ///   Columns are consumed in stream order; binary mode (enabled on the replication slot)
  ///   means integer, boolean, timestamp and array columns are read without text parsing.
  ///   NULL and unchanged-TOAST values are skipped — variables keep their initialised defaults.
  ///   RemainingDataDependencies is always empty: it lives in a separate table not present in WAL.
  /// </summary>
  internal static async Task<TaskData> ReadTaskData(ReplicationTuple  tuple,
                                                    CancellationToken ct)
  {
    string sessionId       = "",
           taskId          = "",
           ownerPodId      = "",
           ownerPodName    = "",
           payloadId       = "",
           initialTaskId   = "",
           createdBy       = "",
           statusMessage   = "",
           outputError     = "",
           optOptions      = "{}",
           optPartitionId  = "",
           optAppName      = "",
           optAppVersion   = "",
           optAppNamespace = "",
           optAppService   = "",
           optEngineType   = "";

    var parentTaskIds     = Array.Empty<string>();
    var dataDeps          = Array.Empty<string>();
    var expectedOutputIds = Array.Empty<string>();
    var retryOfIds        = Array.Empty<string>();

    var status        = 0;
    var outputStatus  = 0;
    var optMaxRetries = 0;
    var optPriority   = 0;
    var optMaxDuration = 0L;

    var      creationDate    = default(DateTime);
    DateTime? submittedDate  = null,
              startDate      = null,
              endDate        = null,
              receptionDate  = null,
              acquisitionDate = null,
              processedDate  = null,
              fetchedDate    = null,
              podTtl         = null;

    TimeSpan? processingToEnd = null,
              creationToEnd   = null,
              receivedToEnd   = null;

    await foreach (var col in tuple)
    {
      if (col.IsDBNull || col.IsUnchangedToastedValue)
      {
        continue;
      }

      switch (col.GetFieldName())
      {
        case "session_id":
          sessionId = await col.Get<string>(ct).ConfigureAwait(false);
          break;
        case "task_id":
          taskId = await col.Get<string>(ct).ConfigureAwait(false);
          break;
        case "owner_pod_id":
          ownerPodId = await col.Get<string>(ct).ConfigureAwait(false);
          break;
        case "owner_pod_name":
          ownerPodName = await col.Get<string>(ct).ConfigureAwait(false);
          break;
        case "payload_id":
          payloadId = await col.Get<string>(ct).ConfigureAwait(false);
          break;
        case "initial_task_id":
          initialTaskId = await col.Get<string>(ct).ConfigureAwait(false);
          break;
        case "created_by":
          createdBy = await col.Get<string>(ct).ConfigureAwait(false);
          break;
        case "status_message":
          statusMessage = await col.Get<string>(ct).ConfigureAwait(false);
          break;
        case "output_error":
          outputError = await col.Get<string>(ct).ConfigureAwait(false);
          break;
        case "options_options":
          optOptions = await col.Get<string>(ct).ConfigureAwait(false);
          break;
        case "options_partition_id":
          optPartitionId = await col.Get<string>(ct).ConfigureAwait(false);
          break;
        case "options_app_name":
          optAppName = await col.Get<string>(ct).ConfigureAwait(false);
          break;
        case "options_app_version":
          optAppVersion = await col.Get<string>(ct).ConfigureAwait(false);
          break;
        case "options_app_namespace":
          optAppNamespace = await col.Get<string>(ct).ConfigureAwait(false);
          break;
        case "options_app_service":
          optAppService = await col.Get<string>(ct).ConfigureAwait(false);
          break;
        case "options_engine_type":
          optEngineType = await col.Get<string>(ct).ConfigureAwait(false);
          break;
        case "parent_task_ids":
          parentTaskIds = await col.Get<string[]>(ct).ConfigureAwait(false);
          break;
        case "data_dependencies":
          dataDeps = await col.Get<string[]>(ct).ConfigureAwait(false);
          break;
        case "expected_output_ids":
          expectedOutputIds = await col.Get<string[]>(ct).ConfigureAwait(false);
          break;
        case "retry_of_ids":
          retryOfIds = await col.Get<string[]>(ct).ConfigureAwait(false);
          break;
        case "status":
          status = await col.Get<int>(ct).ConfigureAwait(false);
          break;
        case "output_status":
          outputStatus = await col.Get<int>(ct).ConfigureAwait(false);
          break;
        case "options_max_retries":
          optMaxRetries = await col.Get<int>(ct).ConfigureAwait(false);
          break;
        case "options_priority":
          optPriority = await col.Get<int>(ct).ConfigureAwait(false);
          break;
        case "options_max_duration":
          optMaxDuration = await col.Get<long>(ct).ConfigureAwait(false);
          break;
        case "creation_date":
          creationDate = UtcKind(await col.Get<DateTime>(ct).ConfigureAwait(false));
          break;
        case "submitted_date":
          submittedDate = UtcKind(await col.Get<DateTime>(ct).ConfigureAwait(false));
          break;
        case "start_date":
          startDate = UtcKind(await col.Get<DateTime>(ct).ConfigureAwait(false));
          break;
        case "end_date":
          endDate = UtcKind(await col.Get<DateTime>(ct).ConfigureAwait(false));
          break;
        case "reception_date":
          receptionDate = UtcKind(await col.Get<DateTime>(ct).ConfigureAwait(false));
          break;
        case "acquisition_date":
          acquisitionDate = UtcKind(await col.Get<DateTime>(ct).ConfigureAwait(false));
          break;
        case "processed_date":
          processedDate = UtcKind(await col.Get<DateTime>(ct).ConfigureAwait(false));
          break;
        case "fetched_date":
          fetchedDate = UtcKind(await col.Get<DateTime>(ct).ConfigureAwait(false));
          break;
        case "pod_ttl":
          podTtl = UtcKind(await col.Get<DateTime>(ct).ConfigureAwait(false));
          break;
        case "processing_to_end_duration":
          processingToEnd = TimeSpan.FromTicks(await col.Get<long>(ct).ConfigureAwait(false));
          break;
        case "creation_to_end_duration":
          creationToEnd = TimeSpan.FromTicks(await col.Get<long>(ct).ConfigureAwait(false));
          break;
        case "received_to_end_duration":
          receivedToEnd = TimeSpan.FromTicks(await col.Get<long>(ct).ConfigureAwait(false));
          break;
        default:
          await col.Get(ct).ConfigureAwait(false);
          break;
      }
    }

    var options = new TaskOptions(DeserializeJsonDict(optOptions),
                                  TimeSpan.FromTicks(optMaxDuration),
                                  optMaxRetries,
                                  optPriority,
                                  optPartitionId,
                                  optAppName,
                                  optAppVersion,
                                  optAppNamespace,
                                  optAppService,
                                  optEngineType);

    var output = new Output((OutputStatus)outputStatus,
                            outputError);

    return new TaskData(sessionId,
                        taskId,
                        ownerPodId,
                        ownerPodName,
                        payloadId,
                        parentTaskIds,
                        dataDeps,
                        new Dictionary<string, bool>(),
                        expectedOutputIds,
                        initialTaskId,
                        createdBy,
                        retryOfIds,
                        (TaskStatus)status,
                        statusMessage,
                        options,
                        creationDate,
                        submittedDate,
                        startDate,
                        endDate,
                        receptionDate,
                        acquisitionDate,
                        processedDate,
                        fetchedDate,
                        podTtl,
                        processingToEnd,
                        creationToEnd,
                        receivedToEnd,
                        output);
  }

  /// <summary>
  ///   Reads a WAL tuple directly into a <see cref="Result" /> in a single pass.
  ///   See <see cref="ReadTaskData" /> for the general approach.
  /// </summary>
  internal static async Task<Result> ReadResult(ReplicationTuple  tuple,
                                                CancellationToken ct)
  {
    string sessionId    = "",
           resultId     = "",
           name         = "",
           createdBy    = "",
           completedBy  = "",
           ownerTaskId  = "";

    var dependentTasks = Array.Empty<string>();
    var status         = 0;
    var size           = 0L;
    var manualDeletion = false;
    var opaqueId       = Array.Empty<byte>();

    var      creationDate   = default(DateTime);
    DateTime? completionDate = null;

    await foreach (var col in tuple)
    {
      if (col.IsDBNull || col.IsUnchangedToastedValue)
      {
        continue;
      }

      switch (col.GetFieldName())
      {
        case "session_id":
          sessionId = await col.Get<string>(ct).ConfigureAwait(false);
          break;
        case "result_id":
          resultId = await col.Get<string>(ct).ConfigureAwait(false);
          break;
        case "name":
          name = await col.Get<string>(ct).ConfigureAwait(false);
          break;
        case "created_by":
          createdBy = await col.Get<string>(ct).ConfigureAwait(false);
          break;
        case "completed_by":
          completedBy = await col.Get<string>(ct).ConfigureAwait(false);
          break;
        case "owner_task_id":
          ownerTaskId = await col.Get<string>(ct).ConfigureAwait(false);
          break;
        case "dependent_tasks":
          dependentTasks = await col.Get<string[]>(ct).ConfigureAwait(false);
          break;
        case "status":
          status = await col.Get<int>(ct).ConfigureAwait(false);
          break;
        case "size":
          size = await col.Get<long>(ct).ConfigureAwait(false);
          break;
        case "manual_deletion":
          manualDeletion = await col.Get<bool>(ct).ConfigureAwait(false);
          break;
        case "opaque_id":
          opaqueId = await col.Get<byte[]>(ct).ConfigureAwait(false);
          break;
        case "creation_date":
          creationDate = UtcKind(await col.Get<DateTime>(ct).ConfigureAwait(false));
          break;
        case "completion_date":
          completionDate = UtcKind(await col.Get<DateTime>(ct).ConfigureAwait(false));
          break;
        default:
          await col.Get(ct).ConfigureAwait(false);
          break;
      }
    }

    return new Result(sessionId,
                      resultId,
                      name,
                      createdBy,
                      completedBy,
                      ownerTaskId,
                      (ResultStatus)status,
                      new List<string>(dependentTasks),
                      creationDate,
                      completionDate,
                      size,
                      opaqueId,
                      manualDeletion);
  }

  /// <summary>
  ///   Consumes the old-key or old-row tuple of an UPDATE message so the stream position
  ///   advances past it before the caller reads <see cref="UpdateMessage.NewRow" />.
  ///   With DEFAULT replica identity, PostgreSQL sends the primary-key columns as an old-key
  ///   tuple ('K' tag), which Npgsql surfaces as <see cref="IndexUpdateMessage" />.
  ///   With REPLICA IDENTITY FULL it is a <see cref="FullUpdateMessage" />.
  ///   The base <see cref="UpdateMessage" /> has no old tuple and needs no pre-consumption.
  /// </summary>
  internal static async Task ConsumeOldTuple(UpdateMessage     message,
                                             CancellationToken cancellationToken)
  {
    if (message is FullUpdateMessage full)
    {
      await ConsumeTuple(full.OldRow,
                         cancellationToken)
        .ConfigureAwait(false);
    }
    else if (message is IndexUpdateMessage idx)
    {
      await ConsumeTuple(idx.Key,
                         cancellationToken)
        .ConfigureAwait(false);
    }
  }

  /// <summary>
  ///   Consumes and discards all tuple data in a WAL message.
  ///   Must be called for every message whose tuple is not otherwise read,
  ///   to keep the replication stream position advancing correctly.
  /// </summary>
  internal static async Task ConsumeMessage(PgOutputReplicationMessage message,
                                            CancellationToken          cancellationToken)
  {
    switch (message)
    {
      case InsertMessage insert:
        await ConsumeTuple(insert.NewRow,
                           cancellationToken)
          .ConfigureAwait(false);
        break;

      case FullUpdateMessage update:
        await ConsumeTuple(update.OldRow,
                           cancellationToken)
          .ConfigureAwait(false);
        await ConsumeTuple(update.NewRow,
                           cancellationToken)
          .ConfigureAwait(false);
        break;

      case IndexUpdateMessage update:
        await ConsumeTuple(update.Key,
                           cancellationToken)
          .ConfigureAwait(false);
        await ConsumeTuple(update.NewRow,
                           cancellationToken)
          .ConfigureAwait(false);
        break;

      case UpdateMessage update:
        await ConsumeTuple(update.NewRow,
                           cancellationToken)
          .ConfigureAwait(false);
        break;

      case FullDeleteMessage delete:
        await ConsumeTuple(delete.OldRow,
                           cancellationToken)
          .ConfigureAwait(false);
        break;

      case KeyDeleteMessage delete:
        await ConsumeTuple(delete.Key,
                           cancellationToken)
          .ConfigureAwait(false);
        break;
    }
  }

  // Drains every column in a tuple without allocating strings.
  private static async Task ConsumeTuple(ReplicationTuple  tuple,
                                         CancellationToken ct)
  {
    await foreach (var col in tuple)
    {
      if (!col.IsDBNull && !col.IsUnchangedToastedValue)
      {
        await col.Get(ct).ConfigureAwait(false);
      }
    }
  }

  private static DateTime UtcKind(DateTime dt)
    => DateTime.SpecifyKind(dt,
                            DateTimeKind.Utc);

  private static IDictionary<string, string> DeserializeJsonDict(string json)
  {
    if (string.IsNullOrEmpty(json))
    {
      return new Dictionary<string, string>();
    }

    return JsonSerializer.Deserialize<Dictionary<string, string>>(json) ?? new Dictionary<string, string>();
  }
}
