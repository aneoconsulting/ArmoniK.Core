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
using System.Data;
using System.Linq;
using System.Text.Json;

using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Storage;

using Npgsql;

namespace ArmoniK.Core.Adapters.PostgresSQL.Common;

/// <summary>
///   Maps SQL rows to domain records
/// </summary>
public static class RowMapper
{
  /// <summary>
  ///   Map a data reader row to a TaskData record
  /// </summary>
  /// <param name="reader">The data reader</param>
  /// <returns>The TaskData record</returns>
  public static TaskData MapToTaskData(NpgsqlDataReader reader)
  {
    var options = new TaskOptions(DeserializeJsonDict(reader.GetString(reader.GetOrdinal("options_options"))),
                                 TimeSpan.FromTicks(reader.GetInt64(reader.GetOrdinal("options_max_duration"))),
                                 reader.GetInt32(reader.GetOrdinal("options_max_retries")),
                                 reader.GetInt32(reader.GetOrdinal("options_priority")),
                                 reader.GetString(reader.GetOrdinal("options_partition_id")),
                                 reader.GetString(reader.GetOrdinal("options_app_name")),
                                 reader.GetString(reader.GetOrdinal("options_app_version")),
                                 reader.GetString(reader.GetOrdinal("options_app_namespace")),
                                 reader.GetString(reader.GetOrdinal("options_app_service")),
                                 reader.GetString(reader.GetOrdinal("options_engine_type")));

    var output = new Output((OutputStatus)reader.GetInt32(reader.GetOrdinal("output_status")),
                            reader.GetString(reader.GetOrdinal("output_error")));

    return new TaskData(reader.GetString(reader.GetOrdinal("session_id")),
                        reader.GetString(reader.GetOrdinal("task_id")),
                        reader.GetString(reader.GetOrdinal("owner_pod_id")),
                        reader.GetString(reader.GetOrdinal("owner_pod_name")),
                        reader.GetString(reader.GetOrdinal("payload_id")),
                        GetStringArray(reader,
                                       "parent_task_ids"),
                        GetStringArray(reader,
                                       "data_dependencies"),
                        new Dictionary<string, bool>(), // RemainingDataDependencies loaded separately
                        GetStringArray(reader,
                                       "expected_output_ids"),
                        reader.GetString(reader.GetOrdinal("initial_task_id")),
                        reader.GetString(reader.GetOrdinal("created_by")),
                        GetStringArray(reader,
                                       "retry_of_ids"),
                        (TaskStatus)reader.GetInt32(reader.GetOrdinal("status")),
                        reader.GetString(reader.GetOrdinal("status_message")),
                        options,
                        GetUtcDateTime(reader,
                                       reader.GetOrdinal("creation_date")),
                        GetNullableDateTime(reader,
                                            "submitted_date"),
                        GetNullableDateTime(reader,
                                            "start_date"),
                        GetNullableDateTime(reader,
                                            "end_date"),
                        GetNullableDateTime(reader,
                                            "reception_date"),
                        GetNullableDateTime(reader,
                                            "acquisition_date"),
                        GetNullableDateTime(reader,
                                            "processed_date"),
                        GetNullableDateTime(reader,
                                            "fetched_date"),
                        GetNullableDateTime(reader,
                                            "pod_ttl"),
                        GetNullableTimeSpan(reader,
                                            "processing_to_end_duration"),
                        GetNullableTimeSpan(reader,
                                            "creation_to_end_duration"),
                        GetNullableTimeSpan(reader,
                                            "received_to_end_duration"),
                        output);
  }

  /// <summary>
  ///   Map a data reader row to a SessionData record
  /// </summary>
  /// <param name="reader">The data reader</param>
  /// <returns>The SessionData record</returns>
  public static SessionData MapToSessionData(NpgsqlDataReader reader)
  {
    var options = new TaskOptions(DeserializeJsonDict(reader.GetString(reader.GetOrdinal("options_options"))),
                                 TimeSpan.FromTicks(reader.GetInt64(reader.GetOrdinal("options_max_duration"))),
                                 reader.GetInt32(reader.GetOrdinal("options_max_retries")),
                                 reader.GetInt32(reader.GetOrdinal("options_priority")),
                                 reader.GetString(reader.GetOrdinal("options_partition_id")),
                                 reader.GetString(reader.GetOrdinal("options_app_name")),
                                 reader.GetString(reader.GetOrdinal("options_app_version")),
                                 reader.GetString(reader.GetOrdinal("options_app_namespace")),
                                 reader.GetString(reader.GetOrdinal("options_app_service")),
                                 reader.GetString(reader.GetOrdinal("options_engine_type")));

    return new SessionData(reader.GetString(reader.GetOrdinal("session_id")),
                           (SessionStatus)reader.GetInt32(reader.GetOrdinal("status")),
                           reader.GetBoolean(reader.GetOrdinal("client_submission")),
                           reader.GetBoolean(reader.GetOrdinal("worker_submission")),
                           GetUtcDateTime(reader,
                                          reader.GetOrdinal("creation_date")),
                           GetNullableDateTime(reader,
                                               "cancellation_date"),
                           GetNullableDateTime(reader,
                                               "closure_date"),
                           GetNullableDateTime(reader,
                                               "purge_date"),
                           GetNullableDateTime(reader,
                                               "deletion_date"),
                           GetNullableDateTime(reader,
                                               "deletion_ttl"),
                           GetNullableTimeSpan(reader,
                                               "duration"),
                           GetStringArray(reader,
                                          "partition_ids"),
                           options);
  }

  /// <summary>
  ///   Map a data reader row to a Result record
  /// </summary>
  /// <param name="reader">The data reader</param>
  /// <returns>The Result record</returns>
  public static Result MapToResult(NpgsqlDataReader reader)
    => new(reader.GetString(reader.GetOrdinal("session_id")),
           reader.GetString(reader.GetOrdinal("result_id")),
           reader.GetString(reader.GetOrdinal("name")),
           reader.GetString(reader.GetOrdinal("created_by")),
           reader.GetString(reader.GetOrdinal("completed_by")),
           reader.GetString(reader.GetOrdinal("owner_task_id")),
           (ResultStatus)reader.GetInt32(reader.GetOrdinal("status")),
           GetStringArray(reader,
                          "dependent_tasks")
             .ToList(),
           GetUtcDateTime(reader,
                          reader.GetOrdinal("creation_date")),
           GetNullableDateTime(reader,
                               "completion_date"),
           reader.GetInt64(reader.GetOrdinal("size")),
           GetByteArray(reader,
                        "opaque_id"),
           reader.GetBoolean(reader.GetOrdinal("manual_deletion")));

  /// <summary>
  ///   Map a data reader row to a PartitionData record
  /// </summary>
  /// <param name="reader">The data reader</param>
  /// <returns>The PartitionData record</returns>
  public static PartitionData MapToPartitionData(NpgsqlDataReader reader)
  {
    var podConfigOrdinal = reader.GetOrdinal("pod_configuration");
    PodConfiguration? podConfig = null;
    if (!reader.IsDBNull(podConfigOrdinal))
    {
      var json = reader.GetString(podConfigOrdinal);
      var dict = JsonSerializer.Deserialize<Dictionary<string, string>>(json) ?? new Dictionary<string, string>();
      podConfig = new PodConfiguration(dict);
    }

    return new PartitionData(reader.GetString(reader.GetOrdinal("partition_id")),
                             GetStringArray(reader,
                                            "parent_partition_ids"),
                             reader.GetInt32(reader.GetOrdinal("pod_reserved")),
                             reader.GetInt32(reader.GetOrdinal("pod_max")),
                             reader.GetInt32(reader.GetOrdinal("preemption_pct")),
                             reader.GetInt32(reader.GetOrdinal("priority")),
                             podConfig);
  }

  private static IList<string> GetStringArray(NpgsqlDataReader reader,
                                              string           columnName)
  {
    var ordinal = reader.GetOrdinal(columnName);
    if (reader.IsDBNull(ordinal))
    {
      return Array.Empty<string>();
    }

    return reader.GetFieldValue<string[]>(ordinal);
  }

  private static byte[] GetByteArray(NpgsqlDataReader reader,
                                     string           columnName)
  {
    var ordinal = reader.GetOrdinal(columnName);
    if (reader.IsDBNull(ordinal))
    {
      return Array.Empty<byte>();
    }

    return reader.GetFieldValue<byte[]>(ordinal);
  }

  private static DateTime GetUtcDateTime(NpgsqlDataReader reader,
                                         int              ordinal)
    => DateTime.SpecifyKind(reader.GetDateTime(ordinal),
                            DateTimeKind.Utc);

  private static DateTime? GetNullableDateTime(NpgsqlDataReader reader,
                                               string           columnName)
  {
    var ordinal = reader.GetOrdinal(columnName);
    return reader.IsDBNull(ordinal)
             ? null
             : GetUtcDateTime(reader,
                              ordinal);
  }

  private static TimeSpan? GetNullableTimeSpan(NpgsqlDataReader reader,
                                               string           columnName)
  {
    var ordinal = reader.GetOrdinal(columnName);
    return reader.IsDBNull(ordinal)
             ? null
             : TimeSpan.FromTicks(reader.GetInt64(ordinal));
  }

  private static IDictionary<string, string> DeserializeJsonDict(string json)
  {
    if (string.IsNullOrEmpty(json))
    {
      return new Dictionary<string, string>();
    }

    return JsonSerializer.Deserialize<Dictionary<string, string>>(json) ?? new Dictionary<string, string>();
  }
}
