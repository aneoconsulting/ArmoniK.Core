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
  ///   Map a data reader row to a TaskData record, defaulting any field whose column is not present in
  ///   <paramref name="columns" />
  /// </summary>
  /// <param name="reader">The data reader</param>
  /// <param name="columns">
  ///   Columns present in the reader's result set, or null if the full row (every column) is present
  /// </param>
  /// <returns>The TaskData record</returns>
  public static TaskData MapToTaskData(NpgsqlDataReader      reader,
                                       IReadOnlySet<string>? columns = null)
  {
    bool Has(string column)
      => columns is null || columns.Contains(column);

    string Str(string column)
      => Has(column)
           ? reader.GetString(reader.GetOrdinal(column))
           : "";

    int Int(string column)
      => Has(column)
           ? reader.GetInt32(reader.GetOrdinal(column))
           : 0;

    IDictionary<string, string> JsonDict(string column)
      => Has(column)
           ? DeserializeJsonDict(reader.GetString(reader.GetOrdinal(column)))
           : new Dictionary<string, string>();

    TimeSpan TicksDuration(string column)
      => Has(column)
           ? TimeSpan.FromTicks(reader.GetInt64(reader.GetOrdinal(column)))
           : TimeSpan.Zero;

    IList<string> StrArray(string column)
      => Has(column)
           ? GetStringArray(reader,
                            column)
           : Array.Empty<string>();

    DateTime UtcDate(string column)
      => Has(column)
           ? GetUtcDateTime(reader,
                            reader.GetOrdinal(column))
           : default;

    DateTime? NullableDate(string column)
      => Has(column)
           ? GetNullableDateTime(reader,
                                 column)
           : null;

    TimeSpan? NullableSpan(string column)
      => Has(column)
           ? GetNullableTimeSpan(reader,
                                 column)
           : null;

    var options = new TaskOptions(JsonDict("options_options"),
                                  TicksDuration("options_max_duration"),
                                  Int("options_max_retries"),
                                  Int("options_priority"),
                                  Str("options_partition_id"),
                                  Str("options_app_name"),
                                  Str("options_app_version"),
                                  Str("options_app_namespace"),
                                  Str("options_app_service"),
                                  Str("options_engine_type"));

    var output = new Output((OutputStatus)Int("output_status"),
                            Str("output_error"));

    return new TaskData(Str("session_id"),
                        Str("task_id"),
                        Str("owner_pod_id"),
                        Str("owner_pod_name"),
                        Str("payload_id"),
                        StrArray("parent_task_ids"),
                        StrArray("data_dependencies"),
                        new Dictionary<string, bool>(), // RemainingDataDependencies loaded separately
                        StrArray("expected_output_ids"),
                        Str("initial_task_id"),
                        Str("created_by"),
                        StrArray("retry_of_ids"),
                        (TaskStatus)Int("status"),
                        Str("status_message"),
                        options,
                        UtcDate("creation_date"),
                        NullableDate("submitted_date"),
                        NullableDate("start_date"),
                        NullableDate("end_date"),
                        NullableDate("reception_date"),
                        NullableDate("acquisition_date"),
                        NullableDate("processed_date"),
                        NullableDate("fetched_date"),
                        NullableDate("pod_ttl"),
                        NullableSpan("processing_to_end_duration"),
                        NullableSpan("creation_to_end_duration"),
                        NullableSpan("received_to_end_duration"),
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
  ///   Map a data reader row to a Result record, defaulting any field whose column is not present in
  ///   <paramref name="columns" />
  /// </summary>
  /// <param name="reader">The data reader</param>
  /// <param name="columns">
  ///   Columns present in the reader's result set, or null if the full row (every column) is present
  /// </param>
  /// <returns>The Result record</returns>
  public static Result MapToResult(NpgsqlDataReader      reader,
                                   IReadOnlySet<string>? columns = null)
  {
    bool Has(string column)
      => columns is null || columns.Contains(column);

    string Str(string column)
      => Has(column)
           ? reader.GetString(reader.GetOrdinal(column))
           : "";

    int Int(string column)
      => Has(column)
           ? reader.GetInt32(reader.GetOrdinal(column))
           : 0;

    long Long(string column)
      => Has(column)
           ? reader.GetInt64(reader.GetOrdinal(column))
           : 0;

    bool Bool(string column)
      => Has(column) && reader.GetBoolean(reader.GetOrdinal(column));

    byte[] ByteArray(string column)
      => Has(column)
           ? GetByteArray(reader,
                          column)
           : Array.Empty<byte>();

    DateTime UtcDate(string column)
      => Has(column)
           ? GetUtcDateTime(reader,
                            reader.GetOrdinal(column))
           : default;

    DateTime? NullableDate(string column)
      => Has(column)
           ? GetNullableDateTime(reader,
                                 column)
           : null;

    return new Result(Str("session_id"),
                      Str("result_id"),
                      Str("name"),
                      Str("created_by"),
                      Str("completed_by"),
                      Str("owner_task_id"),
                      (ResultStatus)Int("status"),
                      new List<string>(), // DependentTasks loaded separately
                      UtcDate("creation_date"),
                      NullableDate("completion_date"),
                      Long("size"),
                      ByteArray("opaque_id"),
                      Bool("manual_deletion"));
  }

  /// <summary>
  ///   Map a data reader row to a PartitionData record
  /// </summary>
  /// <param name="reader">The data reader</param>
  /// <returns>The PartitionData record</returns>
  public static PartitionData MapToPartitionData(NpgsqlDataReader reader)
  {
    var               podConfigOrdinal = reader.GetOrdinal("pod_configuration");
    PodConfiguration? podConfig        = null;
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
