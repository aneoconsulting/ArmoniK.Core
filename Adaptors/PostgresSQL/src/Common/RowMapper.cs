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
using System.Globalization;
using System.Linq;
using System.Text;
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

  /// <summary>
  ///   Map a WAL column dictionary (from <c>WalHelpers.ReadAllTextColumns</c>) to a TaskData record.
  ///   RemainingDataDependencies is always empty — it lives in a separate table not present in WAL.
  /// </summary>
  public static TaskData MapToTaskDataFromWal(IReadOnlyDictionary<string, string?> cols)
  {
    var options = new TaskOptions(WalJsonDict(cols,
                                              "options_options"),
                                  TimeSpan.FromTicks(WalInt64(cols,
                                                              "options_max_duration")),
                                  WalInt32(cols,
                                           "options_max_retries"),
                                  WalInt32(cols,
                                           "options_priority"),
                                  WalString(cols,
                                            "options_partition_id"),
                                  WalString(cols,
                                            "options_app_name"),
                                  WalString(cols,
                                            "options_app_version"),
                                  WalString(cols,
                                            "options_app_namespace"),
                                  WalString(cols,
                                            "options_app_service"),
                                  WalString(cols,
                                            "options_engine_type"));

    var output = new Output((OutputStatus)WalInt32(cols,
                                                   "output_status"),
                            WalString(cols,
                                      "output_error"));

    return new TaskData(WalString(cols,
                                  "session_id"),
                        WalString(cols,
                                  "task_id"),
                        WalString(cols,
                                  "owner_pod_id"),
                        WalString(cols,
                                  "owner_pod_name"),
                        WalString(cols,
                                  "payload_id"),
                        WalStringArray(cols,
                                       "parent_task_ids"),
                        WalStringArray(cols,
                                       "data_dependencies"),
                        new Dictionary<string, bool>(),
                        WalStringArray(cols,
                                       "expected_output_ids"),
                        WalString(cols,
                                  "initial_task_id"),
                        WalString(cols,
                                  "created_by"),
                        WalStringArray(cols,
                                       "retry_of_ids"),
                        (TaskStatus)WalInt32(cols,
                                             "status"),
                        WalString(cols,
                                  "status_message"),
                        options,
                        WalDateTime(cols,
                                    "creation_date"),
                        WalNullableDateTime(cols,
                                            "submitted_date"),
                        WalNullableDateTime(cols,
                                            "start_date"),
                        WalNullableDateTime(cols,
                                            "end_date"),
                        WalNullableDateTime(cols,
                                            "reception_date"),
                        WalNullableDateTime(cols,
                                            "acquisition_date"),
                        WalNullableDateTime(cols,
                                            "processed_date"),
                        WalNullableDateTime(cols,
                                            "fetched_date"),
                        WalNullableDateTime(cols,
                                            "pod_ttl"),
                        WalNullableTimeSpan(cols,
                                            "processing_to_end_duration"),
                        WalNullableTimeSpan(cols,
                                            "creation_to_end_duration"),
                        WalNullableTimeSpan(cols,
                                            "received_to_end_duration"),
                        output);
  }

  /// <summary>
  ///   Map a WAL column dictionary (from <c>WalHelpers.ReadAllTextColumns</c>) to a Result record.
  /// </summary>
  public static Result MapToResultFromWal(IReadOnlyDictionary<string, string?> cols)
    => new(WalString(cols,
                     "session_id"),
           WalString(cols,
                     "result_id"),
           WalString(cols,
                     "name"),
           WalString(cols,
                     "created_by"),
           WalString(cols,
                     "completed_by"),
           WalString(cols,
                     "owner_task_id"),
           (ResultStatus)WalInt32(cols,
                                  "status"),
           WalStringArray(cols,
                          "dependent_tasks").ToList(),
           WalDateTime(cols,
                       "creation_date"),
           WalNullableDateTime(cols,
                               "completion_date"),
           WalInt64(cols,
                    "size"),
           WalByteArray(cols,
                        "opaque_id"),
           WalBool(cols,
                   "manual_deletion"));

  private static string WalString(IReadOnlyDictionary<string, string?> cols,
                                  string                                name)
    => cols.GetValueOrDefault(name) ?? "";

  private static int WalInt32(IReadOnlyDictionary<string, string?> cols,
                               string                               name)
    => int.Parse(cols.GetValueOrDefault(name) ?? "0",
                 CultureInfo.InvariantCulture);

  private static long WalInt64(IReadOnlyDictionary<string, string?> cols,
                                string                               name)
    => long.Parse(cols.GetValueOrDefault(name) ?? "0",
                  CultureInfo.InvariantCulture);

  private static bool WalBool(IReadOnlyDictionary<string, string?> cols,
                               string                               name)
    => (cols.GetValueOrDefault(name) ?? "") == "t";

  private static DateTime WalDateTime(IReadOnlyDictionary<string, string?> cols,
                                       string                               name)
    => DateTime.SpecifyKind(DateTime.Parse(cols[name]!,
                                           CultureInfo.InvariantCulture),
                            DateTimeKind.Utc);

  private static DateTime? WalNullableDateTime(IReadOnlyDictionary<string, string?> cols,
                                                string                               name)
  {
    var s = cols.GetValueOrDefault(name);
    return s is null
             ? null
             : DateTime.SpecifyKind(DateTime.Parse(s,
                                                    CultureInfo.InvariantCulture),
                                    DateTimeKind.Utc);
  }

  private static TimeSpan? WalNullableTimeSpan(IReadOnlyDictionary<string, string?> cols,
                                                string                               name)
  {
    var s = cols.GetValueOrDefault(name);
    return s is null
             ? null
             : TimeSpan.FromTicks(long.Parse(s,
                                             CultureInfo.InvariantCulture));
  }

  private static IDictionary<string, string> WalJsonDict(IReadOnlyDictionary<string, string?> cols,
                                                          string                               name)
    => DeserializeJsonDict(cols.GetValueOrDefault(name) ?? "{}");

  private static string[] WalStringArray(IReadOnlyDictionary<string, string?> cols,
                                          string                               name)
    => ParsePgTextArray(cols.GetValueOrDefault(name));

  private static byte[] WalByteArray(IReadOnlyDictionary<string, string?> cols,
                                      string                               name)
    => ParsePgByteA(cols.GetValueOrDefault(name));

  // PostgreSQL TEXT[] text format: {elem1,"elem,2",...}  Handles quoting and backslash escapes.
  private static string[] ParsePgTextArray(string? s)
  {
    if (s is null || s == "{}")
    {
      return Array.Empty<string>();
    }

    var inner   = s.AsSpan(1, s.Length - 2); // strip { and }
    var results = new List<string>();
    var buf     = new StringBuilder();
    var i       = 0;

    while (i <= inner.Length)
    {
      if (i == inner.Length || inner[i] == ',')
      {
        var element = buf.ToString();
        if (!element.Equals("NULL",
                            StringComparison.OrdinalIgnoreCase))
        {
          results.Add(element);
        }

        buf.Clear();
        i++;
      }
      else if (inner[i] == '"')
      {
        i++;
        while (i < inner.Length && inner[i] != '"')
        {
          if (inner[i] == '\\' && i + 1 < inner.Length)
          {
            buf.Append(inner[++i]);
          }
          else
          {
            buf.Append(inner[i]);
          }

          i++;
        }

        i++; // skip closing quote
      }
      else
      {
        buf.Append(inner[i]);
        i++;
      }
    }

    return results.ToArray();
  }

  // PostgreSQL bytea text format: \x followed by hex pairs, e.g. \x0102ff
  private static byte[] ParsePgByteA(string? s)
  {
    if (s is null || s.Length < 2 || !s.StartsWith("\\x",
                                                    StringComparison.Ordinal))
    {
      return Array.Empty<byte>();
    }

    var hex   = s.AsSpan(2);
    var bytes = new byte[hex.Length / 2];
    for (var i = 0; i < bytes.Length; i++)
    {
      bytes[i] = byte.Parse(hex.Slice(i * 2,
                                      2),
                            NumberStyles.HexNumber,
                            CultureInfo.InvariantCulture);
    }

    return bytes;
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
