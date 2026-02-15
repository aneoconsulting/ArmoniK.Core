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
using System.Text.Json;

using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Storage;

using FluentValidation.Internal;

using Npgsql;

using NpgsqlTypes;

namespace ArmoniK.Core.Adapters.PostgresSQL.Common;

/// <summary>
///   Shared SQL helpers for building parameterized queries
/// </summary>
public static class SqlHelper
{
  /// <summary>
  ///   Build SQL SET clauses from an UpdateDefinition
  /// </summary>
  /// <typeparam name="T">Entity type</typeparam>
  /// <param name="updates">Update definition</param>
  /// <param name="cmd">Command to add parameters to</param>
  /// <param name="prefix">Parameter name prefix</param>
  /// <returns>SQL SET clause string</returns>
  public static string BuildSetClauses<T>(UpdateDefinition<T> updates,
                                          NpgsqlCommand       cmd,
                                          string              prefix = "u")
  {
    var sets     = new List<string>();
    var paramIdx = 0;

    foreach (var setter in updates.Setters)
    {
      var propName = setter.Property.GetMember()
                           .Name;

      // Handle setting the whole Options object (TaskOptions)
      if (propName == "Options" && setter.Value is TaskOptions opts)
      {
        AddOptionsSetClauses(sets,
                             cmd,
                             opts,
                             prefix,
                             ref paramIdx);
        continue;
      }

      // Handle setting the whole Output object
      if (propName == "Output" && setter.Value is Output output)
      {
        var pStatus = $"@{prefix}{paramIdx++}";
        var pError  = $"@{prefix}{paramIdx++}";
        sets.Add($"output_status = {pStatus}");
        sets.Add($"output_error = {pError}");
        cmd.Parameters.AddWithValue(pStatus,
                                    (int)output.Status);
        cmd.Parameters.AddWithValue(pError,
                                    output.Error);
        continue;
      }

      var columnName = PropertyMapping.GetColumnName(typeof(T),
                                                     propName);
      var paramName = $"@{prefix}{paramIdx++}";
      sets.Add($"{columnName} = {paramName}");
      AddParameterWithConversion(cmd,
                                 paramName,
                                 setter.Value);
    }

    return string.Join(", ",
                       sets);
  }

  /// <summary>
  ///   Add a parameter to a command with type conversion
  /// </summary>
  public static void AddParameterWithConversion(NpgsqlCommand cmd,
                                                string        paramName,
                                                object?       value)
  {
    switch (value)
    {
      case null:
        cmd.Parameters.AddWithValue(paramName,
                                    DBNull.Value);
        break;
      case Enum e:
        cmd.Parameters.AddWithValue(paramName,
                                    Convert.ToInt32(e));
        break;
      case TimeSpan ts:
        cmd.Parameters.AddWithValue(paramName,
                                    ts.Ticks);
        break;
      case IList<string> list:
        cmd.Parameters.AddWithValue(paramName,
                                    NpgsqlDbType.Array | NpgsqlDbType.Text,
                                    list.ToArray());
        break;
      case IDictionary<string, string> dict:
        cmd.Parameters.AddWithValue(paramName,
                                    NpgsqlDbType.Jsonb,
                                    JsonSerializer.Serialize(dict));
        break;
      case IDictionary<string, bool> boolDict:
        cmd.Parameters.AddWithValue(paramName,
                                    NpgsqlDbType.Array | NpgsqlDbType.Text,
                                    boolDict.Keys.ToArray());
        break;
      case PodConfiguration podConfig:
        cmd.Parameters.AddWithValue(paramName,
                                    NpgsqlDbType.Jsonb,
                                    JsonSerializer.Serialize(podConfig.Configuration));
        break;
      case byte[] bytes:
        cmd.Parameters.AddWithValue(paramName,
                                    NpgsqlDbType.Bytea,
                                    bytes);
        break;
      default:
        cmd.Parameters.AddWithValue(paramName,
                                    value);
        break;
    }
  }

  /// <summary>
  ///   Add parameters from ExpressionToSql translation to a command
  /// </summary>
  public static void AddExpressionParameters(NpgsqlCommand                cmd,
                                             Dictionary<string, object?> parameters)
  {
    foreach (var (name, value) in parameters)
    {
      switch (value)
      {
        case null:
          cmd.Parameters.AddWithValue(name,
                                      DBNull.Value);
          break;
        case string[] arr:
          cmd.Parameters.AddWithValue(name,
                                      NpgsqlDbType.Array | NpgsqlDbType.Text,
                                      arr);
          break;
        case IList<string> list:
          cmd.Parameters.AddWithValue(name,
                                      NpgsqlDbType.Array | NpgsqlDbType.Text,
                                      list.ToArray());
          break;
        case ICollection<string> col:
          cmd.Parameters.AddWithValue(name,
                                      NpgsqlDbType.Array | NpgsqlDbType.Text,
                                      col.ToArray());
          break;
        default:
          cmd.Parameters.AddWithValue(name,
                                      value);
          break;
      }
    }
  }

  private static void AddOptionsSetClauses(List<string>  sets,
                                           NpgsqlCommand cmd,
                                           TaskOptions   opts,
                                           string        prefix,
                                           ref int       paramIdx)
  {
    var fields = new (string column, object? value)[]
                 {
                   ("options_options", JsonSerializer.Serialize(opts.Options)),
                   ("options_max_duration", opts.MaxDuration.Ticks),
                   ("options_max_retries", opts.MaxRetries),
                   ("options_priority", opts.Priority),
                   ("options_partition_id", opts.PartitionId),
                   ("options_app_name", opts.ApplicationName),
                   ("options_app_version", opts.ApplicationVersion),
                   ("options_app_namespace", opts.ApplicationNamespace),
                   ("options_app_service", opts.ApplicationService),
                   ("options_engine_type", opts.EngineType),
                 };

    foreach (var (column, value) in fields)
    {
      var pName = $"@{prefix}{paramIdx++}";
      sets.Add($"{column} = {pName}");
      if (column == "options_options")
      {
        cmd.Parameters.AddWithValue(pName,
                                    NpgsqlDbType.Jsonb,
                                    value!);
      }
      else
      {
        cmd.Parameters.AddWithValue(pName,
                                    value!);
      }
    }
  }

  /// <summary>
  ///   Add task INSERT parameters to a command
  /// </summary>
  public static void AddTaskInsertParameters(NpgsqlCommand cmd,
                                             TaskData      task,
                                             string        prefix = "")
  {
    cmd.Parameters.AddWithValue($"{prefix}session_id",
                                task.SessionId);
    cmd.Parameters.AddWithValue($"{prefix}task_id",
                                task.TaskId);
    cmd.Parameters.AddWithValue($"{prefix}owner_pod_id",
                                task.OwnerPodId);
    cmd.Parameters.AddWithValue($"{prefix}owner_pod_name",
                                task.OwnerPodName);
    cmd.Parameters.AddWithValue($"{prefix}payload_id",
                                task.PayloadId);
    cmd.Parameters.AddWithValue($"{prefix}parent_task_ids",
                                NpgsqlDbType.Array | NpgsqlDbType.Text,
                                task.ParentTaskIds.ToArray());
    cmd.Parameters.AddWithValue($"{prefix}data_dependencies",
                                NpgsqlDbType.Array | NpgsqlDbType.Text,
                                task.DataDependencies.ToArray());
    cmd.Parameters.AddWithValue($"{prefix}expected_output_ids",
                                NpgsqlDbType.Array | NpgsqlDbType.Text,
                                task.ExpectedOutputIds.ToArray());
    cmd.Parameters.AddWithValue($"{prefix}initial_task_id",
                                task.InitialTaskId);
    cmd.Parameters.AddWithValue($"{prefix}created_by",
                                task.CreatedBy);
    cmd.Parameters.AddWithValue($"{prefix}retry_of_ids",
                                NpgsqlDbType.Array | NpgsqlDbType.Text,
                                task.RetryOfIds.ToArray());
    cmd.Parameters.AddWithValue($"{prefix}status",
                                (int)task.Status);
    cmd.Parameters.AddWithValue($"{prefix}status_message",
                                task.StatusMessage);
    cmd.Parameters.AddWithValue($"{prefix}options_options",
                                NpgsqlDbType.Jsonb,
                                JsonSerializer.Serialize(task.Options.Options));
    cmd.Parameters.AddWithValue($"{prefix}options_max_duration",
                                task.Options.MaxDuration.Ticks);
    cmd.Parameters.AddWithValue($"{prefix}options_max_retries",
                                task.Options.MaxRetries);
    cmd.Parameters.AddWithValue($"{prefix}options_priority",
                                task.Options.Priority);
    cmd.Parameters.AddWithValue($"{prefix}options_partition_id",
                                task.Options.PartitionId);
    cmd.Parameters.AddWithValue($"{prefix}options_app_name",
                                task.Options.ApplicationName);
    cmd.Parameters.AddWithValue($"{prefix}options_app_version",
                                task.Options.ApplicationVersion);
    cmd.Parameters.AddWithValue($"{prefix}options_app_namespace",
                                task.Options.ApplicationNamespace);
    cmd.Parameters.AddWithValue($"{prefix}options_app_service",
                                task.Options.ApplicationService);
    cmd.Parameters.AddWithValue($"{prefix}options_engine_type",
                                task.Options.EngineType);
    cmd.Parameters.AddWithValue($"{prefix}creation_date",
                                task.CreationDate);
    cmd.Parameters.AddWithValue($"{prefix}submitted_date",
                                (object?)task.SubmittedDate ?? DBNull.Value);
    cmd.Parameters.AddWithValue($"{prefix}start_date",
                                (object?)task.StartDate ?? DBNull.Value);
    cmd.Parameters.AddWithValue($"{prefix}end_date",
                                (object?)task.EndDate ?? DBNull.Value);
    cmd.Parameters.AddWithValue($"{prefix}reception_date",
                                (object?)task.ReceptionDate ?? DBNull.Value);
    cmd.Parameters.AddWithValue($"{prefix}acquisition_date",
                                (object?)task.AcquisitionDate ?? DBNull.Value);
    cmd.Parameters.AddWithValue($"{prefix}processed_date",
                                (object?)task.ProcessedDate ?? DBNull.Value);
    cmd.Parameters.AddWithValue($"{prefix}fetched_date",
                                (object?)task.FetchedDate ?? DBNull.Value);
    cmd.Parameters.AddWithValue($"{prefix}pod_ttl",
                                (object?)task.PodTtl ?? DBNull.Value);
    cmd.Parameters.AddWithValue($"{prefix}processing_to_end_duration",
                                (object?)task.ProcessingToEndDuration?.Ticks ?? DBNull.Value);
    cmd.Parameters.AddWithValue($"{prefix}creation_to_end_duration",
                                (object?)task.CreationToEndDuration?.Ticks ?? DBNull.Value);
    cmd.Parameters.AddWithValue($"{prefix}received_to_end_duration",
                                (object?)task.ReceivedToEndDuration?.Ticks ?? DBNull.Value);
    cmd.Parameters.AddWithValue($"{prefix}output_status",
                                (int)task.Output.Status);
    cmd.Parameters.AddWithValue($"{prefix}output_error",
                                task.Output.Error);
  }
}
