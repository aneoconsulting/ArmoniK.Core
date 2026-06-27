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

using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Storage;

namespace ArmoniK.Core.Adapters.PostgresSQL.Common;

/// <summary>
///   Maps C# property paths to SQL column names for each entity type
/// </summary>
public static class PropertyMapping
{
  private static readonly Dictionary<string, string> TaskDataMap = new(StringComparer.OrdinalIgnoreCase)
                                                                   {
                                                                     { "SessionId", "session_id" },
                                                                     { "TaskId", "task_id" },
                                                                     { "OwnerPodId", "owner_pod_id" },
                                                                     { "OwnerPodName", "owner_pod_name" },
                                                                     { "PayloadId", "payload_id" },
                                                                     { "ParentTaskIds", "parent_task_ids" },
                                                                     { "DataDependencies", "data_dependencies" },
                                                                     { "ExpectedOutputIds", "expected_output_ids" },
                                                                     { "InitialTaskId", "initial_task_id" },
                                                                     { "CreatedBy", "created_by" },
                                                                     { "RetryOfIds", "retry_of_ids" },
                                                                     { "Status", "status" },
                                                                     { "StatusMessage", "status_message" },
                                                                     { "Options", "options" },
                                                                     { "Options.Options", "options_options" },
                                                                     { "Options.MaxDuration", "options_max_duration" },
                                                                     { "Options.MaxRetries", "options_max_retries" },
                                                                     { "Options.Priority", "options_priority" },
                                                                     { "Options.PartitionId", "options_partition_id" },
                                                                     { "Options.ApplicationName", "options_app_name" },
                                                                     { "Options.ApplicationVersion", "options_app_version" },
                                                                     { "Options.ApplicationNamespace", "options_app_namespace" },
                                                                     { "Options.ApplicationService", "options_app_service" },
                                                                     { "Options.EngineType", "options_engine_type" },
                                                                     { "CreationDate", "creation_date" },
                                                                     { "SubmittedDate", "submitted_date" },
                                                                     { "StartDate", "start_date" },
                                                                     { "EndDate", "end_date" },
                                                                     { "ReceptionDate", "reception_date" },
                                                                     { "AcquisitionDate", "acquisition_date" },
                                                                     { "ProcessedDate", "processed_date" },
                                                                     { "FetchedDate", "fetched_date" },
                                                                     { "PodTtl", "pod_ttl" },
                                                                     { "ProcessingToEndDuration", "processing_to_end_duration" },
                                                                     { "CreationToEndDuration", "creation_to_end_duration" },
                                                                     { "ReceivedToEndDuration", "received_to_end_duration" },
                                                                     { "Output", "output" },
                                                                     { "Output.Status", "output_status" },
                                                                     { "Output.Error", "output_error" },
                                                                   };

  private static readonly Dictionary<string, string> SessionDataMap = new(StringComparer.OrdinalIgnoreCase)
                                                                      {
                                                                        { "SessionId", "session_id" },
                                                                        { "Status", "status" },
                                                                        { "ClientSubmission", "client_submission" },
                                                                        { "WorkerSubmission", "worker_submission" },
                                                                        { "CreationDate", "creation_date" },
                                                                        { "CancellationDate", "cancellation_date" },
                                                                        { "ClosureDate", "closure_date" },
                                                                        { "PurgeDate", "purge_date" },
                                                                        { "DeletionDate", "deletion_date" },
                                                                        { "DeletionTtl", "deletion_ttl" },
                                                                        { "Duration", "duration" },
                                                                        { "PartitionIds", "partition_ids" },
                                                                        { "Options", "options" },
                                                                        { "Options.Options", "options_options" },
                                                                        { "Options.MaxDuration", "options_max_duration" },
                                                                        { "Options.MaxRetries", "options_max_retries" },
                                                                        { "Options.Priority", "options_priority" },
                                                                        { "Options.PartitionId", "options_partition_id" },
                                                                        { "Options.ApplicationName", "options_app_name" },
                                                                        { "Options.ApplicationVersion", "options_app_version" },
                                                                        { "Options.ApplicationNamespace", "options_app_namespace" },
                                                                        { "Options.ApplicationService", "options_app_service" },
                                                                        { "Options.EngineType", "options_engine_type" },
                                                                      };

  private static readonly Dictionary<string, string> ResultMap = new(StringComparer.OrdinalIgnoreCase)
                                                                 {
                                                                   { "SessionId", "session_id" },
                                                                   { "ResultId", "result_id" },
                                                                   { "Name", "name" },
                                                                   { "CreatedBy", "created_by" },
                                                                   { "CompletedBy", "completed_by" },
                                                                   { "OwnerTaskId", "owner_task_id" },
                                                                   { "Status", "status" },
                                                                   { "DependentTasks", "dependent_tasks" },
                                                                   { "CreationDate", "creation_date" },
                                                                   { "CompletionDate", "completion_date" },
                                                                   { "Size", "size" },
                                                                   { "OpaqueId", "opaque_id" },
                                                                   { "ManualDeletion", "manual_deletion" },
                                                                 };

  private static readonly Dictionary<string, string> PartitionDataMap = new(StringComparer.OrdinalIgnoreCase)
                                                                        {
                                                                          { "PartitionId", "partition_id" },
                                                                          { "ParentPartitionIds", "parent_partition_ids" },
                                                                          { "PodReserved", "pod_reserved" },
                                                                          { "PodMax", "pod_max" },
                                                                          { "PreemptionPercentage", "preemption_pct" },
                                                                          { "Priority", "priority" },
                                                                          { "PodConfiguration", "pod_configuration" },
                                                                        };

  private static readonly Dictionary<string, string> ApplicationMap = new(StringComparer.OrdinalIgnoreCase)
                                                                       {
                                                                         { "Name", "options_app_name" },
                                                                         { "Namespace", "options_app_namespace" },
                                                                         { "Version", "options_app_version" },
                                                                         { "Service", "options_app_service" },
                                                                       };

  private static readonly Dictionary<Type, Dictionary<string, string>> TypeMappings = new()
                                                                                      {
                                                                                        { typeof(TaskData), TaskDataMap },
                                                                                        { typeof(SessionData), SessionDataMap },
                                                                                        { typeof(Result), ResultMap },
                                                                                        { typeof(PartitionData), PartitionDataMap },
                                                                                        { typeof(Application), ApplicationMap },
                                                                                      };

  /// <summary>
  ///   Get the SQL column name for a given C# property path on a given entity type
  /// </summary>
  /// <typeparam name="T">Entity type</typeparam>
  /// <param name="propertyPath">C# property path (e.g., "Options.PartitionId")</param>
  /// <returns>SQL column name</returns>
  public static string GetColumnName<T>(string propertyPath)
  {
    if (TypeMappings.TryGetValue(typeof(T),
                                 out var map) && map.TryGetValue(propertyPath,
                                                                 out var column))
    {
      return column;
    }

    throw new ArgumentException($"No column mapping found for {typeof(T).Name}.{propertyPath}");
  }

  /// <summary>
  ///   Get the SQL column name for a given C# property path on a given entity type
  /// </summary>
  /// <param name="type">Entity type</param>
  /// <param name="propertyPath">C# property path (e.g., "Options.PartitionId")</param>
  /// <returns>SQL column name</returns>
  public static string GetColumnName(Type   type,
                                     string propertyPath)
  {
    if (TypeMappings.TryGetValue(type,
                                 out var map) && map.TryGetValue(propertyPath,
                                                                 out var column))
    {
      return column;
    }

    throw new ArgumentException($"No column mapping found for {type.Name}.{propertyPath}");
  }

  /// <summary>
  ///   Try to get the SQL column name for a given C# property path on a given entity type
  /// </summary>
  /// <param name="type">Entity type</param>
  /// <param name="propertyPath">C# property path</param>
  /// <param name="columnName">Output column name</param>
  /// <returns>True if mapping was found</returns>
  public static bool TryGetColumnName(Type       type,
                                      string     propertyPath,
                                      out string columnName)
  {
    columnName = "";
    return TypeMappings.TryGetValue(type,
                                    out var map) && map.TryGetValue(propertyPath,
                                                                    out columnName!);
  }

  /// <summary>
  ///   Get all column mappings for a given entity type
  /// </summary>
  /// <typeparam name="T">Entity type</typeparam>
  /// <returns>Dictionary of property path to column name</returns>
  public static IReadOnlyDictionary<string, string> GetMappings<T>()
  {
    if (TypeMappings.TryGetValue(typeof(T),
                                 out var map))
    {
      return map;
    }

    throw new ArgumentException($"No mappings found for {typeof(T).Name}");
  }
}
