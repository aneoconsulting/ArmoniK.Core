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
using System.Text.Json;

using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Storage;

using Npgsql;

namespace ArmoniK.Core.Adapters.PostgresSQL.Common;

/// <summary>
///   Maps SQL rows to domain records, over the set of columns a query actually selected.
/// </summary>
/// <remarks>
///   Build one with <see cref="For{TSource,TResult}" /> to select only what a projection selector
///   needs - <see cref="SelectList" /> is then the column list to put in the query, and
///   <see cref="NeedsSeparatelyStoredData" /> says whether the query's join-table companion is
///   needed too. Use <see cref="FullRow" /> when every column is selected.
/// </remarks>
public sealed class RowMapper
{
  // For each entity type with a field backed by a separate join table: the field's name, the id
  // field needed to correlate with that join table, and whether the field must still be loaded
  // when the full row is needed (identity selector, or a shape the analysis doesn't recognize).
  //
  // TaskData.RemainingDataDependencies: the only real consumer (TaskLifeCycleHelper's
  // EndTaskAsync/RetryTask path) reads it off a task that has already left Creating/Pending, at
  // which point it is guaranteed empty by the state machine - so it is safe to skip whenever the
  // selector doesn't ask for it explicitly, identity included.
  //
  // Result.DependentTasks: TaskLifeCycleHelper.ResolveDependencies reads it off results obtained
  // via the identity selector and relies on it being genuinely populated to resolve task
  // readiness - so, unlike TaskData, the identity/fallback case must still load it.
  private static readonly IReadOnlyDictionary<Type, SeparatelyStoredField> SeparatelyStoredFields = new Dictionary<Type, SeparatelyStoredField>
                                                                                                    {
                                                                                                      [typeof(TaskData)] =
                                                                                                        new(nameof(TaskData.RemainingDataDependencies),
                                                                                                            nameof(TaskData.TaskId),
                                                                                                            false),
                                                                                                      [typeof(Result)] = new(nameof(Result.DependentTasks),
                                                                                                                             nameof(Result.ResultId),
                                                                                                                             true),
                                                                                                    };

  private readonly IReadOnlySet<string>? columns_;

  private RowMapper(IReadOnlySet<string>? columns,
                    bool                  needsSeparatelyStoredData)
  {
    columns_                  = columns;
    NeedsSeparatelyStoredData = needsSeparatelyStoredData;
  }

  /// <summary>
  ///   A mapper for a query selecting every column.
  /// </summary>
  public static RowMapper FullRow { get; } = new(null,
                                                 false);

  /// <summary>
  ///   The column list to select, either the minimal set this mapper reads or <c>*</c> for the full row.
  /// </summary>
  public string SelectList
    => columns_ is null
         ? "*"
         : string.Join(", ",
                       columns_);

  /// <summary>
  ///   Whether the mapped field that lives in a separate join table
  ///   (<see cref="TaskData.RemainingDataDependencies" /> or <see cref="Result.DependentTasks" />) is
  ///   needed, and so must be loaded by its own query. Always false for <see cref="FullRow" />, whose
  ///   callers load it or not on their own terms.
  /// </summary>
  public bool NeedsSeparatelyStoredData { get; }

  /// <summary>
  ///   Build a mapper reading only the columns a projection selector needs, falling back to the full
  ///   row when they cannot be determined (identity selector, a field with no column mapping, or an
  ///   expression shape this analysis does not recognize).
  /// </summary>
  /// <typeparam name="TSource">Entity type being projected from</typeparam>
  /// <typeparam name="TResult">Type the selector projects to</typeparam>
  /// <param name="selector">The projection selector</param>
  /// <returns>A mapper over the columns that selector needs</returns>
  public static RowMapper For<TSource, TResult>(Expression<Func<TSource, TResult>> selector)
  {
    var columns           = new HashSet<string>();
    var needsSeparateData = false;
    SeparatelyStoredField? separateField = SeparatelyStoredFields.TryGetValue(typeof(TSource),
                                                                              out var field)
                                             ? field
                                             : null;

    if (!TryCollectColumns<TSource>(selector.Body,
                                    selector.Parameters[0],
                                    separateField,
                                    columns,
                                    ref needsSeparateData))
    {
      // Full row needed. Whether the separately-stored field must still be loaded in this case
      // depends on the entity type - see SeparatelyStoredFields.
      return new RowMapper(null,
                           separateField?.LoadOnFallback ?? false);
    }

    // The join query correlates by id, so it must always be selected when needed.
    if (needsSeparateData && separateField is not null)
    {
      columns.Add(PropertyMapping.GetColumnName(typeof(TSource),
                                                separateField.Value.IdFieldPath));
    }

    return new RowMapper(columns.Count == 0
                           ? null
                           : columns,
                         needsSeparateData);
  }

  /// <summary>
  ///   Add the columns <paramref name="expression" /> reads to <paramref name="collected" />.
  /// </summary>
  /// <typeparam name="TSource">Entity type being projected from</typeparam>
  /// <param name="expression">Expression to walk</param>
  /// <param name="param">The selector's parameter, identifying member accesses rooted at the entity</param>
  /// <param name="separateField">The entity's join-table-backed field, if it has one</param>
  /// <param name="collected">Set the columns found are added to</param>
  /// <param name="needsSeparateData">Set to true if <paramref name="separateField" /> is read</param>
  /// <returns>
  ///   False if this expression's shape means the full row is needed, in which case
  ///   <paramref name="collected" /> is incomplete and must be discarded
  /// </returns>
  private static bool TryCollectColumns<TSource>(Expression             expression,
                                                 ParameterExpression    param,
                                                 SeparatelyStoredField? separateField,
                                                 HashSet<string>        collected,
                                                 ref bool               needsSeparateData)
  {
    switch (expression)
    {
      case ParameterExpression p when p == param:
        // Identity selector: the whole object is needed.
        return false;

      case UnaryExpression
           {
             NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked or ExpressionType.TypeAs,
           } unary:
        return TryCollectColumns<TSource>(unary.Operand,
                                          param,
                                          separateField,
                                          collected,
                                          ref needsSeparateData);

      case MemberExpression member when separateField is
                                        {
                                        } separate                                                             && IsRootedAtParameter(member,
                                                                                                                                      param) && GetMemberPath(member) == separate.FieldName:
        needsSeparateData = true;
        return true;

      case MemberExpression
           {
             Member.Name: "Count",
             Expression : MemberExpression collectionMember,
           } when IsRootedAtParameter(collectionMember,
                                      param) && PropertyMapping.TryGetColumnName(typeof(TSource),
                                                                                 GetMemberPath(collectionMember),
                                                                                 out var collectionColumn):
        // e.g. data.DataDependencies.Count: the underlying array column is enough to
        // reconstruct the collection (and thus its Count) client-side.
        collected.Add(collectionColumn);
        return true;

      case MemberExpression member when IsRootedAtParameter(member,
                                                            param):
        var path = GetMemberPath(member);
        if (PropertyMapping.TryGetColumnName(typeof(TSource),
                                             path,
                                             out var column))
        {
          collected.Add(column);
          return true;
        }

        // Not a leaf field: it may be a compound member (e.g. "Options" or "Output")
        // whose sub-fields are individually mapped as "Options.X". Expand to every
        // sub-column in that case.
        var prefix   = $"{path}.";
        var expanded = false;
        foreach (var (memberPath, sqlColumn) in PropertyMapping.GetMappings<TSource>())
        {
          if (!memberPath.StartsWith(prefix,
                                     StringComparison.OrdinalIgnoreCase))
          {
            continue;
          }

          collected.Add(sqlColumn);
          expanded = true;
        }

        // Neither a leaf field nor a compound member with known sub-fields - fall back to
        // the full row.
        return expanded;

      case MemberExpression:
        // Member access rooted at a captured variable/closure, not the parameter: no column needed.
        return true;

      case NewExpression newExpression:
        // A loop rather than Arguments.All(...): a ref parameter cannot be used inside a lambda.
        foreach (var argument in newExpression.Arguments)
        {
          if (!TryCollectColumns<TSource>(argument,
                                          param,
                                          separateField,
                                          collected,
                                          ref needsSeparateData))
          {
            return false;
          }
        }

        return true;

      case MemberInitExpression memberInit:
        if (!TryCollectColumns<TSource>(memberInit.NewExpression,
                                        param,
                                        separateField,
                                        collected,
                                        ref needsSeparateData))
        {
          return false;
        }

        foreach (var binding in memberInit.Bindings)
        {
          if (binding is not MemberAssignment assignment || !TryCollectColumns<TSource>(assignment.Expression,
                                                                                        param,
                                                                                        separateField,
                                                                                        collected,
                                                                                        ref needsSeparateData))
          {
            return false;
          }
        }

        return true;

      case ConstantExpression:
        return true;

      default:
        // Unrecognized shape: fall back to the full row rather than risk missing a column.
        return false;
    }
  }

  /// <summary>
  ///   Map a data reader row to a TaskData record, defaulting any field this mapper does not select
  /// </summary>
  /// <param name="reader">The data reader</param>
  /// <returns>The TaskData record</returns>
  public TaskData MapToTaskData(NpgsqlDataReader reader)
  {
    var options = new TaskOptions(JsonDict(reader,
                                           "options_options"),
                                  TicksDuration(reader,
                                                "options_max_duration"),
                                  Int(reader,
                                      "options_max_retries"),
                                  Int(reader,
                                      "options_priority"),
                                  Str(reader,
                                      "options_partition_id"),
                                  Str(reader,
                                      "options_app_name"),
                                  Str(reader,
                                      "options_app_version"),
                                  Str(reader,
                                      "options_app_namespace"),
                                  Str(reader,
                                      "options_app_service"),
                                  Str(reader,
                                      "options_engine_type"));

    var output = new Output((OutputStatus)Int(reader,
                                              "output_status"),
                            Str(reader,
                                "output_error"));

    return new TaskData(Str(reader,
                            "session_id"),
                        Str(reader,
                            "task_id"),
                        Str(reader,
                            "owner_pod_id"),
                        Str(reader,
                            "owner_pod_name"),
                        Str(reader,
                            "payload_id"),
                        StrArray(reader,
                                 "parent_task_ids"),
                        StrArray(reader,
                                 "data_dependencies"),
                        new Dictionary<string, bool>(), // RemainingDataDependencies loaded separately
                        StrArray(reader,
                                 "expected_output_ids"),
                        Str(reader,
                            "initial_task_id"),
                        Str(reader,
                            "created_by"),
                        StrArray(reader,
                                 "retry_of_ids"),
                        (TaskStatus)Int(reader,
                                        "status"),
                        Str(reader,
                            "status_message"),
                        options,
                        UtcDate(reader,
                                "creation_date"),
                        NullableDate(reader,
                                     "submitted_date"),
                        NullableDate(reader,
                                     "start_date"),
                        NullableDate(reader,
                                     "end_date"),
                        NullableDate(reader,
                                     "reception_date"),
                        NullableDate(reader,
                                     "acquisition_date"),
                        NullableDate(reader,
                                     "processed_date"),
                        NullableDate(reader,
                                     "fetched_date"),
                        NullableDate(reader,
                                     "pod_ttl"),
                        NullableSpan(reader,
                                     "processing_to_end_duration"),
                        NullableSpan(reader,
                                     "creation_to_end_duration"),
                        NullableSpan(reader,
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
  ///   Map a data reader row to a Result record, defaulting any field this mapper does not select
  /// </summary>
  /// <param name="reader">The data reader</param>
  /// <returns>The Result record</returns>
  public Result MapToResult(NpgsqlDataReader reader)
    => new(Str(reader,
               "session_id"),
           Str(reader,
               "result_id"),
           Str(reader,
               "name"),
           Str(reader,
               "created_by"),
           Str(reader,
               "completed_by"),
           Str(reader,
               "owner_task_id"),
           (ResultStatus)Int(reader,
                             "status"),
           new List<string>(), // DependentTasks loaded separately
           UtcDate(reader,
                   "creation_date"),
           NullableDate(reader,
                        "completion_date"),
           Long(reader,
                "size"),
           ByteArray(reader,
                     "opaque_id"),
           Bool(reader,
                "manual_deletion"));

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

  private static bool IsRootedAtParameter(Expression          expression,
                                          ParameterExpression param)
  {
    var current = expression;
    while (current is MemberExpression member)
    {
      current = member.Expression;
    }

    return current == param;
  }

  private static string GetMemberPath(Expression expression)
  {
    var parts   = new List<string>();
    var current = expression;
    while (current is MemberExpression member)
    {
      parts.Add(member.Member.Name);
      current = member.Expression;
    }

    parts.Reverse();
    return string.Join(".",
                       parts);
  }

  private bool Has(string column)
    => columns_ is null || columns_.Contains(column);

  private string Str(NpgsqlDataReader reader,
                     string           column)
    => Has(column)
         ? reader.GetString(reader.GetOrdinal(column))
         : "";

  private int Int(NpgsqlDataReader reader,
                  string           column)
    => Has(column)
         ? reader.GetInt32(reader.GetOrdinal(column))
         : 0;

  private long Long(NpgsqlDataReader reader,
                    string           column)
    => Has(column)
         ? reader.GetInt64(reader.GetOrdinal(column))
         : 0;

  private bool Bool(NpgsqlDataReader reader,
                    string           column)
    => Has(column) && reader.GetBoolean(reader.GetOrdinal(column));

  private byte[] ByteArray(NpgsqlDataReader reader,
                           string           column)
    => Has(column)
         ? GetByteArray(reader,
                        column)
         : Array.Empty<byte>();

  private IDictionary<string, string> JsonDict(NpgsqlDataReader reader,
                                               string           column)
    => Has(column)
         ? DeserializeJsonDict(reader.GetString(reader.GetOrdinal(column)))
         : new Dictionary<string, string>();

  private TimeSpan TicksDuration(NpgsqlDataReader reader,
                                 string           column)
    => Has(column)
         ? TimeSpan.FromTicks(reader.GetInt64(reader.GetOrdinal(column)))
         : TimeSpan.Zero;

  private IList<string> StrArray(NpgsqlDataReader reader,
                                 string           column)
    => Has(column)
         ? GetStringArray(reader,
                          column)
         : Array.Empty<string>();

  private DateTime UtcDate(NpgsqlDataReader reader,
                           string           column)
    => Has(column)
         ? GetUtcDateTime(reader,
                          reader.GetOrdinal(column))
         : default;

  private DateTime? NullableDate(NpgsqlDataReader reader,
                                 string           column)
    => Has(column)
         ? GetNullableDateTime(reader,
                               column)
         : null;

  private TimeSpan? NullableSpan(NpgsqlDataReader reader,
                                 string           column)
    => Has(column)
         ? GetNullableTimeSpan(reader,
                               column)
         : null;

  /// <summary>
  ///   A field backed by a separate join table rather than a column of the entity's own table.
  /// </summary>
  /// <param name="FieldName">Name of the field on the entity</param>
  /// <param name="IdFieldPath">Id field needed to correlate the entity with the join table</param>
  /// <param name="LoadOnFallback">
  ///   Whether the field must still be loaded when the full row is needed (identity selector, or a
  ///   shape the analysis does not recognize)
  /// </param>
  private readonly record struct SeparatelyStoredField(string FieldName,
                                                       string IdFieldPath,
                                                       bool   LoadOnFallback);
}
