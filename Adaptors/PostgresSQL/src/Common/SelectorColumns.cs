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
using System.Linq.Expressions;

using ArmoniK.Core.Common.Storage;

namespace ArmoniK.Core.Adapters.PostgresSQL.Common;

/// <summary>
///   Result of analyzing a projection selector: the minimal set of columns needed to satisfy it, and
///   whether it needs data that is not a plain column (currently TaskData.RemainingDataDependencies or
///   Result.DependentTasks, both of which live in a separate join table and must be loaded via a
///   dedicated query).
/// </summary>
/// <param name="Columns">
///   The set of columns needed, or null if the full row is required (identity selector, a field with
///   no column mapping, or an expression shape this visitor does not recognize)
/// </param>
/// <param name="NeedsSeparatelyStoredData">
///   True if the selector needs TaskData.RemainingDataDependencies or Result.DependentTasks loaded via
///   their separate join table
/// </param>
public sealed record SelectorRequirements(IReadOnlySet<string>? Columns,
                                          bool                  NeedsSeparatelyStoredData);

/// <summary>
///   Determines the minimal set of SQL columns needed to satisfy a projection selector, so queries
///   can select only what the caller asked for instead of always fetching every column.
/// </summary>
public static class SelectorColumns
{
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

  /// <summary>
  ///   Try to determine the columns (and any non-column data) referenced by a projection selector.
  /// </summary>
  /// <typeparam name="TSource">Entity type being projected from</typeparam>
  /// <typeparam name="TResult">Type the selector projects to</typeparam>
  /// <param name="selector">The projection selector</param>
  /// <returns>The selector's requirements; see <see cref="SelectorRequirements" /></returns>
  public static SelectorRequirements TryGetColumns<TSource, TResult>(Expression<Func<TSource, TResult>> selector)
  {
    var parameter         = selector.Parameters[0];
    var columns           = new HashSet<string>();
    var needsSeparateData = false;
    var hasSeparateField = SeparatelyStoredFields.TryGetValue(typeof(TSource),
                                                              out var separateField);

    if (!TryCollect(selector.Body,
                    parameter,
                    columns))
    {
      // Full row needed (identity selector, or an expression shape this visitor does not
      // recognize). Whether the separately-stored field must still be loaded in this case
      // depends on the entity type - see SeparatelyStoredFields.
      return new SelectorRequirements(null,
                                      hasSeparateField && separateField.LoadOnFallback);
    }

    // The join query correlates by id, so it must always be selected when needed.
    if (needsSeparateData && hasSeparateField)
    {
      columns.Add(PropertyMapping.GetColumnName(typeof(TSource),
                                                separateField.IdFieldPath));
    }

    return new SelectorRequirements(columns.Count == 0
                                      ? null
                                      : columns,
                                    needsSeparateData);

    bool TryCollect(Expression          expression,
                    ParameterExpression param,
                    HashSet<string>     collected)
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
          return TryCollect(unary.Operand,
                            param,
                            collected);

        case MemberExpression member when hasSeparateField                                                             && IsRootedAtParameter(member,
                                                                                                                                              param) && GetMemberPath(member) ==
                                          separateField.FieldName:
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
          return newExpression.Arguments.All(argument => TryCollect(argument,
                                                                    param,
                                                                    collected));

        case MemberInitExpression memberInit:
          if (!TryCollect(memberInit.NewExpression,
                          param,
                          collected))
          {
            return false;
          }

          foreach (var binding in memberInit.Bindings)
          {
            if (binding is not MemberAssignment assignment || !TryCollect(assignment.Expression,
                                                                          param,
                                                                          collected))
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

  /// <summary>
  ///   A field backed by a separate join table rather than a column of the entity's own table.
  /// </summary>
  /// <param name="FieldName">Name of the field on the entity</param>
  /// <param name="IdFieldPath">Id field needed to correlate the entity with the join table</param>
  /// <param name="LoadOnFallback">
  ///   Whether the field must still be loaded when the full row is needed (identity selector, or a
  ///   shape this visitor does not recognize)
  /// </param>
  private readonly record struct SeparatelyStoredField(string FieldName,
                                                       string IdFieldPath,
                                                       bool   LoadOnFallback);
}
