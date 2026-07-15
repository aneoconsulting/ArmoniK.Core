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
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

using ArmoniK.Core.Adapters.PostgresSQL.Common;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.PostgresSQL.Tests;

/// <summary>
///   Ensures every C# property path in <see cref="PropertyMapping" /> resolves to a column that
///   actually exists in the migration script that creates the schema.
/// </summary>
[TestFixture]
public class PropertyMappingTests
{
  // Entries whose key is a strict prefix of another key in the same map (e.g. "Options" before
  // "Options.PartitionId") are container nodes used only to build nested paths and are not expected
  // to resolve to a real column on their own, so they are excluded from the case list below.
  private static readonly (Type Type, string Table, IReadOnlyDictionary<string, string> Mappings)[] EntityMappings =
  {
    (typeof(TaskData), "tasks", PropertyMapping.GetMappings<TaskData>()),
    (typeof(SessionData), "sessions", PropertyMapping.GetMappings<SessionData>()),
    (typeof(Result), "results", PropertyMapping.GetMappings<Result>()),
    (typeof(PartitionData), "partitions", PropertyMapping.GetMappings<PartitionData>()),
    (typeof(Application), "tasks", PropertyMapping.GetMappings<Application>()),
  };

  private static Dictionary<string, HashSet<string>> schemaColumns_ = null!;

  [OneTimeSetUp]
  public void LoadSchema()
    => schemaColumns_ = ParseSchema(ReadMigrationScript());

  public static IEnumerable<TestCaseData> PropertyPathCases()
  {
    foreach (var (type, table, mappings) in EntityMappings)
    {
      foreach (var (path, column) in mappings)
      {
        if (mappings.Keys.Any(other => other.StartsWith(path + ".",
                                                         StringComparison.OrdinalIgnoreCase)))
        {
          continue;
        }

        yield return new TestCaseData(table,
                                      path,
                                      column).SetName($"{type.Name}.{path}");
      }
    }
  }

  [TestCaseSource(nameof(PropertyPathCases))]
  public void PropertyPathMapsToExistingColumn(string table,
                                               string path,
                                               string column)
  {
    Assert.That(schemaColumns_,
               Does.ContainKey(table),
               $"Table '{table}' is not defined in the migration script");
    Assert.That(schemaColumns_[table],
               Does.Contain(column),
               $"'{path}' maps to column '{column}', which does not exist in table '{table}'");
  }

  private static string ReadMigrationScript()
  {
    var assembly     = typeof(NpgsqlConnectionProvider).Assembly;
    var resourceName = assembly.GetManifestResourceNames()
                               .Single(name => name.EndsWith("0001_InitialSchema.sql",
                                                             StringComparison.Ordinal));

    using var stream = assembly.GetManifestResourceStream(resourceName)!;
    using var reader = new StreamReader(stream);
    return reader.ReadToEnd();
  }

  private static Dictionary<string, HashSet<string>> ParseSchema(string sql)
  {
    var tables = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

    foreach (Match tableMatch in Regex.Matches(sql,
                                               @"CREATE TABLE IF NOT EXISTS\s+(\w+)\s*\((.*?)\n\);",
                                               RegexOptions.Singleline))
    {
      var columns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
      var depth   = 0;
      var current = new StringBuilder();

      foreach (var c in tableMatch.Groups[2].Value)
      {
        switch (c)
        {
          case '(':
            depth++;
            break;
          case ')':
            depth--;
            break;
        }

        if (c == ',' && depth == 0)
        {
          AddColumnName(columns,
                        current.ToString());
          current.Clear();
        }
        else
        {
          current.Append(c);
        }
      }

      AddColumnName(columns,
                    current.ToString());
      tables[tableMatch.Groups[1].Value] = columns;
    }

    return tables;
  }

  private static readonly HashSet<string> TableConstraintKeywords = new(StringComparer.OrdinalIgnoreCase)
                                                                    {
                                                                      "PRIMARY",
                                                                      "FOREIGN",
                                                                      "UNIQUE",
                                                                      "CONSTRAINT",
                                                                      "CHECK",
                                                                    };

  private static void AddColumnName(HashSet<string> columns,
                                    string           columnDefinition)
  {
    var trimmed = columnDefinition.Trim();
    if (trimmed.Length == 0)
    {
      return;
    }

    var firstWord = trimmed.Split((char[]?)null,
                                  StringSplitOptions.RemoveEmptyEntries)[0];
    if (TableConstraintKeywords.Contains(firstWord))
    {
      return;
    }

    columns.Add(firstWord);
  }
}
