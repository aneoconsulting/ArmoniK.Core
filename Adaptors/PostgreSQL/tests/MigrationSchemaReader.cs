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
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

using ArmoniK.Core.Adapters.PostgreSQL.Common;

namespace ArmoniK.Core.Adapters.PostgreSQL.Tests;

/// <summary>
///   Reads and parses the migration scripts so tests can verify column names against them.
/// </summary>
/// <remarks>
///   Every script, in the order DbUp applies them, not just the initial one. Reading only the first was
///   correct while there was only one; once a second migration added columns by <c>ALTER TABLE</c>, it made
///   the mapping tests assert against a schema the database no longer had, and they failed on columns that
///   do exist.
/// </remarks>
internal static class MigrationSchemaReader
{
  private static readonly HashSet<string> TableConstraintKeywords = new(StringComparer.OrdinalIgnoreCase)
                                                                    {
                                                                      "PRIMARY",
                                                                      "FOREIGN",
                                                                      "UNIQUE",
                                                                      "CONSTRAINT",
                                                                      "CHECK",
                                                                    };

  public static string ReadMigrationScript()
  {
    var assembly = typeof(NpgsqlConnectionProvider).Assembly;

    // Ordinal sort over the whole resource name reproduces DbUp's ordering, which is why the scripts are
    // numbered. Concatenated rather than parsed separately so a later ALTER TABLE is applied on top of the
    // CREATE TABLE that preceded it.
    var scripts = assembly.GetManifestResourceNames()
                          .Where(name => name.Contains(".Migrations.Scripts.",
                                                       StringComparison.Ordinal) && name.EndsWith(".sql",
                                                                                                  StringComparison.Ordinal))
                          .OrderBy(name => name,
                                   StringComparer.Ordinal)
                          .Select(name =>
                                  {
                                    using var stream = assembly.GetManifestResourceStream(name)!;
                                    using var reader = new StreamReader(stream);
                                    return reader.ReadToEnd();
                                  });

    return string.Join('\n',
                       scripts);
  }

  public static Dictionary<string, HashSet<string>> ParseSchema(string sql)
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

    // Columns a later migration adds to a table an earlier one created. Only ADD COLUMN: a test that
    // silently ignored DROP COLUMN would keep passing for a column the database no longer has, so the day
    // one appears this must learn about it rather than be left as is.
    foreach (Match alterMatch in Regex.Matches(sql,
                                               @"ALTER TABLE\s+(?:IF EXISTS\s+)?(\w+)\s+ADD COLUMN\s+(?:IF NOT EXISTS\s+)?(\w+)",
                                               RegexOptions.IgnoreCase))
    {
      var table = alterMatch.Groups[1].Value;

      if (tables.TryGetValue(table,
                             out var columns))
      {
        columns.Add(alterMatch.Groups[2].Value);
      }
    }

    return tables;
  }

  private static void AddColumnName(HashSet<string> columns,
                                    string          columnDefinition)
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
