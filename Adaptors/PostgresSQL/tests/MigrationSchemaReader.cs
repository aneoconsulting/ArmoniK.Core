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

using ArmoniK.Core.Adapters.PostgresSQL.Common;

namespace ArmoniK.Core.Adapters.PostgresSQL.Tests;

/// <summary>
///   Reads and parses the initial migration script so tests can verify column names against it.
/// </summary>
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
    var resourceName = assembly.GetManifestResourceNames()
                               .Single(name => name.EndsWith("0001_InitialSchema.sql",
                                                             StringComparison.Ordinal));

    using var stream = assembly.GetManifestResourceStream(resourceName)!;
    using var reader = new StreamReader(stream);
    return reader.ReadToEnd();
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
