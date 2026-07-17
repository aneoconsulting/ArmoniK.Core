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
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.PostgresSQL.Tests;

/// <summary>
///   Ensures every column name read by <c>RowMapper</c> resolves to a column that actually exists
///   in the migration script that creates the schema.
/// </summary>
[TestFixture]
public class RowMapperTests
{
  [OneTimeSetUp]
  public void LoadSchema()
    => schemaColumns_ = MigrationSchemaReader.ParseSchema(MigrationSchemaReader.ReadMigrationScript());

  // Each mapping method reads columns from exactly one table; the source is sliced at each
  // method's start (and at the point where the private per-value helpers begin) to attribute
  // every column literal found in between to the right table.
  private static readonly (string Method, string Table)[] Sections =
  {
    ("MapToTaskData", "tasks"),
    ("MapToSessionData", "sessions"),
    ("MapToResult", "results"),
    ("MapToPartitionData", "partitions"),
  };

  private const string HelpersBoundary = "private static IList<string> GetStringArray";

  private static Dictionary<string, HashSet<string>> schemaColumns_ = null!;

  public static IEnumerable<TestCaseData> ColumnCases()
  {
    var source = StripCommentLines(ReadRowMapperSource());

    var starts = Sections.Select(section => (section.Table, Index: RequireIndex(source,
                                                                                $" {section.Method}(NpgsqlDataReader reader)")))
                         .ToList();
    var helpersIndex = RequireIndex(source,
                                    HelpersBoundary);

    for (var i = 0; i < starts.Count; i++)
    {
      var start = starts[i].Index;
      var stop = i + 1 < starts.Count
                   ? starts[i + 1].Index
                   : helpersIndex;
      var body = source[start..stop];

      foreach (Match match in Regex.Matches(body,
                                            "\"([a-zA-Z_][a-zA-Z0-9_]*)\""))
      {
        yield return new TestCaseData(starts[i].Table,
                                      match.Groups[1].Value).SetName($"{starts[i].Table}.{match.Groups[1].Value}");
      }
    }
  }

  [TestCaseSource(nameof(ColumnCases))]
  public void ColumnMapsToExistingSchemaColumn(string table,
                                               string column)
  {
    Assert.That(schemaColumns_,
                Does.ContainKey(table),
                $"Table '{table}' is not defined in the migration script");
    Assert.That(schemaColumns_[table],
                Does.Contain(column),
                $"RowMapper reads column '{column}' from table '{table}', which does not exist in the migration script");
  }

  private static string StripCommentLines(string source)
    => string.Join('\n',
                   source.Split('\n')
                         .Where(line => !line.TrimStart()
                                             .StartsWith("///",
                                                         StringComparison.Ordinal)));

  private static int RequireIndex(string source,
                                  string marker)
  {
    var index = source.IndexOf(marker,
                               StringComparison.Ordinal);
    if (index < 0)
    {
      throw new InvalidOperationException($"Could not find '{marker}' in RowMapper.cs; this test needs updating to match the file's current structure.");
    }

    return index;
  }

  private static string ReadRowMapperSource([CallerFilePath] string testFilePath = "")
  {
    var testDir = Path.GetDirectoryName(testFilePath)!;
    var path = Path.Combine(testDir,
                            "..",
                            "src",
                            "Common",
                            "RowMapper.cs");
    return File.ReadAllText(path);
  }
}
