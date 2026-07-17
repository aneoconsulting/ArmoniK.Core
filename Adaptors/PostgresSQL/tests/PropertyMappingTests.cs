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
  [OneTimeSetUp]
  public void LoadSchema()
    => schemaColumns_ = MigrationSchemaReader.ParseSchema(MigrationSchemaReader.ReadMigrationScript());

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
}
