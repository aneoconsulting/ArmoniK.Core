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

using System.Linq;

using ArmoniK.Core.Adapters.PostgreSQL.Common;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.PostgreSQL.Tests;

/// <summary>
///   Ensures membership tests are translated to SQL that the indexes of the columns can serve
/// </summary>
[TestFixture]
public class ExpressionToSqlTests
{
  // GIN indexes on arrays serve @>, not = ANY
  [Test]
  public void AnArrayColumnContainingAValueUsesContainment()
    => Assert.That(ExpressionToSql<SessionData>.Translate(session => session.CreatedUnder.Contains("role:team"))
                                               .sql,
                   Does.Contain("created_under @> ARRAY[")
                       .And.Not.Contain("ANY"));

  [Test]
  public void AnArrayColumnContainingAValueThroughEnumerableUsesContainment()
    => Assert.That(ExpressionToSql<SessionData>.Translate(session => Enumerable.Contains(session.SharedWith,
                                                                                         "user:alice"))
                                               .sql,
                   Does.Contain("shared_with @> ARRAY[")
                       .And.Not.Contain("ANY"));

  [Test]
  public void AColumnInAListOfValuesUsesAny()
  {
    var ids = new[]
              {
                "s1",
                "s2",
              };
    Assert.That(ExpressionToSql<SessionData>.Translate(session => ids.Contains(session.SessionId))
                                            .sql,
                Does.Contain("session_id = ANY("));
  }
}
