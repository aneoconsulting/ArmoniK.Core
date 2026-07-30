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

using ArmoniK.Core.Adapters.PostgresSQL.Common;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.PostgresSQL.Tests;

/// <summary>
///   Covers <see cref="RowMapper.For{TSource,TResult}" />, the selector analysis deciding which columns
///   a query selects. It fails silently by construction - a column it misses is not an error, it is a
///   field quietly left at its default - so its behaviour is pinned here rather than left to the
///   incidental coverage of the table tests.
/// </summary>
[TestFixture]
public class RowMapperSelectorTests
{
  private static string[] ColumnsOf(RowMapper mapper)
    => mapper.SelectList.Split(", ",
                               StringSplitOptions.RemoveEmptyEntries);

  [Test]
  public void IdentitySelectorOnTaskDataTakesFullRowAndSkipsTheJoinQuery()
  {
    Expression<Func<TaskData, TaskData>> selector = data => data;

    var mapper = RowMapper.For(selector);

    Assert.That(mapper.SelectList,
                Is.EqualTo("*"));
    // TaskData's join-table field is guaranteed empty by the state machine wherever its only
    // consumer reads it, so the identity selector deliberately does not pay for the join query.
    Assert.That(mapper.NeedsSeparatelyStoredData,
                Is.False);
  }

  [Test]
  public void IdentitySelectorOnResultTakesFullRowAndStillLoadsTheJoinQuery()
  {
    Expression<Func<Result, Result>> selector = result => result;

    var mapper = RowMapper.For(selector);

    Assert.That(mapper.SelectList,
                Is.EqualTo("*"));
    // Unlike TaskData: TaskLifeCycleHelper.ResolveDependencies reads DependentTasks off results
    // fetched with the identity selector and relies on it being populated to resolve readiness.
    Assert.That(mapper.NeedsSeparatelyStoredData,
                Is.True);
  }

  [Test]
  public void LeafMemberSelectsOnlyItsOwnColumn()
  {
    Expression<Func<TaskData, string>> selector = data => data.TaskId;

    var mapper = RowMapper.For(selector);

    Assert.That(ColumnsOf(mapper),
                Is.EquivalentTo(new[]
                                {
                                  "task_id",
                                }));
    Assert.That(mapper.NeedsSeparatelyStoredData,
                Is.False);
  }

  [Test]
  public void AnonymousTypeSelectsEveryProjectedMembersColumn()
  {
    Expression<Func<TaskData, object>> selector = data => new
                                                          {
                                                            data.TaskId,
                                                            data.SessionId,
                                                            data.PayloadId,
                                                          };

    var mapper = RowMapper.For(selector);

    Assert.That(ColumnsOf(mapper),
                Is.EquivalentTo(new[]
                                {
                                  "task_id",
                                  "session_id",
                                  "payload_id",
                                }));
  }

  [Test]
  public void CompoundMemberExpandsToAllOfItsSubColumns()
  {
    Expression<Func<TaskData, TaskOptions>> selector = data => data.Options;

    var mapper = RowMapper.For(selector);

    Assert.That(ColumnsOf(mapper),
                Is.EquivalentTo(new[]
                                {
                                  "options_options",
                                  "options_max_duration",
                                  "options_max_retries",
                                  "options_priority",
                                  "options_partition_id",
                                  "options_app_name",
                                  "options_app_version",
                                  "options_app_namespace",
                                  "options_app_service",
                                  "options_engine_type",
                                }));
  }

  [Test]
  public void CountOnAnArrayBackedCollectionSelectsTheArrayColumn()
  {
    Expression<Func<TaskData, int>> selector = data => data.DataDependencies.Count;

    var mapper = RowMapper.For(selector);

    Assert.That(ColumnsOf(mapper),
                Is.EquivalentTo(new[]
                                {
                                  "data_dependencies",
                                }));
  }

  [Test]
  public void ProjectingRemainingDataDependenciesAsksForTheJoinQueryAndTheIdColumn()
  {
    Expression<Func<TaskData, IDictionary<string, bool>>> selector = data => data.RemainingDataDependencies;

    var mapper = RowMapper.For(selector);

    Assert.That(mapper.NeedsSeparatelyStoredData,
                Is.True);
    // The join query correlates on the id, so it has to be selected even though the selector
    // never reads it.
    Assert.That(ColumnsOf(mapper),
                Does.Contain("task_id"));
  }

  [Test]
  public void ProjectingDependentTasksAsksForTheJoinQueryAndTheIdColumn()
  {
    Expression<Func<Result, List<string>>> selector = result => result.DependentTasks;

    var mapper = RowMapper.For(selector);

    Assert.That(mapper.NeedsSeparatelyStoredData,
                Is.True);
    Assert.That(ColumnsOf(mapper),
                Does.Contain("result_id"));
  }

  [Test]
  public void UnrecognisedShapeFallsBackToTheFullRow()
  {
    // A method call is not a shape the analysis understands, so it must not guess at the columns.
    Expression<Func<TaskData, string>> selector = data => data.TaskId.Substring(0,
                                                                                2);

    var mapper = RowMapper.For(selector);

    Assert.That(mapper.SelectList,
                Is.EqualTo("*"));
  }

  [Test]
  public void ProjectedJoinFieldSurvivesAnUnrecognisedSibling()
  {
    // Falling back to the full row covers the unrecognised half, but the join-table field is not a
    // column: having been explicitly projected, it still has to be loaded by its own query.
    Expression<Func<TaskData, object>> selector = data => new
                                                          {
                                                            Deps = data.RemainingDataDependencies,
                                                            Trimmed = data.TaskId.Substring(0,
                                                                                            2),
                                                          };

    var mapper = RowMapper.For(selector);

    Assert.That(mapper.SelectList,
                Is.EqualTo("*"));
    Assert.That(mapper.NeedsSeparatelyStoredData,
                Is.True);
  }

  [Test]
  public void CapturedVariableAddsNoColumn()
  {
    var captured = "captured";

    Expression<Func<TaskData, object>> selector = data => new
                                                          {
                                                            data.TaskId,
                                                            Extra = captured,
                                                          };

    var mapper = RowMapper.For(selector);

    Assert.That(ColumnsOf(mapper),
                Is.EquivalentTo(new[]
                                {
                                  "task_id",
                                }));
  }

  [Test]
  public void EntityWithoutAJoinTableFieldNeverAsksForTheJoinQuery()
  {
    Expression<Func<SessionData, string>> selector = session => session.SessionId;

    var mapper = RowMapper.For(selector);

    Assert.That(ColumnsOf(mapper),
                Is.EquivalentTo(new[]
                                {
                                  "session_id",
                                }));
    Assert.That(mapper.NeedsSeparatelyStoredData,
                Is.False);
  }

  [Test]
  public void FullRowMapperSelectsEverything()
  {
    Assert.That(RowMapper.FullRow.SelectList,
                Is.EqualTo("*"));
    Assert.That(RowMapper.FullRow.NeedsSeparatelyStoredData,
                Is.False);
  }
}
