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
using System.Threading.Tasks;

using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.TestBase;
using ArmoniK.Utils;

using Microsoft.Extensions.DependencyInjection;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.PostgresSQL.Tests;

[TestFixture]
public class ResultTableTests : ResultTableTestBase
{
  private PostgresDatabaseProvider? tableProvider_;

  public override void TearDown()
  {
    tableProvider_?.Dispose();
    RunTests = false;
  }

  public override void GetResultTableInstance()
  {
    tableProvider_ = new PostgresDatabaseProvider();
    var provider = tableProvider_.GetServiceProvider();

    ResultTable = provider.GetRequiredService<IResultTable>();
    RunTests    = true;
  }

  [Test]
  public async Task AddTaskDependenciesTwiceShouldNotDuplicate()
  {
    if (RunTests)
    {
      const string resultId  = "ResultForDedupTest";
      const string sessionId = "SessionId";

      await ResultTable!.Create(new[]
                                {
                                  new Result(sessionId,
                                             resultId,
                                             "",
                                             "CreatedBy",
                                             "CompletedBy",
                                             "OwnerId",
                                             ResultStatus.Created,
                                             new List<string>(),
                                             DateTime.UtcNow,
                                             null,
                                             0,
                                             Array.Empty<byte>(),
                                             false),
                                })
                        .ConfigureAwait(false);

      await ResultTable.AddTaskDependencies(new Dictionary<string, ICollection<string>>
                                            {
                                              {
                                                resultId, new[]
                                                          {
                                                            "Task1",
                                                            "Task2",
                                                          }
                                              },
                                            })
                       .ConfigureAwait(false);

      // Task2 overlaps with the first call; ON CONFLICT DO NOTHING should dedup it.
      await ResultTable.AddTaskDependencies(new Dictionary<string, ICollection<string>>
                                            {
                                              {
                                                resultId, new[]
                                                          {
                                                            "Task2",
                                                            "Task3",
                                                          }
                                              },
                                            })
                       .ConfigureAwait(false);

      var dependents = await ResultTable.GetDependents(sessionId,
                                                       resultId)
                                        .ToListAsync()
                                        .ConfigureAwait(false);

      Assert.That(dependents,
                  Is.EquivalentTo(new List<string>
                                  {
                                    "Task1",
                                    "Task2",
                                    "Task3",
                                  }));
    }
  }
}
