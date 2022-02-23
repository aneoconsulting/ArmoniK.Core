// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.Memory;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

[TestFixture(TestOf = typeof(ResultTable))]
internal class AdapterMemoryResultTableTests
{
  // ResultTableBaseTests
  private IResultTable resultTable_;

  [SetUp]
  public void SetUp()
  {
    resultTable_ = new ResultTable();
    // Create a simple ResultTable with only two entries before
    // any test is executed.
    resultTable_.Create(new[]
    {
      new Result("SessionId",
                 "ResultIsAvailable",
                 "OwnerId",
                 "DispatchId",
                 true,
                 DateTime.Today,
                 new[] { (byte) 1 }),
      new Result("SessionId",
                 "ResultIsNotAvailable",
                 "OwnerId",
                 "DispatchId",
                 false,
                 DateTime.Today,
                 new[] { (byte) 1 }),
    });
  }

  [Test]
  public async Task TestResultsAreAvailableAsync()
  {
    var checkTable = await resultTable_.AreResultsAvailableAsync("SessionId",
                                                                 new[] { "ResultIsAvailable" },
                                                                 CancellationToken.None);
    Assert.True(checkTable);
  }

  [Test]
  public async Task TestResultsAreNotAvailableAsync()
  {
    var checkTable = await resultTable_.AreResultsAvailableAsync("SessionId",
                                                                 new[] { "ResultIsNotAvailable" },
                                                                 CancellationToken.None);
    Assert.False(checkTable);
  }

  [Test]
  public async Task TestChangeResultDispatch()
  {
    await resultTable_.ChangeResultDispatch("SessionId",
                                            "DispatchId",
                                            "NewDispatchId",
                                            CancellationToken.None);
    var result = await resultTable_.GetResult("SessionId",
                                              "ResultIsAvailable",
                                              CancellationToken.None);

    Assert.True(result.OriginDispatchId == "NewDispatchId");
  }

  [Test]
  public async Task TestChangeResultOwnership()
  {
    await resultTable_.ChangeResultOwnership("SessionId",
                                             new[] { "ResultIsAvailable" },
                                             "OwnerId",
                                             "NewOwnerId",
                                             CancellationToken.None);
    var result = await resultTable_.GetResult("SessionId",
                                              "ResultIsAvailable",
                                              CancellationToken.None);

    Assert.True(result.OwnerTaskId == "NewOwnerId");
  }

  [Test]
  public async Task TestSetResult()
  {
    await resultTable_.SetResult("SessionId",
                                 "OwnerId",
                                 "ResultIsNotAvailable",
                                 CancellationToken.None);

    var result = resultTable_.GetResult("SessionId",
                                        "ResultIsNotAvailable",
                                        CancellationToken.None);

    Assert.True((await result).IsResultAvailable);
  }
}
