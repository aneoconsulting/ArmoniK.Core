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

namespace ArmoniK.Core.Adapters.Tests.TableStorage;

[TestFixture(TestOf = typeof(ResultTable))]
internal class AdapterMemoryResultTableTests
{
  private readonly IResultTable resultTable_ = new ResultTable();

  [OneTimeSetUp]
  public void SetUp()
  {
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
    var checkTable = resultTable_.AreResultsAvailableAsync("SessionId",
                                                        new []{"ResultIsAvailable"},
                                                        CancellationToken.None);
    Assert.True(await checkTable);
  }

  [Test]
  public async Task TestResultsAreNotAvailableAsync()
  {
    var checkTable = resultTable_.AreResultsAvailableAsync("SessionId",
                                                        new[] { "ResultIsNotAvailable" },
                                                        CancellationToken.None);
    Assert.False(await checkTable);
  }
}
