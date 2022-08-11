// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
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
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

[TestFixture]
public class PartitionTableTestBase
{
  [SetUp]
  public void SetUp()
  {
    GetPartitionTableInstance();

    if (RunTests)
    {
      PartitionTable!.CreatePartitionsAsync(new[]
                                            {
                                              new PartitionData("PartitionId0",
                                                                new List<string>(),
                                                                1,
                                                                12,
                                                                50,
                                                                2,
                                                                new PodConfiguration(new Dictionary<string, string>())),
                                              new PartitionData("PartitionId1",
                                                                new List<string>(),
                                                                1,
                                                                10,
                                                                50,
                                                                1,
                                                                new PodConfiguration(new Dictionary<string, string>())),
                                            })
                     .Wait();

    }
  }

  [TearDown]
  public virtual void TearDown()
  {
    PartitionTable = null;
    RunTests       = false;
  }

  /* Interface to test */
  protected IPartitionTable? PartitionTable;

  /* Boolean to control that tests are executed in
   * an instance of this class */
  protected bool RunTests;

  /* Function be override so it returns the suitable instance
   * of TaskTable to the corresponding interface implementation */
  public virtual void GetPartitionTableInstance()
  {
  }

  [Test]
  public async Task ReadTaskAsyncShouldSucceed()
  {
    if (RunTests)
    {
      var result = await PartitionTable!.ReadPartitionAsync("PartitionId0",
                                                            CancellationToken.None)
                                        .ConfigureAwait(false);

      Assert.AreEqual("PartitionId0",
                      result.PartitionId);
    }
  }

  [Test]
  public Task ReadTaskAsyncShouldThrowException()
  {
    if (RunTests)
    {
      Assert.ThrowsAsync<PartitionNotFoundException>(() => PartitionTable!.ReadPartitionAsync("PartitionIdNotFound",
                                                                                              CancellationToken.None));
    }

    return Task.CompletedTask;
  }

  [Test]
  public Task DeleteTaskAsyncShouldThrowException()
  {
    if (RunTests)
    {
      Assert.ThrowsAsync<PartitionNotFoundException>(() => PartitionTable!.DeletePartitionAsync("PartitionIdNotFound",
                                                                                                CancellationToken.None));
    }

    return Task.CompletedTask;
  }

  [Test]
  public async Task DeleteAlreadyDeletedTaskAsyncShouldThrowException()
  {
    await PartitionTable!.DeletePartitionAsync("PartitionId0",
                                               CancellationToken.None)
                         .ConfigureAwait(false);

    if (RunTests)
    {
      Assert.ThrowsAsync<PartitionNotFoundException>(() => PartitionTable!.DeletePartitionAsync("PartitionId0",
                                                                                                CancellationToken.None));
    }
  }

}
