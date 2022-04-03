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
using System.Threading.Tasks;

using ArmoniK.Core.Common.StateMachines;
using Microsoft.Extensions.Logging.Abstractions;
using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.StateMachines;


[TestFixture]
public class ComputeRequestStateMachineTest
{
  [SetUp]
  public void Setup()
  {
    sm_ = new ComputeRequestStateMachine(NullLogger<ComputeRequestStateMachine>.Instance);
  }

  private ComputeRequestStateMachine sm_;

  [Test]
  public void PayloadFirstShouldFail()
  {
    Assert.ThrowsAsync<InvalidOperationException>(async () => await sm_.AddPayloadChunkAsync());
  }

  [Test]
  public void DataChunkFirstShouldFail()
  {
    Assert.ThrowsAsync<InvalidOperationException>(async () => await sm_.AddDataDependencyChunkAsync());
  }

  [Test]
  public void InitDataFirstShouldFail()
  {
    Assert.ThrowsAsync<InvalidOperationException>(async () => await sm_.InitDataDependencyAsync());
  }

  [Test]
  public async Task TwoInitRequestsShouldFail()
  {
    await sm_.InitRequestAsync();
    Assert.ThrowsAsync<InvalidOperationException>(async() => await sm_.InitRequestAsync());
  }

  [Test]
  public async Task GetQueueWithoutPayloadCompleteShouldFail()
  {
    await sm_.InitRequestAsync();
    await sm_.AddPayloadChunkAsync();
    await sm_.AddPayloadChunkAsync();
    await sm_.AddPayloadChunkAsync();

    Assert.ThrowsAsync<InvalidOperationException>(async () => await sm_.CompleteRequestAsync());
  }

  [Test]
  public async Task GetQueueWithPayloadCompleteShouldSucceed()
  {
    await sm_.InitRequestAsync();
    await sm_.AddPayloadChunkAsync();
    await sm_.AddPayloadChunkAsync();
    await sm_.AddPayloadChunkAsync();
    await sm_.CompletePayloadAsync();

    await sm_.CompleteRequestAsync();

    Assert.AreEqual(ComputeRequestStateMachine.State.DataLast,
                    sm_.GetState());
  }

  [Test]
  public async Task DataDepWithoutChunkShouldFail()
  {
    await sm_.InitRequestAsync();
    await sm_.AddPayloadChunkAsync();
    await sm_.AddPayloadChunkAsync();
    await sm_.AddPayloadChunkAsync();
    await sm_.CompletePayloadAsync();

    await sm_.InitDataDependencyAsync();
    Assert.ThrowsAsync<InvalidOperationException>(async () => await sm_.CompleteDataDependencyAsync());
  }

  [Test]
  public async Task HappyPathShouldSucceed()
  {
    await sm_.InitRequestAsync();
    await sm_.AddPayloadChunkAsync();
    await sm_.AddPayloadChunkAsync();
    await sm_.AddPayloadChunkAsync();
    await sm_.CompletePayloadAsync();

    await sm_.InitDataDependencyAsync();
    await sm_.AddDataDependencyChunkAsync();
    await sm_.CompleteDataDependencyAsync();

    await sm_.InitDataDependencyAsync();
    await sm_.AddDataDependencyChunkAsync();
    await sm_.CompleteDataDependencyAsync();

    await sm_.CompleteRequestAsync();
    Assert.AreEqual(ComputeRequestStateMachine.State.DataLast,
                    sm_.GetState());
  }

  [Test]
  public async Task HappyPathSmallShouldSucceed()
  {
    await sm_.InitRequestAsync();
    await sm_.CompletePayloadAsync();

    await sm_.InitDataDependencyAsync();
    await sm_.AddDataDependencyChunkAsync();
    await sm_.CompleteDataDependencyAsync();

    await sm_.CompleteRequestAsync();
    Assert.AreEqual(ComputeRequestStateMachine.State.DataLast,
                    sm_.GetState());
  }

  [Test]
  public async Task HappyPathNoDataDepShouldSucceed()
  {
    await sm_.InitRequestAsync();
    await sm_.CompletePayloadAsync();
    await sm_.CompleteRequestAsync();
    Assert.AreEqual(ComputeRequestStateMachine.State.DataLast,
                    sm_.GetState());
  }

  [Test]
  public async Task HappyPathNoDataDepNoGetQueueShouldFail()
  {
    await sm_.InitRequestAsync();
    await sm_.CompletePayloadAsync();
    Assert.AreNotEqual(ComputeRequestStateMachine.State.DataLast,
                    sm_.GetState());
  }

  [Test]
  public async Task HappyPathMultipleLargeDataShouldSucceed()
  {
    await sm_.InitRequestAsync();
    await sm_.AddPayloadChunkAsync();
    await sm_.AddPayloadChunkAsync();
    await sm_.AddPayloadChunkAsync();
    await sm_.AddPayloadChunkAsync();
    await sm_.AddPayloadChunkAsync();
    await sm_.AddPayloadChunkAsync();
    await sm_.CompletePayloadAsync();

    await sm_.InitDataDependencyAsync();
    await sm_.AddDataDependencyChunkAsync();
    await sm_.AddDataDependencyChunkAsync();
    await sm_.AddDataDependencyChunkAsync();
    await sm_.AddDataDependencyChunkAsync();
    await sm_.AddDataDependencyChunkAsync();
    await sm_.AddDataDependencyChunkAsync();
    await sm_.CompleteDataDependencyAsync();

    await sm_.InitDataDependencyAsync();
    await sm_.AddDataDependencyChunkAsync();
    await sm_.AddDataDependencyChunkAsync();
    await sm_.AddDataDependencyChunkAsync();
    await sm_.AddDataDependencyChunkAsync();
    await sm_.AddDataDependencyChunkAsync();
    await sm_.AddDataDependencyChunkAsync();
    await sm_.AddDataDependencyChunkAsync();
    await sm_.CompleteDataDependencyAsync();

    await sm_.CompleteRequestAsync();
    Assert.AreEqual(ComputeRequestStateMachine.State.DataLast,
                    sm_.GetState());
  }

  [Test]
  public void GenerateGraphShouldSucceed()
  {
    var str = sm_.GenerateGraph();
    Console.WriteLine(str);
    Assert.IsFalse(string.IsNullOrEmpty(str));
  }
}