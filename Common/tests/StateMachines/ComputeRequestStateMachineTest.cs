// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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

using ArmoniK.Core.Common.StateMachines;

using Microsoft.Extensions.Logging.Abstractions;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.StateMachines;

[TestFixture]
public class ComputeRequestStateMachineTest
{
  [SetUp]
  public void Setup()
    => sm_ = new ComputeRequestStateMachine(NullLogger<ComputeRequestStateMachine>.Instance);

  private ComputeRequestStateMachine? sm_;

  [Test]
  public void PayloadFirstShouldFail()
    => Assert.Throws<InvalidOperationException>(() => sm_!.AddPayloadChunk());

  [Test]
  public void DataChunkFirstShouldFail()
    => Assert.Throws<InvalidOperationException>(() => sm_!.AddDataDependencyChunk());

  [Test]
  public void InitDataFirstShouldFail()
    => Assert.Throws<InvalidOperationException>(() => sm_!.InitDataDependency());

  [Test]
  public void TwoInitRequestsShouldFail()
  {
    sm_!.InitRequest();
    Assert.Throws<InvalidOperationException>(() => sm_.InitRequest());
  }

  [Test]
  public void GetQueueWithoutPayloadCompleteShouldFail()
  {
    sm_!.InitRequest();
    sm_.AddPayloadChunk();
    sm_.AddPayloadChunk();
    sm_.AddPayloadChunk();

    Assert.Throws<InvalidOperationException>(() => sm_.CompleteRequest());
  }

  [Test]
  public void GetQueueWithPayloadCompleteShouldSucceed()
  {
    sm_!.InitRequest();
    sm_.AddPayloadChunk();
    sm_.AddPayloadChunk();
    sm_.AddPayloadChunk();
    sm_.CompletePayload();

    sm_.CompleteRequest();

    Assert.AreEqual(ComputeRequestStateMachine.State.DataLast,
                    sm_.GetState());
  }

  [Test]
  public void DataDepWithoutChunkShouldFail()
  {
    sm_!.InitRequest();
    sm_.AddPayloadChunk();
    sm_.AddPayloadChunk();
    sm_.AddPayloadChunk();
    sm_.CompletePayload();

    sm_.InitDataDependency();
    Assert.Throws<InvalidOperationException>(() => sm_.CompleteDataDependency());
  }

  [Test]
  public void HappyPathShouldSucceed()
  {
    sm_!.InitRequest();
    sm_.AddPayloadChunk();
    sm_.AddPayloadChunk();
    sm_.AddPayloadChunk();
    sm_.CompletePayload();

    sm_.InitDataDependency();
    sm_.AddDataDependencyChunk();
    sm_.CompleteDataDependency();

    sm_.InitDataDependency();
    sm_.AddDataDependencyChunk();
    sm_.CompleteDataDependency();

    sm_.CompleteRequest();
    Assert.AreEqual(ComputeRequestStateMachine.State.DataLast,
                    sm_.GetState());
  }

  [Test]
  public void HappyPathSmallShouldSucceed()
  {
    sm_!.InitRequest();
    sm_.CompletePayload();

    sm_.InitDataDependency();
    sm_.AddDataDependencyChunk();
    sm_.CompleteDataDependency();

    sm_.CompleteRequest();
    Assert.AreEqual(ComputeRequestStateMachine.State.DataLast,
                    sm_.GetState());
  }

  [Test]
  public void HappyPathNoDataDepShouldSucceed()
  {
    sm_!.InitRequest();
    sm_.CompletePayload();
    sm_.CompleteRequest();
    Assert.AreEqual(ComputeRequestStateMachine.State.DataLast,
                    sm_.GetState());
  }

  [Test]
  public void HappyPathNoDataDepNoGetQueueShouldFail()
  {
    sm_!.InitRequest();
    sm_.CompletePayload();
    Assert.AreNotEqual(ComputeRequestStateMachine.State.DataLast,
                       sm_.GetState());
  }

  [Test]
  public void HappyPathMultipleLargeDataShouldSucceed()
  {
    sm_!.InitRequest();
    sm_.AddPayloadChunk();
    sm_.AddPayloadChunk();
    sm_.AddPayloadChunk();
    sm_.AddPayloadChunk();
    sm_.AddPayloadChunk();
    sm_.AddPayloadChunk();
    sm_.CompletePayload();

    sm_.InitDataDependency();
    sm_.AddDataDependencyChunk();
    sm_.AddDataDependencyChunk();
    sm_.AddDataDependencyChunk();
    sm_.AddDataDependencyChunk();
    sm_.AddDataDependencyChunk();
    sm_.AddDataDependencyChunk();
    sm_.CompleteDataDependency();

    sm_.InitDataDependency();
    sm_.AddDataDependencyChunk();
    sm_.AddDataDependencyChunk();
    sm_.AddDataDependencyChunk();
    sm_.AddDataDependencyChunk();
    sm_.AddDataDependencyChunk();
    sm_.AddDataDependencyChunk();
    sm_.AddDataDependencyChunk();
    sm_.CompleteDataDependency();

    sm_.CompleteRequest();
    Assert.AreEqual(ComputeRequestStateMachine.State.DataLast,
                    sm_.GetState());
  }

  [Test]
  public void GenerateGraphShouldSucceed()
  {
    var str = sm_!.GenerateGraph();
    Console.WriteLine(str);
    Assert.IsFalse(string.IsNullOrEmpty(str));
  }

  [Test]
  public void GenerateMermaidGraphShouldSucceed()
  {
    var str = sm_!.GenerateMermaidGraph();
    Console.WriteLine(str);
    Assert.IsFalse(string.IsNullOrEmpty(str));
  }
}
