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
public class ProcessReplyResultStateMachineTest
{
  [SetUp]
  public void Setup()
    => sm_ = new ProcessReplyResultStateMachine(NullLogger<ProcessReplyResultStateMachine>.Instance);

  private ProcessReplyResultStateMachine? sm_;

  [Test]
  public void DataChunkFirstShouldFail()
    => Assert.Throws<InvalidOperationException>(() => sm_!.AddDataChunk());

  [Test]
  public void CompleteDataFirstShouldFail()
    => Assert.Throws<InvalidOperationException>(() => sm_!.CompleteData());

  [Test]
  public void CompleteRequestFirstShouldFail()
    => Assert.Throws<InvalidOperationException>(() => sm_!.CompleteRequest());

  [Test]
  public void TwoInitRequestsShouldFail()
  {
    sm_!.InitKey();
    Assert.Throws<InvalidOperationException>(() => sm_.InitKey());
  }

  [Test]
  public void DoubleCompleteRequestWithoutPayloadCompleteShouldFail()
  {
    sm_!.InitKey();
    sm_.AddDataChunk();
    sm_.AddDataChunk();
    sm_.AddDataChunk();
    sm_.CompleteData();

    sm_.CompleteRequest();
    Assert.Throws<InvalidOperationException>(() => sm_.CompleteRequest());
  }

  [Test]
  public void CompleteRequestWithoutPayloadCompleteShouldFail()
  {
    sm_!.InitKey();
    sm_.AddDataChunk();
    sm_.AddDataChunk();
    sm_.AddDataChunk();

    Assert.Throws<InvalidOperationException>(() => sm_.CompleteRequest());
  }

  [Test]
  public void CompleteRequestWithPayloadCompleteShouldSucceed()
  {
    sm_!.InitKey();
    sm_.AddDataChunk();
    sm_.AddDataChunk();
    sm_.AddDataChunk();
    sm_.CompleteData();

    sm_.CompleteRequest();

    Assert.AreEqual(ProcessReplyResultStateMachine.State.InitKeyedDataLast,
                    sm_.GetState());
  }

  [Test]
  public void HappyPathSmallShouldSucceed()
  {
    sm_!.InitKey();
    sm_.AddDataChunk();

    sm_.CompleteData();
    sm_.CompleteRequest();

    Assert.AreEqual(ProcessReplyResultStateMachine.State.InitKeyedDataLast,
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
