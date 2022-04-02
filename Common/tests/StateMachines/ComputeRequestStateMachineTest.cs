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
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.StateMachines;

using Google.Protobuf;

using Microsoft.Extensions.Logging.Abstractions;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.StateMachines;

public static class ComputeRequestStateMachineExt
{
  public static void InitRequest(this ComputeRequestStateMachine sm) => sm.Init(10,
             "sessionId",
             "taskId",
             new Dictionary<string, string>(),
             ByteString.Empty,
             new List<string>());

  public static void AddEmptyPayloadChunk(this ComputeRequestStateMachine sm) => sm.AddPayloadChunk(ByteString.Empty);

  public static void AddEmptyDataChunk(this ComputeRequestStateMachine sm) => sm.AddDataDependencyChunk(ByteString.Empty);

  public static void InitEmptyDataDepKey(this ComputeRequestStateMachine sm) => sm.InitDataDependency(string.Empty);

}

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
    Assert.Throws<InvalidOperationException>(() => sm_.AddEmptyPayloadChunk());
  }

  [Test]
  public void DataChunkFirstShouldFail()
  {
    Assert.Throws<InvalidOperationException>(() => sm_.AddEmptyDataChunk());
  }

  [Test]
  public void InitDataFirstShouldFail()
  {
    Assert.Throws<InvalidOperationException>(() => sm_.InitEmptyDataDepKey());
  }

  [Test]
  public void InitRequestFirstShouldSucceed()
  {
    sm_.InitRequest();
  }

  [Test]
  public void TwoInitRequestsShouldFail()
  {
    sm_.InitRequest();
    Assert.Throws<InvalidOperationException>( () => sm_.InitRequest());
  }

  [Test]
  public void GetQueueWithoutPayloadCompleteShouldFail()
  {
    sm_.InitRequest();
    sm_.AddEmptyPayloadChunk();
    sm_.AddEmptyPayloadChunk();
    sm_.AddEmptyPayloadChunk();

    Assert.Throws<InvalidOperationException>(() => sm_.GetQueue());
  }

  [Test]
  public void GetQueueWithPayloadCompleteShouldSucceed()
  {
    sm_.InitRequest();
    sm_.AddEmptyPayloadChunk();
    sm_.AddEmptyPayloadChunk();
    sm_.AddEmptyPayloadChunk();
    sm_.CompletePayload();

    sm_.GetQueue();
  }

  [Test]
  public void DataDepWithoutChunkShouldFail()
  {
    sm_.InitRequest();
    sm_.AddEmptyPayloadChunk();
    sm_.AddEmptyPayloadChunk();
    sm_.AddEmptyPayloadChunk();
    sm_.CompletePayload();

    sm_.InitEmptyDataDepKey();
    Assert.Throws<InvalidOperationException>(() => sm_.CompleteDataDependency());
  }

  [Test]
  public void HappyPathShouldSucceed()
  {
    sm_.InitRequest();
    sm_.AddEmptyPayloadChunk();
    sm_.AddEmptyPayloadChunk();
    sm_.AddEmptyPayloadChunk();
    sm_.CompletePayload();

    sm_.InitEmptyDataDepKey();
    sm_.AddEmptyDataChunk();
    sm_.CompleteDataDependency();

    sm_.InitEmptyDataDepKey();
    sm_.AddEmptyDataChunk();
    sm_.CompleteDataDependency();

    sm_.GetQueue();
  }

  [Test]
  public void HappyPathSmallShouldSucceed()
  {
    sm_.InitRequest();
    sm_.CompletePayload();

    sm_.InitEmptyDataDepKey();
    sm_.AddEmptyDataChunk();
    sm_.CompleteDataDependency();

    sm_.GetQueue();
  }

  [Test]
  public void HappyPathNoDataDepShouldSucceed()
  {
    sm_.InitRequest();
    sm_.CompletePayload();
    sm_.GetQueue();
  }

  [Test]
  public void HappyPathMultipleLargeDataShouldSucceed()
  {
    sm_.InitRequest();
    sm_.AddEmptyPayloadChunk();
    sm_.AddEmptyPayloadChunk();
    sm_.AddEmptyPayloadChunk();
    sm_.AddEmptyPayloadChunk();
    sm_.AddEmptyPayloadChunk();
    sm_.AddEmptyPayloadChunk();
    sm_.CompletePayload();

    sm_.InitEmptyDataDepKey();
    sm_.AddEmptyDataChunk();
    sm_.AddEmptyDataChunk();
    sm_.AddEmptyDataChunk();
    sm_.AddEmptyDataChunk();
    sm_.AddEmptyDataChunk();
    sm_.CompleteDataDependency();

    sm_.InitEmptyDataDepKey();
    sm_.AddEmptyDataChunk();
    sm_.AddEmptyDataChunk();
    sm_.AddEmptyDataChunk();
    sm_.AddEmptyDataChunk();
    sm_.AddEmptyDataChunk();
    sm_.CompleteDataDependency();

    sm_.GetQueue();
  }

  [Test]
  public void GenerateGraphShouldSucceed()
  {
    Console.WriteLine(sm_.GenerateGraph());
  }
}