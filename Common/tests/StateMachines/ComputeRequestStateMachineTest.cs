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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.StateMachines;

using Google.Protobuf;

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

  private readonly ProcessRequest.Types.ComputeRequest initRequest_ = new()
  {
    InitRequest = new ProcessRequest.Types.ComputeRequest.Types.InitRequest(),
  };

  private readonly ProcessRequest.Types.ComputeRequest payloadDataRequest_ = new()
  {
    Payload = new DataChunk
    {
      Data = ByteString.Empty,
    },
  };

  private readonly ProcessRequest.Types.ComputeRequest payloadDataCompleteRequest_ = new()
  {
    Payload = new DataChunk
    {
      DataComplete = true,
    },
  };

  private readonly ProcessRequest.Types.ComputeRequest payloadDataNotCompleteRequest_ = new()
  {
    Payload = new DataChunk
    {
      DataComplete = false,
    },
  };

  private readonly ProcessRequest.Types.ComputeRequest initDataKeyRequest_ = new()
  {
    InitData = new ProcessRequest.Types.ComputeRequest.Types.InitData
    {
      Key = string.Empty,
    },
  };

  private readonly ProcessRequest.Types.ComputeRequest initDataLastTrueRequest_ = new()
  {
    InitData = new ProcessRequest.Types.ComputeRequest.Types.InitData
    {
      LastData = true,
    },
  };

  private readonly ProcessRequest.Types.ComputeRequest initDataLastFalseRequest_ = new()
  {
    InitData = new ProcessRequest.Types.ComputeRequest.Types.InitData
    {
      LastData = false,
    },
  };

  private readonly ProcessRequest.Types.ComputeRequest dataRequest_ = new()
  {
    Data = new DataChunk
    {
      Data = ByteString.Empty,
    },
  };

  private readonly ProcessRequest.Types.ComputeRequest dataCompleteRequest_ = new()
  {
    Data = new DataChunk
    {
      DataComplete = true,
    },
  };

  private readonly ProcessRequest.Types.ComputeRequest dataNotCompleteRequest_ = new()
  {
    Data = new DataChunk
    {
      DataComplete = false,
    },
  };

  private ComputeRequestStateMachine sm_;

  [Test]
  public void PayloadFirstShouldFail()
  {
    Assert.ThrowsAsync<InvalidOperationException>(async () => await sm_.ReceiveRequest(new ProcessRequest.Types.ComputeRequest
    {
      Payload = null,
    }));
  }

  [Test]
  public async Task InitRequestFirstShouldSucceed()
  {
    await sm_.ReceiveRequest(initRequest_);
  }

  [Test]
  public async Task TwoInitRequestsShouldFail()
  {
    await sm_.ReceiveRequest(initRequest_);
    Assert.ThrowsAsync<InvalidOperationException>(async () => await sm_.ReceiveRequest(initRequest_));
  }

  [Test]
  public async Task HappyPathShouldSucceed()
  {
    await sm_.ReceiveRequest(initRequest_);
    await sm_.ReceiveRequest(payloadDataRequest_);
    await sm_.ReceiveRequest(payloadDataRequest_);
    await sm_.ReceiveRequest(payloadDataRequest_);
    await sm_.ReceiveRequest(payloadDataCompleteRequest_);

    await sm_.ReceiveRequest(initDataKeyRequest_);
    await sm_.ReceiveRequest(dataRequest_);
    await sm_.ReceiveRequest(dataCompleteRequest_);

    await sm_.ReceiveRequest(initDataKeyRequest_);
    await sm_.ReceiveRequest(dataRequest_);
    await sm_.ReceiveRequest(dataCompleteRequest_);
    await sm_.ReceiveRequest(initDataLastTrueRequest_);
  }

  [Test]
  public async Task HappyPathSmallShouldSucceed()
  {
    await sm_.ReceiveRequest(initRequest_);
    await sm_.ReceiveRequest(payloadDataCompleteRequest_);

    await sm_.ReceiveRequest(initDataKeyRequest_);
    await sm_.ReceiveRequest(dataRequest_);
    await sm_.ReceiveRequest(dataCompleteRequest_);

    await sm_.ReceiveRequest(initDataLastTrueRequest_);
  }

  [Test]
  public async Task HappyPathNoDataDepShouldSucceed()
  {
    await sm_.ReceiveRequest(initRequest_);
    await sm_.ReceiveRequest(payloadDataCompleteRequest_);
    await sm_.ReceiveRequest(initDataLastTrueRequest_);
  }


  [Test]
  public async Task InitDataLastFalseRequestShouldFail()
  {
    await sm_.ReceiveRequest(initRequest_);
    await sm_.ReceiveRequest(payloadDataCompleteRequest_);

    await sm_.ReceiveRequest(initDataKeyRequest_);
    await sm_.ReceiveRequest(dataRequest_);
    await sm_.ReceiveRequest(dataCompleteRequest_);

    Assert.ThrowsAsync<InvalidOperationException>(async () => await sm_.ReceiveRequest(initDataLastFalseRequest_));
  }

  [Test]
  public async Task DataNotCompleteRequestShouldFail()
  {
    await sm_.ReceiveRequest(initRequest_);
    await sm_.ReceiveRequest(payloadDataCompleteRequest_);

    await sm_.ReceiveRequest(initDataKeyRequest_);
    Assert.ThrowsAsync<InvalidOperationException>(async () => await sm_.ReceiveRequest(dataNotCompleteRequest_));
  }

  [Test]
  public async Task PayloadDataNotCompleteRequestShouldFail()
  {
    await sm_.ReceiveRequest(initRequest_);
    Assert.ThrowsAsync<InvalidOperationException>(async () => await sm_.ReceiveRequest(payloadDataNotCompleteRequest_));
  }

  [Test]
  public async Task PayloadDataNotCompleteRequestShouldFail2()
  {
    await sm_.ReceiveRequest(initRequest_);
    await sm_.ReceiveRequest(payloadDataRequest_);
    await sm_.ReceiveRequest(payloadDataRequest_);
    await sm_.ReceiveRequest(payloadDataRequest_);
    await sm_.ReceiveRequest(payloadDataRequest_);
    Assert.ThrowsAsync<InvalidOperationException>(async () => await sm_.ReceiveRequest(payloadDataNotCompleteRequest_));
  }

  [Test]
  public async Task HappyPathMultipleLargeDataShouldSucceed()
  {
    await sm_.ReceiveRequest(initRequest_);
    await sm_.ReceiveRequest(payloadDataRequest_);
    await sm_.ReceiveRequest(payloadDataRequest_);
    await sm_.ReceiveRequest(payloadDataRequest_);
    await sm_.ReceiveRequest(payloadDataRequest_);
    await sm_.ReceiveRequest(payloadDataRequest_);
    await sm_.ReceiveRequest(payloadDataRequest_);
    await sm_.ReceiveRequest(payloadDataCompleteRequest_);

    await sm_.ReceiveRequest(initDataKeyRequest_);
    await sm_.ReceiveRequest(dataRequest_);
    await sm_.ReceiveRequest(dataRequest_);
    await sm_.ReceiveRequest(dataRequest_);
    await sm_.ReceiveRequest(dataRequest_);
    await sm_.ReceiveRequest(dataRequest_);
    await sm_.ReceiveRequest(dataRequest_);
    await sm_.ReceiveRequest(dataCompleteRequest_);

    await sm_.ReceiveRequest(initDataKeyRequest_);
    await sm_.ReceiveRequest(dataRequest_);
    await sm_.ReceiveRequest(dataRequest_);
    await sm_.ReceiveRequest(dataRequest_);
    await sm_.ReceiveRequest(dataRequest_);
    await sm_.ReceiveRequest(dataRequest_);
    await sm_.ReceiveRequest(dataRequest_);
    await sm_.ReceiveRequest(dataRequest_);
    await sm_.ReceiveRequest(dataCompleteRequest_);
    await sm_.ReceiveRequest(initDataLastTrueRequest_);
  }
}