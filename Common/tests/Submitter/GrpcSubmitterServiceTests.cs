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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Tests.Helpers;
using ArmoniK.Core.Control.Submitter.Services;

using Grpc.Core;

using Moq;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Submitter;

[TestFixture]
public class GrpcSubmitterServiceTests
{
  [SetUp]
  public void SetUp()
  {
  }

  [TearDown]
  public virtual void TearDown()
  {
  }

  [Test]
  public async Task TryGetResultStreamConstructionShouldSucceed()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.TryGetResult(It.IsAny<ResultRequest>(),
                                                            It.IsAny<IServerStreamWriter<ResultReply>>(),
                                                            CancellationToken.None))
                 .Returns(() => Task.CompletedTask);


    var service = new GrpcSubmitterService(mockSubmitter.Object);

    mockSubmitter.Verify();

    var helperServerStreamWriter = new TestHelperServerStreamWriter<ResultReply>();

    await service.TryGetResultStream(new ResultRequest
                                     {
                                       Key     = "Key",
                                       Session = "Session",
                                     },
                                     helperServerStreamWriter,
                                     TestServerCallContext.Create())
                 .ConfigureAwait(false);

    Assert.AreEqual(0,
                    helperServerStreamWriter.Messages.Count);
  }

  [Test]
  public async Task TryGetResultStreamArmoniKExceptionShouldThrowRpcException()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.TryGetResult(It.IsAny<ResultRequest>(),
                                                            It.IsAny<IServerStreamWriter<ResultReply>>(),
                                                            CancellationToken.None))
                 .Returns(() => throw new ArmoniKException());


    var service = new GrpcSubmitterService(mockSubmitter.Object);

    mockSubmitter.Verify();

    var helperServerStreamWriter = new TestHelperServerStreamWriter<ResultReply>();

    try
    {
      await service.TryGetResultStream(new ResultRequest
                                       {
                                         Key     = "Key",
                                         Session = "Session",
                                       },
                                       helperServerStreamWriter,
                                       TestServerCallContext.Create())
                   .ConfigureAwait(false);
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.Internal,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task TryGetResultStreamTaskNotFoundExceptionShouldThrowRpcException()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.TryGetResult(It.IsAny<ResultRequest>(),
                                                            It.IsAny<IServerStreamWriter<ResultReply>>(),
                                                            CancellationToken.None))
                 .Returns(() => throw new TaskNotFoundException());


    var service = new GrpcSubmitterService(mockSubmitter.Object);

    mockSubmitter.Verify();

    var helperServerStreamWriter = new TestHelperServerStreamWriter<ResultReply>();

    try
    {
      await service.TryGetResultStream(new ResultRequest
                                       {
                                         Key     = "Key",
                                         Session = "Session",
                                       },
                                       helperServerStreamWriter,
                                       TestServerCallContext.Create())
                   .ConfigureAwait(false);
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.NotFound,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task TryGetResultStreamResultNotFoundExceptionShouldThrowRpcException()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.TryGetResult(It.IsAny<ResultRequest>(),
                                                            It.IsAny<IServerStreamWriter<ResultReply>>(),
                                                            CancellationToken.None))
                 .Returns(() => throw new ResultNotFoundException());


    var service = new GrpcSubmitterService(mockSubmitter.Object);

    mockSubmitter.Verify();

    var helperServerStreamWriter = new TestHelperServerStreamWriter<ResultReply>();

    try
    {
      await service.TryGetResultStream(new ResultRequest
                                       {
                                         Key     = "Key",
                                         Session = "Session",
                                       },
                                       helperServerStreamWriter,
                                       TestServerCallContext.Create())
                   .ConfigureAwait(false);
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.NotFound,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task TryGetResultStreamExceptionShouldThrowRpcException()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.TryGetResult(It.IsAny<ResultRequest>(),
                                                            It.IsAny<IServerStreamWriter<ResultReply>>(),
                                                            CancellationToken.None))
                 .Returns(() => throw new Exception());


    var service = new GrpcSubmitterService(mockSubmitter.Object);

    mockSubmitter.Verify();

    var helperServerStreamWriter = new TestHelperServerStreamWriter<ResultReply>();

    try
    {
      await service.TryGetResultStream(new ResultRequest
                                       {
                                         Key     = "Key",
                                         Session = "Session",
                                       },
                                       helperServerStreamWriter,
                                       TestServerCallContext.Create())
                   .ConfigureAwait(false);
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.Unknown,
                      e.StatusCode);
    }
  }

  [Test]
  public async Task TryGetResultStreamInvalidOperationExceptionShouldThrowRpcException()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.TryGetResult(It.IsAny<ResultRequest>(),
                                                            It.IsAny<IServerStreamWriter<ResultReply>>(),
                                                            CancellationToken.None))
                 .Returns(() => throw new InvalidOperationException());


    var service = new GrpcSubmitterService(mockSubmitter.Object);

    mockSubmitter.Verify();

    var helperServerStreamWriter = new TestHelperServerStreamWriter<ResultReply>();

    try
    {
      await service.TryGetResultStream(new ResultRequest
                                       {
                                         Key     = "Key",
                                         Session = "Session",
                                       },
                                       helperServerStreamWriter,
                                       TestServerCallContext.Create())
                   .ConfigureAwait(false);
    }
    catch (RpcException e)
    {
      Console.WriteLine(e);
      Assert.AreEqual(StatusCode.Unknown,
                      e.StatusCode);
    }
  }
}
