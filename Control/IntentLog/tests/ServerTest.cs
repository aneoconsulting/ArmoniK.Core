// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Control.IntentLog.Protocol;
using ArmoniK.Core.Control.IntentLog.Protocol.Client;
using ArmoniK.Core.Control.IntentLog.Protocol.Messages;
using ArmoniK.Core.Control.IntentLog.Protocol.Server;
using ArmoniK.Core.Control.IntentLog.Tests.Utils;
using ArmoniK.Core.Utils;

using Microsoft.Extensions.Logging;

using Moq;

using NUnit.Framework;

using Intent = ArmoniK.Core.Control.IntentLog.Protocol.Server.Intent;

namespace ArmoniK.Core.Control.IntentLog.Tests;

[TestFixture(TestOf = typeof(Server))]
public class ServerTest
{
  [SetUp]
  public void Setup()
  {
    cts_     = new CancellationTokenSource();
    logger_  = new Mock<ILogger<Client>>(MockBehavior.Strict);
    handler_ = new Mock<IServerHandler>(MockBehavior.Strict);

    logger_.Setup(m => m.Log(It.Is<LogLevel>(x => x <= LogLevel.Debug),
                             It.IsAny<EventId>(),
                             It.IsAny<It.IsAnyType>(),
                             It.IsAny<Exception?>(),
                             It.IsAny<Func<It.IsAnyType, Exception?, string>>()));

    var (clientStream, serverStream) = ChannelStream.CreatePair();

    clientStream_ = clientStream;
    connection_ = new Connection(handler_.Object,
                                 serverStream,
                                 () =>
                                 {
                                 },
                                 logger_.Object,
                                 cts_.Token);
  }

  [TearDown]
  public async Task TearDown()
  {
    await cts_.CancelAsync();
    await connection_.DisposeAsync();
  }

  private CancellationTokenSource cts_;
  private Mock<ILogger<Client>>   logger_;
  private Mock<IServerHandler>    handler_;
  private Stream                  clientStream_;
  private Connection              connection_;

  [Test]
  [Timeout(10000)]
  [TestCase(RequestType.Close)]
  [TestCase(RequestType.Abort)]
  [TestCase(RequestType.Timeout)]
  [TestCase(RequestType.Reset)]
  public async Task OpenAmendClose(RequestType requestType)
  {
    var guid = Guid.NewGuid();

    handler_.Setup(m => m.OpenAsync(It.Is<Intent>(intent => intent.Id                         == guid),
                                    It.Is<byte[]>(payload => Encoding.UTF8.GetString(payload) == "open"),
                                    It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask)
            .Verifiable(Times.Once);
    handler_.Setup(m => m.AmendAsync(It.Is<Intent>(intent => intent.Id                         == guid),
                                     It.Is<byte[]>(payload => Encoding.UTF8.GetString(payload) == "amend"),
                                     It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask)
            .Verifiable(Times.Once);

    (requestType switch
     {
       RequestType.Close => handler_.Setup(m => m.CloseAsync(It.Is<Intent>(intent => intent.Id                         == guid),
                                                             It.Is<byte[]>(payload => Encoding.UTF8.GetString(payload) == requestType.ToString()),
                                                             It.IsAny<CancellationToken>())),
       RequestType.Abort => handler_.Setup(m => m.AbortAsync(It.Is<Intent>(intent => intent.Id                         == guid),
                                                             It.Is<byte[]>(payload => Encoding.UTF8.GetString(payload) == requestType.ToString()),
                                                             It.IsAny<CancellationToken>())),
       RequestType.Timeout => handler_.Setup(m => m.TimeoutAsync(It.Is<Intent>(intent => intent.Id                         == guid),
                                                                 It.Is<byte[]>(payload => Encoding.UTF8.GetString(payload) == requestType.ToString()),
                                                                 It.IsAny<CancellationToken>())),
       RequestType.Reset => handler_.Setup(m => m.ResetAsync(It.Is<Intent>(intent => intent.Id                         == guid),
                                                             It.Is<byte[]>(payload => Encoding.UTF8.GetString(payload) == requestType.ToString()),
                                                             It.IsAny<CancellationToken>())),
     }).Returns(Task.CompletedTask)
       .Verifiable(Times.Once);

    await clientStream_.RequestAsync(guid,
                                     RequestType.Open,
                                     "open"u8);

    var response = await Response.ReceiveAsync(clientStream_);

    Assert.That(response.IntentId,
                Is.EqualTo(guid));
    Assert.That(response.Type,
                Is.EqualTo(ResponseType.Success));
    Assert.That(response.Payload,
                Is.Empty);

    await clientStream_.RequestAsync(guid,
                                     RequestType.Amend,
                                     "amend"u8);


    response = await Response.ReceiveAsync(clientStream_);

    Assert.That(response.IntentId,
                Is.EqualTo(guid));
    Assert.That(response.Type,
                Is.EqualTo(ResponseType.Success));
    Assert.That(response.Payload,
                Is.Empty);

    await clientStream_.RequestAsync(guid,
                                     requestType,
                                     Encoding.UTF8.GetBytes(requestType.ToString()));


    response = await Response.ReceiveAsync(clientStream_);

    Assert.That(response.IntentId,
                Is.EqualTo(guid));
    Assert.That(response.Type,
                Is.EqualTo(ResponseType.Success));
    Assert.That(response.Payload,
                Is.Empty);

    await cts_.CancelAsync();
    await connection_.DisposeAsync();

    handler_.Verify();
  }

  [Test]
  [Timeout(10000)]
  public async Task Unclosed()
  {
    var guid = Guid.NewGuid();

    handler_.Setup(m => m.OpenAsync(It.Is<Intent>(intent => intent.Id                         == guid),
                                    It.Is<byte[]>(payload => Encoding.UTF8.GetString(payload) == "open"),
                                    It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask)
            .Verifiable(Times.Once);
    handler_.Setup(m => m.ResetAsync(It.Is<Intent>(intent => intent.Id                         == guid),
                                     It.Is<byte[]>(payload => Encoding.UTF8.GetString(payload) == ""),
                                     It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask)
            .Verifiable(Times.Once);

    await clientStream_.RequestAsync(guid,
                                     RequestType.Open,
                                     "open"u8);

    var response = await Response.ReceiveAsync(clientStream_);

    Assert.That(response.IntentId,
                Is.EqualTo(guid));
    Assert.That(response.Type,
                Is.EqualTo(ResponseType.Success));
    Assert.That(response.Payload,
                Is.Empty);

    await cts_.CancelAsync();
    await connection_.DisposeAsync();

    handler_.Verify();
  }

  [Test]
  [Timeout(10000)]
  public async Task OpenFailure()
  {
    var guid = Guid.NewGuid();

    handler_.Setup(m => m.OpenAsync(It.Is<Intent>(intent => intent.Id                         == guid),
                                    It.Is<byte[]>(payload => Encoding.UTF8.GetString(payload) == "open"),
                                    It.IsAny<CancellationToken>()))
            .ThrowsAsync(new ServerError("Error message",
                                         "error payload"u8.ToArray()))
            .Verifiable(Times.Once);
    handler_.Setup(m => m.ResetAsync(It.Is<Intent>(intent => intent.Id                         == guid),
                                     It.Is<byte[]>(payload => Encoding.UTF8.GetString(payload) == ""),
                                     It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask)
            .Verifiable(Times.Once);

    await clientStream_.RequestAsync(guid,
                                     RequestType.Open,
                                     "open"u8);

    var response = await Response.ReceiveAsync(clientStream_);

    Assert.That(response.IntentId,
                Is.EqualTo(guid));
    Assert.That(response.Type,
                Is.EqualTo(ResponseType.Error));
    Assert.That(response.Payload,
                Is.EqualTo("error payload"));

    await cts_.CancelAsync();
    await connection_.DisposeAsync();

    handler_.Verify();
  }


  [Test]
  [Timeout(10000)]
  [TestCase(RequestType.Amend)]
  [TestCase(RequestType.Close)]
  [TestCase(RequestType.Abort)]
  [TestCase(RequestType.Timeout)]
  [TestCase(RequestType.Reset)]
  public async Task OpenCloseFailure(RequestType requestType)
  {
    var guid = Guid.NewGuid();

    handler_.Setup(m => m.OpenAsync(It.Is<Intent>(intent => intent.Id                         == guid),
                                    It.Is<byte[]>(payload => Encoding.UTF8.GetString(payload) == "open"),
                                    It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask)
            .Verifiable(Times.Once);

    handler_.Setup(m => m.ResetAsync(It.Is<Intent>(intent => intent.Id       == guid),
                                     It.Is<byte[]>(payload => payload.Length == 0),
                                     It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask)
            .Verifiable(requestType is RequestType.Amend
                          ? Times.Once
                          : Times.Never);

    (requestType switch
     {
       RequestType.Amend => handler_.Setup(m => m.AmendAsync(It.Is<Intent>(intent => intent.Id                         == guid),
                                                             It.Is<byte[]>(payload => Encoding.UTF8.GetString(payload) == requestType.ToString()),
                                                             It.IsAny<CancellationToken>())),
       RequestType.Close => handler_.Setup(m => m.CloseAsync(It.Is<Intent>(intent => intent.Id                         == guid),
                                                             It.Is<byte[]>(payload => Encoding.UTF8.GetString(payload) == requestType.ToString()),
                                                             It.IsAny<CancellationToken>())),
       RequestType.Abort => handler_.Setup(m => m.AbortAsync(It.Is<Intent>(intent => intent.Id                         == guid),
                                                             It.Is<byte[]>(payload => Encoding.UTF8.GetString(payload) == requestType.ToString()),
                                                             It.IsAny<CancellationToken>())),
       RequestType.Timeout => handler_.Setup(m => m.TimeoutAsync(It.Is<Intent>(intent => intent.Id                         == guid),
                                                                 It.Is<byte[]>(payload => Encoding.UTF8.GetString(payload) == requestType.ToString()),
                                                                 It.IsAny<CancellationToken>())),
       RequestType.Reset => handler_.Setup(m => m.ResetAsync(It.Is<Intent>(intent => intent.Id                         == guid),
                                                             It.Is<byte[]>(payload => Encoding.UTF8.GetString(payload) == requestType.ToString()),
                                                             It.IsAny<CancellationToken>())),
     }).ThrowsAsync(new ServerError("Error message",
                                    "error payload"u8.ToArray()))
       .Verifiable(Times.Once);

    await clientStream_.RequestAsync(guid,
                                     RequestType.Open,
                                     "open"u8);

    var response = await Response.ReceiveAsync(clientStream_);

    Assert.That(response.IntentId,
                Is.EqualTo(guid));
    Assert.That(response.Type,
                Is.EqualTo(ResponseType.Success));
    Assert.That(response.Payload,
                Is.Empty);


    await clientStream_.RequestAsync(guid,
                                     requestType,
                                     Encoding.UTF8.GetBytes(requestType.ToString()));


    response = await Response.ReceiveAsync(clientStream_);

    Assert.That(response.IntentId,
                Is.EqualTo(guid));
    Assert.That(response.Type,
                Is.EqualTo(ResponseType.Error));
    Assert.That(response.Payload,
                Is.EqualTo("error payload"u8.ToArray()));

    await cts_.CancelAsync();
    await connection_.DisposeAsync();

    handler_.Verify();
  }
}
