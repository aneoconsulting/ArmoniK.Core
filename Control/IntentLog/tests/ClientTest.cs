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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Control.IntentLog.Protocol.Client;
using ArmoniK.Core.Control.IntentLog.Protocol.Messages;
using ArmoniK.Core.Control.IntentLog.Tests.Utils;

using Microsoft.Extensions.Logging;

using Moq;

using NUnit.Framework;

namespace ArmoniK.Core.Control.IntentLog.Tests;

[TestFixture(TestOf = typeof(Client))]
public class ClientTest
{
  [Test]
  [Timeout(1000)]
  public async Task OpenClose()
  {
    var logger = new Mock<ILogger<Client>>();
    var (clientStream, serverStream) = ChannelStream.CreatePair();
    await using var client = new Client(clientStream,
                                        logger.Object);

    var intentTask = client.OpenAsync("payload"u8.ToArray());
    var request    = await Request.ReceiveAsync(serverStream);
    Assert.That(request.Type,
                Is.EqualTo(RequestType.Open));
    Assert.That(request.Payload,
                Is.EqualTo("payload"u8.ToArray()));

    await serverStream.RespondSuccessAsync(request.IntentId);

    await using var intent = await intentTask.ConfigureAwait(false);

    var closeTask = intent.CloseAsync(Array.Empty<byte>());
    request = await Request.ReceiveAsync(serverStream);
    await serverStream.RespondSuccessAsync(request.IntentId);

    await closeTask;
    logger.VerifyNoLog(LogLevel.Information);
  }

  [Test]
  [Timeout(1000)]
  public async Task ParallelIntents()
  {
    var logger = new Mock<ILogger<Client>>();
    var (clientStream, serverStream) = ChannelStream.CreatePair();
    await using var client = new Client(clientStream,
                                        logger.Object);

    var intentTask1 = client.OpenAsync("payload1"u8.ToArray());
    var request1    = await Request.ReceiveAsync(serverStream);
    var intentTask2 = client.OpenAsync("payload2"u8.ToArray());
    var request2    = await Request.ReceiveAsync(serverStream);

    Assert.That(request1.Type,
                Is.EqualTo(RequestType.Open));
    Assert.That(request1.Payload,
                Is.EqualTo("payload1"));
    Assert.That(request2.Type,
                Is.EqualTo(RequestType.Open));
    Assert.That(request2.Payload,
                Is.EqualTo("payload2"));

    await Task.Delay(10);

    Assert.That(intentTask1.IsCompleted,
                Is.False);
    Assert.That(intentTask2.IsCompleted,
                Is.False);

    await serverStream.RespondSuccessAsync(request2.IntentId);

    await Task.Delay(10);
    Assert.That(intentTask1.IsCompleted,
                Is.False);
    Assert.That(intentTask2.IsCompleted,
                Is.True);

    await using var intent2 = await intentTask2;

    await serverStream.RespondSuccessAsync(request1.IntentId);

    await Task.Delay(10);
    Assert.That(intentTask1.IsCompleted,
                Is.True);
    Assert.That(intentTask2.IsCompleted,
                Is.True);

    await using var intent1 = await intentTask2;

    var closeTask1 = intent1.CloseAsync(Array.Empty<byte>());
    var closeTask2 = intent2.CloseAsync(Array.Empty<byte>());

    request1 = await Request.ReceiveAsync(serverStream);
    request2 = await Request.ReceiveAsync(serverStream);

    await serverStream.RespondSuccessAsync(request1.IntentId);
    await serverStream.RespondSuccessAsync(request2.IntentId);

    await Task.WhenAll(closeTask1,
                       closeTask2);
    logger.VerifyNoLog(LogLevel.Information);
  }

  [Test]
  [TestCase(null)]
  [TestCase(RequestType.Close)]
  [TestCase(RequestType.Abort)]
  [TestCase(RequestType.Timeout)]
  [TestCase(RequestType.Reset)]
  public async Task DisposeIntent(RequestType? type)
  {
    var logger = new Mock<ILogger<Client>>();
    var (clientStream, serverStream) = ChannelStream.CreatePair();
    await using var client = new Client(clientStream,
                                        logger.Object);

    var intentTask = client.OpenAsync("payload"u8.ToArray());
    var request    = await Request.ReceiveAsync(serverStream);

    await serverStream.RespondSuccessAsync(request.IntentId);


    var intent = await intentTask;

    var disposePayload = "dispose"u8.ToArray();
    Action<byte[]> setOnDispose = type switch
                                  {
                                    RequestType.Close   => intent.CloseOnDispose,
                                    RequestType.Abort   => intent.AbortOnDispose,
                                    RequestType.Timeout => intent.TimeoutOnDispose,
                                    RequestType.Reset   => intent.ResetOnDispose,
                                    _                   => _ => disposePayload = [],
                                  };
    setOnDispose(disposePayload);

    var disposeTask = intent.DisposeAsync();
    request = await Request.ReceiveAsync(serverStream);

    Assert.That(request.Type,
                Is.EqualTo(type ?? RequestType.Close));
    Assert.That(request.Payload,
                Is.EqualTo(disposePayload));
    await serverStream.RespondSuccessAsync(request.IntentId);
    await disposeTask;
    logger.VerifyNoLog(LogLevel.Information);
  }

  [Test]
  [Timeout(1000)]
  [Repeat(10)]
  public async Task OpenStreamClosed([Values] bool early)
  {
    var logger = new Mock<ILogger<Client>>();
    var (clientStream, serverStream) = ChannelStream.CreatePair();

    if (early)
    {
      serverStream.Close();
    }

    await using var client = new Client(clientStream,
                                        logger.Object);
    if (!early)
    {
      serverStream.Close();
    }

    Assert.That(() => client.OpenAsync("open"u8.ToArray()),
                Throws.InstanceOf<EndOfStreamException>());
    logger.VerifyNoLog(LogLevel.Information);
  }

  [Test]
  [Timeout(10000)]
  [Repeat(10)]
  public async Task AmendStreamClosed([Values] bool early)
  {
    var logger = new Mock<ILogger<Client>>();
    var (clientStream, serverStream) = ChannelStream.CreatePair();
    await using var client = new Client(clientStream,
                                        logger.Object);

    var intentTask = client.OpenAsync("open"u8.ToArray());
    var request    = await Request.ReceiveAsync(serverStream);

    await serverStream.RespondSuccessAsync(request.IntentId);

    var intent = await intentTask;

    if (early)
    {
      serverStream.Close();
    }

    var amendTask = intent.AmendAsync("amend"u8.ToArray());

    if (!early)
    {
      await Task.Delay(10);
      serverStream.Close();
    }


    Assert.That(() => amendTask,
                Throws.InstanceOf<EndOfStreamException>());
    logger.VerifyNoLog(LogLevel.Information);
  }

  [Test]
  [Timeout(10000)]
  public async Task OpenFailure()
  {
    var logger = new Mock<ILogger<Client>>();
    var (clientStream, serverStream) = ChannelStream.CreatePair();
    await using var client = new Client(clientStream,
                                        logger.Object);

    var intentTask = client.OpenAsync("open"u8.ToArray());
    var request    = await Request.ReceiveAsync(serverStream);

    await serverStream.RespondErrorAsync(request.IntentId,
                                         "Error"u8);

    Assert.That(() => intentTask,
                Throws.Exception);
    logger.VerifyNoLog(LogLevel.Information);
  }

  [Test]
  [Timeout(10000)]
  [TestCase(RequestType.Amend)]
  [TestCase(RequestType.Close)]
  [TestCase(RequestType.Abort)]
  public async Task IntentFailure(RequestType requestType)
  {
    var logger = new Mock<ILogger<Client>>();
    var (clientStream, serverStream) = ChannelStream.CreatePair();
    await using var client = new Client(clientStream,
                                        logger.Object);

    var intentTask = client.OpenAsync("open"u8.ToArray());
    var request    = await Request.ReceiveAsync(serverStream);

    await serverStream.RespondSuccessAsync(request.IntentId);

    var intent = await intentTask;

    Func<byte[], CancellationToken, Task> requestFunc = requestType switch
                                                        {
                                                          RequestType.Amend => intent.AmendAsync,
                                                          RequestType.Close => intent.CloseAsync,
                                                          RequestType.Abort => intent.AbortAsync,
                                                        };
    var requestTask = requestFunc("payload"u8.ToArray(),
                                  CancellationToken.None);
    request = await Request.ReceiveAsync(serverStream);

    await serverStream.RespondErrorAsync(request.IntentId,
                                         "Error"u8);

    Assert.That(() => requestTask,
                Throws.Exception);
    logger.VerifyNoLog(LogLevel.Information);
  }

  [Test]
  [Timeout(10000)]
  [TestCase(RequestType.Close)]
  [TestCase(RequestType.Abort)]
  [TestCase(RequestType.Timeout)]
  [TestCase(RequestType.Reset)]
  public async Task IntentDisposeFailure(RequestType requestType)
  {
    var logger = new Mock<ILogger<Client>>();

    var (clientStream, serverStream) = ChannelStream.CreatePair();
    await using var client = new Client(clientStream,
                                        logger.Object);

    var intentTask = client.OpenAsync("open"u8.ToArray());
    var request    = await Request.ReceiveAsync(serverStream);

    await serverStream.RespondSuccessAsync(request.IntentId);

    var intent = await intentTask;

    Action<byte[]> onDispose = requestType switch
                               {
                                 RequestType.Close   => intent.CloseOnDispose,
                                 RequestType.Abort   => intent.AbortOnDispose,
                                 RequestType.Timeout => intent.TimeoutOnDispose,

                                 RequestType.Reset => intent.ResetOnDispose,
                               };
    onDispose(""u8.ToArray());
    var disposeTask = intent.DisposeAsync();
    request = await Request.ReceiveAsync(serverStream);

    await serverStream.RespondErrorAsync(request.IntentId,
                                         "Error"u8);

    Assert.That(() => disposeTask,
                Throws.Nothing);

    logger.Verify(m => m.Log(LogLevel.Error,
                             It.IsAny<EventId>(),
                             It.Is<It.IsAnyType>((x,
                                                  y) => x.ToString()!.StartsWith("Error while releasing intent")),
                             It.Is<Exception?>(x => x!.Message.StartsWith("Unknown response:")),
                             It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                  Times.Once);
    logger.VerifyNoLog(LogLevel.Critical);
  }

  [Test]
  [Timeout(10000)]
  [TestCase(ResponseType.Success)]
  [TestCase(ResponseType.Error)]
  [TestCase((ResponseType)100)]
  public async Task BadIntent(ResponseType responseType)
  {
    var logger = new Mock<ILogger<Client>>();
    var (clientStream, serverStream) = ChannelStream.CreatePair();
    await using (var client = new Client(clientStream,
                                         logger.Object))
    {
      await serverStream.RespondAsync(Guid.Empty,
                                      responseType);
      await Task.Delay(10);
    }

    logger.Verify(m => m.Log(LogLevel.Error,
                             It.IsAny<EventId>(),
                             It.Is<It.IsAnyType>((x,
                                                  y) => x.ToString()!.StartsWith("Client error: Received incorrect intent ID from server:")),
                             It.IsAny<Exception?>(),
                             It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                  Times.Once);
    logger.VerifyNoLog(LogLevel.Critical);
  }

  [Test]
  [Timeout(10000)]
  public async Task Ping()
  {
    var logger = new Mock<ILogger<Client>>();
    var (clientStream, serverStream) = ChannelStream.CreatePair();
    await using var client = new Client(clientStream,
                                        logger.Object);

    await serverStream.RespondAsync(new Guid("abcdefghijklmnop"u8),
                                    ResponseType.Ping,
                                    "Ping payload"u8);

    var request = await Request.ReceiveAsync(serverStream);

    Assert.That(request.IntentId,
                Is.EqualTo(new Guid("abcdefghijklmnop"u8)));
    Assert.That(request.Type,
                Is.EqualTo(RequestType.Pong));
    Assert.That(request.Payload,
                Is.EqualTo("Ping payload"));
    logger.VerifyNoLog(LogLevel.Information);
  }

  [Test]
  [Timeout(10000)]
  public async Task Pong()
  {
    var logger = new Mock<ILogger<Client>>();
    var (clientStream, serverStream) = ChannelStream.CreatePair();
    await using var client = new Client(clientStream,
                                        logger.Object);

    await serverStream.RespondAsync(new Guid("abcdefghijklmnop"u8),
                                    ResponseType.Pong,
                                    "Pong payload"u8);
    await Task.Delay(10);
    logger.VerifyNoLog(LogLevel.Information);
  }
}

public static class StreamExtension
{
  public static Task RespondAsync(this Stream        stream,
                                  Guid               intentId,
                                  ResponseType       type,
                                  ReadOnlySpan<byte> payload           = default,
                                  CancellationToken  cancellationToken = default)
    => new Response
       {
         IntentId = intentId,
         Type     = type,
         Payload  = payload.ToArray(),
       }.SendAsync(stream,
                   cancellationToken);

  public static Task RespondSuccessAsync(this Stream       stream,
                                         Guid              intentId,
                                         CancellationToken cancellationToken = default)
    => RespondAsync(stream,
                    intentId,
                    ResponseType.Success,
                    cancellationToken: cancellationToken);

  public static Task RespondErrorAsync(this Stream        stream,
                                       Guid               intentId,
                                       ReadOnlySpan<byte> payload,
                                       CancellationToken  cancellationToken = default)
    => RespondAsync(stream,
                    intentId,
                    ResponseType.Error,
                    payload,
                    cancellationToken);
}
