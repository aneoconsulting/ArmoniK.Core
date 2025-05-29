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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using ArmoniK.Core.Control.IntentLog.Protocol.Messages;

using NUnit.Framework;

namespace ArmoniK.Core.Control.IntentLog.Tests;

[TestFixture(TestOf = typeof(Request))]
public class RequestTest
{
  private static readonly byte[] LoremIpsum =
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed metus nisi, efficitur porttitor diam non, imperdiet tincidunt est. Quisque maximus finibus leo nec consectetur. Suspendisse at eros at metus viverra convallis non sit amet tellus. Etiam et orci scelerisque, sagittis sapien sit amet, porttitor arcu. Suspendisse libero est, vehicula vel eros eu, ultrices malesuada erat. Etiam non sollicitudin lectus. Etiam ut cursus ipsum, sit amet feugiat orci. Aliquam semper ligula nec euismod sodales. Mauris eget varius sapien. Vivamus ornare ut mi quis ultricies. Quisque non vulputate magna. Nullam at arcu ac nulla rhoncus tincidunt in in diam. In vitae leo risus."u8
      .ToArray();

  private static readonly Guid[] GuidTestCases =
  [
    Guid.Empty, new([255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255]), new("ABCDEFGHIJKLMNOP"u8),
  ];

  private static readonly (RequestType, byte[])[] RequestTypeTestCases =
  [
    (RequestType.Ping, [0, 0, 0, 0]), (RequestType.Pong, [1, 0, 0, 0]), (RequestType.Open, [2, 0, 0, 0]), (RequestType.Amend, [3, 0, 0, 0]),
    (RequestType.Close, [4, 0, 0, 0]), (RequestType.Abort, [5, 0, 0, 0]), (RequestType.Timeout, [6, 0, 0, 0]), (RequestType.Reset, [7, 0, 0, 0]),
  ];

  private static readonly (byte[], byte[])[] PayloadTestCases =
  [
    (""u8.ToArray(), [0, 0, 0, 0]), ("payload"u8.ToArray(), [7, 0, 0, 0]), ([61, 62, 63, 0, 64, 65, 66, 255, 67, 68, 69], [11, 0, 0, 0]), (LoremIpsum, [155, 2, 0, 0]),
  ];

  public static IEnumerable<TestCaseData> TestCases()
  {
    foreach (var guid in GuidTestCases)
    {
      foreach (var (type, serializedType) in RequestTypeTestCases)
      {
        foreach (var (payload, serializedPayloadSize) in PayloadTestCases)
        {
          var request = new Request
                        {
                          IntentId = guid,
                          Type     = type,
                          Payload  = payload,
                        };
          var serialized = new[]
            {
              guid.ToByteArray(),
              serializedType,
              serializedPayloadSize,
              payload,
            }.SelectMany(x => x)
             .ToArray();

          var payloadDisplay = Encoding.UTF8.GetString(payload);
          if (payloadDisplay.Length > 19)
          {
            payloadDisplay = $"{payloadDisplay[..16]}...";
          }

          yield return new TestCaseData(request,
                                        serialized).SetArgDisplayNames(guid.ToString(),
                                                                       type.ToString(),
                                                                       payloadDisplay);
        }
      }
    }
  }

  [Test]
  [TestCaseSource(nameof(TestCases))]
  public async Task Serialize(Request request,
                              byte[]  expected)
  {
    var stream = new MemoryStream();

    await request.SendAsync(stream);

    var serialized = stream.ToArray();

    await Console.Error.WriteLineAsync($"Actual  : {BitConverter.ToString(serialized)}\nExpected: {BitConverter.ToString(expected)}");
    Assert.That(serialized,
                Is.EqualTo(expected));
  }

  [Test]
  [TestCaseSource(nameof(TestCases))]
  public async Task Deserialize(Request expected,
                                byte[]  serialized)
  {
    var stream = new MemoryStream(serialized);

    var request = await Request.ReceiveAsync(stream);

    Assert.That(request.IntentId,
                Is.EqualTo(expected.IntentId));
    Assert.That(request.Type,
                Is.EqualTo(expected.Type));
    Assert.That(request.Payload,
                Is.EqualTo(expected.Payload));
  }
}
