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

using MessagePack;

namespace ArmoniK.Core.Control.IntentLog.Protocol.Messages;

[MessagePackObject]
public class Request<T>
  where T : class
{
  /// <summary>
  ///   ID of the intent within the connection
  /// </summary>
  [Key(0)]
  public int IntentId { get; set; }

  /// <summary>
  ///   Action on the intent (eg: Open, Close)
  /// </summary>
  [Key(1)]
  public Action Action { get; set; }

  /// <summary>
  ///   Data related to the payload
  /// </summary>
  [Key(2)]
  public T? Payload { get; set; }

  public async Task SendAsync(Stream            stream,
                              CancellationToken cancellationToken = default)
  {
    var body = MessagePackSerializer.Serialize(Payload);
    var size = body.Length;
    var msg  = new byte[size + 12];

    BitConverter.GetBytes(IntentId)
                .CopyTo(msg.AsSpan(0,
                                   4));
    BitConverter.GetBytes((int)Action)
                .CopyTo(msg.AsSpan(4,
                                   4));
    BitConverter.GetBytes(size)
                .CopyTo(msg.AsSpan(8,
                                   4));

    body.CopyTo(msg.AsSpan(12,
                           size));

    await stream.WriteAsync(msg,
                            cancellationToken)
                .ConfigureAwait(false);
  }

  public static async Task<Request<T>> ReceiveAsync(Stream            stream,
                                                    CancellationToken cancellationToken = default)
  {
    var header = new byte[12];
    await stream.ReadExactlyAsync(header,
                                  cancellationToken)
                .ConfigureAwait(false);

    var intentId = BitConverter.ToInt32(header,
                                        0);
    var action = BitConverter.ToInt32(header,
                                      4);
    var size = BitConverter.ToInt32(header,
                                    8);
    var body = new byte[size];

    await stream.ReadExactlyAsync(body,
                                  cancellationToken)
                .ConfigureAwait(false);

    var payload = MessagePackSerializer.Deserialize<T>(body);

    return new Request<T>
           {
             IntentId = intentId,
             Action   = (Action)action,
             Payload  = payload,
           };
  }
}
