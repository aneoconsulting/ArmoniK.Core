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

namespace ArmoniK.Core.Control.IntentLog.Protocol.Messages;

public class Response
{
  /// <summary>
  ///   ID of the intent within the connection
  /// </summary>
  public Guid IntentId { get; set; }

  /// <summary>
  ///   Type of the response
  /// </summary>
  public ResponseType Type { get; set; }

  /// <summary>
  ///   Paylod of the response
  /// </summary>
  public byte[] Payload { get; set; } = Array.Empty<byte>();

  public async Task SendAsync(Stream            stream,
                              CancellationToken cancellationToken = default)
  {
    var size = Payload.Length;
    var msg  = new byte[size + 24];

    IntentId.TryWriteBytes(msg.AsSpan(0,
                                      16));
    BitConverter.GetBytes((int)Type)
                .CopyTo(msg.AsSpan(16,
                                   4));
    BitConverter.GetBytes(size)
                .CopyTo(msg.AsSpan(20,
                                   4));

    Payload.CopyTo(msg.AsSpan(24,
                              size));

    await stream.WriteAsync(msg,
                            cancellationToken)
                .ConfigureAwait(false);
  }

  public static async Task<Response> ReceiveAsync(Stream            stream,
                                                  CancellationToken cancellationToken = default)
  {
    var header = new byte[24];
    await stream.ReadExactlyAsync(header,
                                  cancellationToken)
                .ConfigureAwait(false);

    var intentId = new Guid(header.AsSpan(0,
                                          16));
    var type = BitConverter.ToInt32(header,
                                    16);
    var size = BitConverter.ToInt32(header,
                                    20);
    var payload = new byte[size];

    await stream.ReadExactlyAsync(payload,
                                  cancellationToken)
                .ConfigureAwait(false);

    return new Response
           {
             IntentId = intentId,
             Type     = (ResponseType)type,
             Payload  = payload,
           };
  }
}

public enum ResponseType
{
  /// <summary>
  ///   Ping
  /// </summary>
  Ping = 0,

  /// <summary>
  ///   Pong
  /// </summary>
  Pong = 1,

  /// <summary>
  ///   Intent action has been successful
  /// </summary>
  Success = 2,

  /// <summary>
  ///   Intent action has failed
  /// </summary>
  Error = 3,
}
