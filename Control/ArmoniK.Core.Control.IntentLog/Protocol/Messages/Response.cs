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

using System.IO;
using System.Threading;
using System.Threading.Tasks;

using MessagePack;

namespace ArmoniK.Core.Control.IntentLog.Protocol.Messages;

[MessagePackObject]
public class Response
{
  /// <summary>
  ///   ID of the intent within the connection
  /// </summary>
  [Key(0)]
  public int IntentId { get; set; }

  /// <summary>
  ///   Error response in case it is an error
  /// </summary>
  [Key(1)]
  public object? Error { get; set; }

  public Task SendAsync(Stream            stream,
                        CancellationToken cancellationToken = default)
    => MessagePackSerializer.SerializeAsync(stream,
                                            this,
                                            cancellationToken: cancellationToken);

  public static Task<Response> ReceiveAsync(Stream            stream,
                                            CancellationToken cancellationToken = default)
    => MessagePackSerializer.DeserializeAsync<Response>(stream,
                                                        cancellationToken: cancellationToken)
                            .AsTask();
}
