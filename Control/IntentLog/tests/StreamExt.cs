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

using ArmoniK.Core.Control.IntentLog.Protocol.Messages;

namespace ArmoniK.Core.Control.IntentLog.Tests.Utils;

public static class StreamExt
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

  public static Task RequestAsync(this Stream        stream,
                                  Guid               intentId,
                                  RequestType        requestType,
                                  ReadOnlySpan<byte> payload           = default,
                                  CancellationToken  cancellationToken = default)
    => new Request
       {
         IntentId = intentId,
         Type     = requestType,
         Payload  = payload.ToArray(),
       }.SendAsync(stream,
                   cancellationToken);
}
