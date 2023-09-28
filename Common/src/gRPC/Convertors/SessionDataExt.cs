// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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

using ArmoniK.Api.gRPC.V1.Sessions;
using ArmoniK.Core.Common.Storage;

using Google.Protobuf.WellKnownTypes;

namespace ArmoniK.Core.Common.gRPC.Convertors;

public static class SessionDataExt
{
  /// <summary>
  ///   Conversion operator from <see cref="SessionData" /> to <see cref="SessionRaw" />
  /// </summary>
  /// <param name="sessionData">The input status count</param>
  /// <returns>
  ///   The converted status count
  /// </returns>
  public static SessionRaw ToGrpcSessionRaw(this SessionData sessionData)
    => new()
       {
         CancelledAt = sessionData.CancellationDate is not null
                         ? Timestamp.FromDateTime(sessionData.CancellationDate.Value)
                         : null,
         CreatedAt = Timestamp.FromDateTime(sessionData.CreationDate),
         Options   = sessionData.Options.ToGrpcTaskOptions(),
         PartitionIds =
         {
           sessionData.PartitionIds,
         },
         SessionId = sessionData.SessionId,
         Status    = sessionData.Status.ToGrpcStatus(),
       };
}
