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

using ArmoniK.Api.gRPC.V1.Sessions;
using ArmoniK.Core.Common.Storage;

using Google.Protobuf.WellKnownTypes;

namespace ArmoniK.Core.Common.gRPC.Convertors;

/// <summary>
///   Provides extension methods for converting between <see cref="SessionData" /> and gRPC message types.
/// </summary>
/// <remarks>
///   This static class contains conversion methods that transform internal session data structures
///   into their corresponding gRPC protocol representation, facilitating communication between
///   the core services and external clients or workers.
/// </remarks>
public static class SessionDataExt
{
  /// <summary>
  ///   Conversion operator from <see cref="SessionData" /> to <see cref="SessionRaw" />
  /// </summary>
  /// <param name="sessionData">The input status count</param>
  /// <returns>
  ///   The session data converted to gRPC format
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
         SessionId        = sessionData.SessionId,
         Status           = sessionData.Status.ToGrpcStatus(),
         WorkerSubmission = sessionData.WorkerSubmission,
         ClientSubmission = sessionData.ClientSubmission,
         DeletedAt = sessionData.DeletionDate is not null
                       ? Timestamp.FromDateTime(sessionData.DeletionDate.Value)
                       : null,
         PurgedAt = sessionData.PurgeDate is not null
                      ? Timestamp.FromDateTime(sessionData.PurgeDate.Value)
                      : null,
         Duration = sessionData.Duration is not null
                      ? Duration.FromTimeSpan(sessionData.Duration.Value)
                      : null,
         ClosedAt = sessionData.ClosureDate is not null
                      ? Timestamp.FromDateTime(sessionData.ClosureDate.Value)
                      : null,
       };
}
