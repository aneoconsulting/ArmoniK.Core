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

using ArmoniK.Api.gRPC.V1;

namespace ArmoniK.Core.Common.gRPC.Convertors;

/// <summary>
///   Provides extension methods for converting between internal <see cref="Storage.SessionStatus" /> and
///   gRPC <see cref="SessionStatus" /> enumeration values.
/// </summary>
/// <remarks>
///   This static class facilitates bidirectional conversion between the internal representation
///   of session status and the gRPC protocol representation used for external communication.
/// </remarks>
public static class SessionStatusExt
{
  /// <summary>
  ///   Converts an internal session status to its corresponding gRPC representation.
  /// </summary>
  /// <param name="status">The internal session status to convert.</param>
  /// <returns>The equivalent gRPC session status.</returns>
  /// <exception cref="ArgumentOutOfRangeException">Thrown when the input status is not recognized.</exception>
  public static SessionStatus ToGrpcStatus(this Storage.SessionStatus status)
    => status switch
       {
         Storage.SessionStatus.Unspecified => SessionStatus.Unspecified,
         Storage.SessionStatus.Cancelled   => SessionStatus.Cancelled,
         Storage.SessionStatus.Running     => SessionStatus.Running,
         Storage.SessionStatus.Paused      => SessionStatus.Paused,
         Storage.SessionStatus.Purged      => SessionStatus.Purged,
         Storage.SessionStatus.Deleted     => SessionStatus.Deleted,
         Storage.SessionStatus.Closed      => SessionStatus.Closed,
         _ => throw new ArgumentOutOfRangeException(nameof(status),
                                                    status,
                                                    null),
       };

  /// <summary>
  ///   Converts a gRPC session status to its corresponding internal representation.
  /// </summary>
  /// <param name="status">The gRPC session status to convert.</param>
  /// <returns>The equivalent internal session status.</returns>
  /// <exception cref="ArgumentOutOfRangeException">Thrown when the input status is not recognized.</exception>
  public static Storage.SessionStatus ToInternalStatus(this SessionStatus status)
    => status switch
       {
         SessionStatus.Unspecified => Storage.SessionStatus.Unspecified,
         SessionStatus.Cancelled   => Storage.SessionStatus.Cancelled,
         SessionStatus.Running     => Storage.SessionStatus.Running,
         SessionStatus.Paused      => Storage.SessionStatus.Paused,
         SessionStatus.Purged      => Storage.SessionStatus.Purged,
         SessionStatus.Deleted     => Storage.SessionStatus.Deleted,
         SessionStatus.Closed      => Storage.SessionStatus.Closed,
         _ => throw new ArgumentOutOfRangeException(nameof(status),
                                                    status,
                                                    null),
       };
}
