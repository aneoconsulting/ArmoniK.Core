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
///   Extension methods for converting between internal and gRPC <see cref="Storage.ResultStatus" /> statuses.
/// </summary>
public static class ResultStatusExt
{
  /// <summary>
  ///   Converts the internal <see cref="Storage.ResultStatus" /> to the gRPC <see cref="ResultStatus" />.
  /// </summary>
  /// <param name="status">The internal status to convert.</param>
  /// <returns>The corresponding gRPC status.</returns>
  public static ResultStatus ToGrpcStatus(this Storage.ResultStatus status)
    => status switch
       {
         Storage.ResultStatus.Unspecified => ResultStatus.Unspecified,
         Storage.ResultStatus.Created     => ResultStatus.Created,
         Storage.ResultStatus.Completed   => ResultStatus.Completed,
         Storage.ResultStatus.Aborted     => ResultStatus.Aborted,
         Storage.ResultStatus.DeletedData => ResultStatus.Deleted,
         _ => throw new ArgumentOutOfRangeException(nameof(status),
                                                    status,
                                                    null),
       };

  /// <summary>
  ///   Converts the gRPC <see cref="ResultStatus" /> to the internal <see cref="Storage.ResultStatus" />.
  /// </summary>
  /// <param name="status">The gRPC status to convert.</param>
  /// <returns>The corresponding internal status.</returns>
  public static Storage.ResultStatus ToInternalStatus(this ResultStatus status)
    => status switch
       {
         ResultStatus.Unspecified => Storage.ResultStatus.Unspecified,
         ResultStatus.Created     => Storage.ResultStatus.Created,
         ResultStatus.Completed   => Storage.ResultStatus.Completed,
         ResultStatus.Aborted     => Storage.ResultStatus.Aborted,
         ResultStatus.Notfound    => Storage.ResultStatus.Unspecified,
         ResultStatus.Deleted     => Storage.ResultStatus.DeletedData,
         _ => throw new ArgumentOutOfRangeException(nameof(status),
                                                    status,
                                                    null),
       };
}
