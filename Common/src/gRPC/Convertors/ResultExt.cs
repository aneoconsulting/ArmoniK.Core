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

using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Core.Common.Storage;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

namespace ArmoniK.Core.Common.gRPC.Convertors;

public static class ResultExt
{
  /// <summary>
  ///   Conversion operator from <see cref="Result" /> to <see cref="ResultRaw" />
  /// </summary>
  /// <param name="result">The input result data</param>
  /// <returns>
  ///   The converted result data
  /// </returns>
  public static ResultRaw ToGrpcResultRaw(this Result result)
    => new()
       {
         SessionId   = result.SessionId,
         Status      = result.Status.ToGrpcStatus(),
         CreatedAt   = Timestamp.FromDateTime(result.CreationDate),
         Name        = result.Name,
         CreatedBy   = result.CreatedBy,
         OwnerTaskId = result.OwnerTaskId,
         ResultId    = result.ResultId,
         Size        = result.Size,
         OpaqueId    = ByteString.CopyFrom(result.OpaqueId),
       };
}
