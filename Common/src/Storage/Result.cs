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

using System;
using System.Collections.Generic;

using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Core.Common.gRPC.Convertors;

using static Google.Protobuf.WellKnownTypes.Timestamp;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
/// </summary>
/// <param name="SessionId">Id of the session that produces and consumes this data</param>
/// <param name="ResultId">Unique Id of the result</param>
/// <param name="Name">Name to reference and access this result</param>
/// <param name="OwnerTaskId">Id of the task that is responsible of generating this result.</param>
/// <param name="Status">Status of the result (can be Created, Completed or Aborted)</param>
/// <param name="DependentTasks">List of tasks that depend on this result.</param>
/// <param name="CreationDate">Date of creation of the current object.</param>
/// <param name="Data">Data for the current <paramref name="Name" /></param>
public record Result(string       SessionId,
                     string       ResultId,
                     string       Name,
                     string       OwnerTaskId,
                     ResultStatus Status,
                     List<string> DependentTasks,
                     DateTime     CreationDate,
                     byte[]       Data)
{
  /// <summary>
  ///   Conversion operator from <see cref="Result" /> to <see cref="ResultRaw" />
  /// </summary>
  /// <param name="result">The input result data</param>
  /// <returns>
  ///   The converted result data
  /// </returns>
  public static implicit operator ResultRaw(Result result)
    => new()
       {
         SessionId   = result.SessionId,
         Status      = result.Status.ToGrpcStatus(),
         CreatedAt   = FromDateTime(result.CreationDate),
         Name        = result.Name,
         OwnerTaskId = result.OwnerTaskId,
         ResultId    = result.ResultId,
       };
}
