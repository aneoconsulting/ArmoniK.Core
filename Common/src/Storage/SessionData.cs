// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Sessions;

using static Google.Protobuf.WellKnownTypes.Timestamp;

namespace ArmoniK.Core.Common.Storage;

public record SessionData(string        SessionId,
                          SessionStatus Status,
                          DateTime      CreationDate,
                          DateTime?     CancellationDate,
                          IList<string> PartitionIds,
                          TaskOptions   Options)
{
  public SessionData(string        sessionId,
                     SessionStatus status,
                     IList<string> partitionIds,
                     TaskOptions   options)
    : this(sessionId,
           status,
           DateTime.UtcNow,
           null,
           partitionIds,
           options)
  {
  }

  public static implicit operator SessionRaw(SessionData sessionData)
    => new()
       {
         CancelledAt = sessionData.CancellationDate is not null
                         ? FromDateTime(sessionData.CancellationDate.Value)
                         : null,
         CreatedAt = FromDateTime(sessionData.CreationDate),
         Options   = sessionData.Options,
         PartitionIds =
         {
           sessionData.PartitionIds,
         },
         SessionId = sessionData.SessionId,
         Status    = sessionData.Status,
       };

  public static implicit operator SessionSummary(SessionData sessionData)
    => new()
       {
         CancelledAt = sessionData.CancellationDate is not null
                         ? FromDateTime(sessionData.CancellationDate.Value)
                         : null,
         CreatedAt = FromDateTime(sessionData.CreationDate),
         SessionId = sessionData.SessionId,
         Status    = sessionData.Status,
       };
}
