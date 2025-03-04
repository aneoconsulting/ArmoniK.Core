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

public static class TaskStatusExt
{
  public static TaskStatus ToGrpcStatus(this Storage.TaskStatus status)
    => status switch
       {
         Storage.TaskStatus.Unspecified => TaskStatus.Unspecified,
         Storage.TaskStatus.Creating    => TaskStatus.Creating,
         Storage.TaskStatus.Submitted   => TaskStatus.Submitted,
         Storage.TaskStatus.Dispatched  => TaskStatus.Dispatched,
         Storage.TaskStatus.Completed   => TaskStatus.Completed,
         Storage.TaskStatus.Error       => TaskStatus.Error,
         Storage.TaskStatus.Timeout     => TaskStatus.Timeout,
         Storage.TaskStatus.Cancelling  => TaskStatus.Cancelling,
         Storage.TaskStatus.Cancelled   => TaskStatus.Cancelled,
         Storage.TaskStatus.Processing  => TaskStatus.Processing,
         Storage.TaskStatus.Processed   => TaskStatus.Processed,
         Storage.TaskStatus.Retried     => TaskStatus.Retried,
         Storage.TaskStatus.Pending     => TaskStatus.Pending,
         Storage.TaskStatus.Paused      => TaskStatus.Paused,
         _ => throw new ArgumentOutOfRangeException(nameof(status),
                                                    status,
                                                    null),
       };

  public static Storage.TaskStatus ToInternalStatus(this TaskStatus status)
    => status switch
       {
         TaskStatus.Unspecified => Storage.TaskStatus.Unspecified,
         TaskStatus.Creating    => Storage.TaskStatus.Creating,
         TaskStatus.Submitted   => Storage.TaskStatus.Submitted,
         TaskStatus.Dispatched  => Storage.TaskStatus.Dispatched,
         TaskStatus.Completed   => Storage.TaskStatus.Completed,
         TaskStatus.Error       => Storage.TaskStatus.Error,
         TaskStatus.Timeout     => Storage.TaskStatus.Timeout,
         TaskStatus.Cancelling  => Storage.TaskStatus.Cancelling,
         TaskStatus.Cancelled   => Storage.TaskStatus.Cancelled,
         TaskStatus.Processing  => Storage.TaskStatus.Processing,
         TaskStatus.Processed   => Storage.TaskStatus.Processed,
         TaskStatus.Retried     => Storage.TaskStatus.Retried,
         TaskStatus.Pending     => Storage.TaskStatus.Pending,
         TaskStatus.Paused      => Storage.TaskStatus.Paused,
         _ => throw new ArgumentOutOfRangeException(nameof(status),
                                                    status,
                                                    null),
       };
}
