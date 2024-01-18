// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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

public static class SessionStatusExt
{
  public static SessionStatus ToGrpcStatus(this Storage.SessionStatus status)
    => status switch
       {
         Storage.SessionStatus.Unspecified => SessionStatus.Unspecified,
         Storage.SessionStatus.Cancelled   => SessionStatus.Cancelled,
         Storage.SessionStatus.Running     => SessionStatus.Running,
         Storage.SessionStatus.Paused      => SessionStatus.Paused,
         Storage.SessionStatus.Purged      => SessionStatus.Purged,
         Storage.SessionStatus.Deleted     => SessionStatus.Deleted,
         _ => throw new ArgumentOutOfRangeException(nameof(status),
                                                    status,
                                                    null),
       };

  public static Storage.SessionStatus ToInternalStatus(this SessionStatus status)
    => status switch
       {
         SessionStatus.Unspecified => Storage.SessionStatus.Unspecified,
         SessionStatus.Cancelled   => Storage.SessionStatus.Cancelled,
         SessionStatus.Running     => Storage.SessionStatus.Running,
         SessionStatus.Paused      => Storage.SessionStatus.Paused,
         SessionStatus.Purged      => Storage.SessionStatus.Purged,
         SessionStatus.Deleted     => Storage.SessionStatus.Deleted,
         _ => throw new ArgumentOutOfRangeException(nameof(status),
                                                    status,
                                                    null),
       };
}
