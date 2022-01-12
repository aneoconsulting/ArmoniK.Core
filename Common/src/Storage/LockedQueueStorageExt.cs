// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
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

using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.gRPC.V1;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Storage;

[PublicAPI]
public static class LockedQueueStorageExt
{
  public static Task EnqueueAsync(this ILockedQueueStorage lockedQueueStorage,
                                  TaskId                   message,
                                  int                      priority,
                                  CancellationToken        cancellationToken = default)
    => lockedQueueStorage.EnqueueMessagesAsync(new[] { message },
                                               priority,
                                               cancellationToken);

  public static LockedQueueMessageDeadlineHandler GetDeadlineHandler(this ILockedQueueStorage lockedQueueStorage,
                                                                     string                   messageId,
                                                                     ILogger                  logger,
                                                                     CancellationToken        cancellationToken = default)
    => new(lockedQueueStorage,
           messageId,
           logger,
           cancellationToken);
}