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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Utils;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Storage;

[PublicAPI]
public class LockedQueueMessageDeadlineHandler : IAsyncDisposable
{
  private readonly CancellationToken   cancellationToken_;
  private readonly Heart               heart_;
  private readonly string              id_;
  private readonly ILockedQueueStorage lockedQueueStorage_;
  private readonly ILogger             logger_;

  public LockedQueueMessageDeadlineHandler(ILockedQueueStorage lockedQueueStorage,
                                           string              id,
                                           ILogger             logger,
                                           CancellationToken   cancellationToken = default)
  {
    lockedQueueStorage_ = lockedQueueStorage;
    id_                 = id;
    cancellationToken_  = cancellationToken;
    heart_ = new(async ct => await lockedQueueStorage_.RenewDeadlineAsync(id_,
                                                                          ct),
                 lockedQueueStorage_.LockRefreshPeriodicity,
                 cancellationToken_);
    heart_.Start();
    logger_ = logger;
  }

  public CancellationToken MessageLockLost => heart_.HeartStopped;

  /// <inheritdoc />
  public async ValueTask DisposeAsync()
  {
    using var _ = logger_.LogFunction(id_,
                                      functionName: $"{nameof(LockedQueueMessageDeadlineHandler)}.{nameof(DisposeAsync)}");
    if (!heart_.HeartStopped.IsCancellationRequested)
      await heart_.Stop();
    GC.SuppressFinalize(this);
  }
}