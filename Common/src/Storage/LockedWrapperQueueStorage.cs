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
// but WITHOUT ANY WARRANTY, without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Storage;

[PublicAPI]
public class LockedWrapperQueueStorage : IQueueStorage
{
  private readonly ILockedQueueStorage                lockedQueueStorage_;
  private readonly ILogger<LockedWrapperQueueStorage> logger_;


  private bool isInitialized_;

  public LockedWrapperQueueStorage(ILockedQueueStorage                lockedQueueStorage,
                                   ILogger<LockedWrapperQueueStorage> logger)
  {
    lockedQueueStorage_ = lockedQueueStorage;
    logger_             = logger;
  }

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      await lockedQueueStorage_.Init(cancellationToken)
                               .ConfigureAwait(false);
    }

    isInitialized_ = true;
  }

  /// <inheritdoc />
  public ValueTask<bool> Check(HealthCheckTag tag)
    => ValueTask.FromResult(isInitialized_);

  /// <inheritdoc />
  public int MaxPriority
    => lockedQueueStorage_.MaxPriority;

  /// <inheritdoc />
  public async IAsyncEnumerable<IQueueMessageHandler> PullAsync(int                                        nbMessages,
                                                                [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    using var logFunction = logger_.LogFunction($"for {nbMessages} messages");

    await foreach (var qm in lockedQueueStorage_.PullAsync(nbMessages,
                                                           cancellationToken)
                                                .WithCancellation(cancellationToken)
                                                .ConfigureAwait(false))
    {
      using var logScope = logger_.BeginPropertyScope(("messageId", qm.MessageId),
                                                      ("taskId", qm.TaskId));

      logger_.LogInformation("Setting messageHandler lock");
      var deadlineHandler = lockedQueueStorage_.GetDeadlineHandler(qm.MessageId,
                                                                   logger_,
                                                                   cancellationToken);

      logger_.LogInformation("Queue messageHandler ready to forward");
      yield return new LockedWrapperQueueMessageHandler(qm,
                                                        deadlineHandler,
                                                        cancellationToken);
    }
  }

  /// <inheritdoc />
  public Task EnqueueMessagesAsync(IEnumerable<string> messages,
                                   int                 priority          = 1,
                                   CancellationToken   cancellationToken = default)
    => lockedQueueStorage_.EnqueueMessagesAsync(messages,
                                                priority,
                                                cancellationToken);

  /// <inheritdoc />
  public Task MessageProcessedAsync(string            id,
                                    CancellationToken cancellationToken = default)
    => lockedQueueStorage_.MessageProcessedAsync(id,
                                                 cancellationToken);

  /// <inheritdoc />
  public Task MessageRejectedAsync(string            id,
                                   CancellationToken cancellationToken = default)
    => lockedQueueStorage_.MessageRejectedAsync(id,
                                                cancellationToken);

  /// <inheritdoc />
  public Task RequeueMessageAsync(string            id,
                                  CancellationToken cancellationToken = default)
    => lockedQueueStorage_.RequeueMessageAsync(id,
                                               cancellationToken);

  /// <inheritdoc />
  public Task ReleaseMessageAsync(string            id,
                                  CancellationToken cancellationToken = default)
    => lockedQueueStorage_.ReleaseMessageAsync(id,
                                               cancellationToken);
}
