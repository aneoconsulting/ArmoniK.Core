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
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Base;

public class QueueMessageHandler : IQueueMessageHandler
{
  private readonly Func<QueueMessageStatus, Task> disposeFunc_;
  private readonly ILogger                        logger_;

  public QueueMessageHandler(string                         messageId,
                             string                         taskId,
                             Func<QueueMessageStatus, Task> disposeFunc,
                             ILogger                        logger,
                             CancellationToken              cancellationToken)
  {
    disposeFunc_      = disposeFunc;
    logger_           = logger;
    MessageId         = messageId;
    TaskId            = taskId;
    CancellationToken = cancellationToken;
    ReceptionDateTime = DateTime.UtcNow;
  }

  public string MessageId { get; init; }
  public string TaskId    { get; init; }

  /// <inheritdoc />
  public QueueMessageStatus Status { get; set; }

  /// <inheritdoc />
  public DateTime ReceptionDateTime { get; init; }

  /// <inheritdoc />
  public CancellationToken CancellationToken { get; set; }

  /// <inheritdoc />
  public async ValueTask DisposeAsync()
  {
    await disposeFunc_(Status)
      .ConfigureAwait(false);
    GC.SuppressFinalize(this);
  }
}
