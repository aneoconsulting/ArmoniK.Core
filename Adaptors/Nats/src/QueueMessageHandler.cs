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
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;

using Microsoft.Extensions.Logging;

using NATS.Client.JetStream;

namespace ArmoniK.Core.Adapters.Nats;

internal class QueueMessageHandler : IQueueMessageHandler
{
  private readonly Heart             autoExtendAckDeadline_;
  private readonly INatsJSContext    js_;
  private readonly ILogger           logger_;
  private readonly NatsJSMsg<string> message_;
  private          StackTrace?       stackTrace_;


  public QueueMessageHandler(NatsJSMsg<string> message,
                             INatsJSContext    js,
                             int               AckWait,
                             int               ackExtendDeadlineStep,
                             ILogger           logger,
                             CancellationToken cancellationToken)
  {
    js_               = js;
    message_          = message;
    MessageId         = message.Data!;
    TaskId            = message.Data!;
    ReceptionDateTime = DateTime.UtcNow;
    logger_           = logger;
    stackTrace_       = new StackTrace(true);
    autoExtendAckDeadline_ = new Heart(ModifyAckDeadline,
                                       TimeSpan.FromSeconds(ackExtendDeadlineStep),
                                       CancellationToken);
    autoExtendAckDeadline_.Start();
  }

  public CancellationToken  CancellationToken { get; set; }
  public string             MessageId         { get; }
  public string             TaskId            { get; }
  public QueueMessageStatus Status            { get; set; }
  public DateTime           ReceptionDateTime { get; init; }

  public async ValueTask DisposeAsync()
  {
    stackTrace_ = null;

    await autoExtendAckDeadline_.Stop()
                                .ConfigureAwait(false);

    switch (Status)
    {
      case QueueMessageStatus.Waiting:
      case QueueMessageStatus.Failed:
      case QueueMessageStatus.Running:
      case QueueMessageStatus.Postponed:
        await message_.AckProgressAsync()
                      .ConfigureAwait(false);
        break;
      case QueueMessageStatus.Cancelled:
      case QueueMessageStatus.Processed:
      case QueueMessageStatus.Poisonous:
        await message_.AckAsync()
                      .ConfigureAwait(false);
        break;
      default:
        throw new ArgumentOutOfRangeException();
    }

    GC.SuppressFinalize(this);
  }

  /// <summary>
  ///   Extends the acknowledgement deadline for the current message.
  /// </summary>
  /// <remarks>
  ///   Calls <see cref="AckProgressAsync" /> to indicate that processing is still in progress.
  /// </remarks>
  public async Task ModifyAckDeadline(CancellationToken cancellationToken)
    => await message_.AckProgressAsync()
                     .ConfigureAwait(false);

  /// <summary>
  ///   Finalizer for <see cref="QueueMessageHandler", /> Acts as a safety net to log undisposed handlers and force cleanup,
  ///   though explicit disposal is strongly recommended.
  /// </summary>
  /// <remarks>
  ///   Ensures that message handlers are properly disposed even if the user forgets to call <c>Dispose</c> or
  ///   <c>DisposeAsync</c>.
  ///   If the handler was not disposed:
  ///   - Logs an error including the message ID, task ID, and the captured creation stack trace.
  ///   - Forces a synchronous call to <see cref="DisposeAsync" /> to release resources.
  ///   This acts as a safety net to prevent resource leaks, but relying on finalizers is discouraged.
  ///   Users should always dispose <see cref="QueueMessageHandler" /> explicitly to avoid non-deterministic cleanup timing.
  /// </remarks>
  ~QueueMessageHandler()
  {
    if (stackTrace_ is null)
    {
      return;
    }

    logger_.LogError("QueueMessageHandler for Message {MessageId} and Task {TaskId} was not disposed: Created {MessageCreationStackTrace}",
                     MessageId,
                     TaskId,
                     stackTrace_);
    DisposeAsync()
      .AsTask()
      .GetAwaiter()
      .GetResult();
  }
}
