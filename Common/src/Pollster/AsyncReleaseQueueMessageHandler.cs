// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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

using ArmoniK.Core.Base;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Pollster;

/// <summary>
///   Wrapper around a <see cref="IQueueMessageHandler" /> provided by a queue adapter
///   whose disposal hands over the release of the message to a <see cref="QueueMessageReleaser" />.
/// </summary>
/// <remarks>
///   <para>
///     Disposing the wrapper returns as soon as the message has been written into the release queue,
///     waiting for a free place if the queue is full:
///     the acknowledgment (or negative acknowledgment) is sent without blocking the caller.
///     If the release queue is closed, the message is released synchronously, as without the wrapper.
///   </para>
///   <para>
///     <see cref="Status" /> is frozen when the wrapper is disposed: the adapter releases the message with the status
///     it had at that time, as if it had been released synchronously. Later changes are ignored and logged.
///   </para>
/// </remarks>
public sealed class AsyncReleaseQueueMessageHandler : IQueueMessageHandler
{
  private readonly IQueueMessageHandler inner_;
  private readonly object               lock_ = new();
  private readonly QueueMessageReleaser releaser_;
  private          bool                 disposed_;
  private          QueueMessageStatus   status_;

  /// <summary>
  ///   Initializes a new instance of the <see cref="AsyncReleaseQueueMessageHandler" /> class.
  /// </summary>
  /// <param name="inner">The message handler provided by the queue adapter</param>
  /// <param name="releaser">The service releasing the messages in background</param>
  public AsyncReleaseQueueMessageHandler(IQueueMessageHandler inner,
                                         QueueMessageReleaser releaser)
  {
    inner_            = inner;
    releaser_         = releaser;
    status_           = inner.Status;
    ReceptionDateTime = inner.ReceptionDateTime;
  }

  /// <inheritdoc />
  /// <remarks>Required by the interface but not used: it is not forwarded to the wrapped message.</remarks>
  [Obsolete("ArmoniK now manages loss of link with the queue")]
  public CancellationToken CancellationToken { get; set; }

  /// <inheritdoc />
  public string MessageId
    => inner_.MessageId;

  /// <inheritdoc />
  public string TaskId
    => inner_.TaskId;

  /// <inheritdoc />
  /// <remarks>Changes made after the disposal of the wrapper are ignored.</remarks>
  public QueueMessageStatus Status
  {
    get
    {
      lock (lock_)
      {
        return status_;
      }
    }
    set
    {
      lock (lock_)
      {
        if (!disposed_)
        {
          status_ = value;
          return;
        }
      }

      releaser_.Logger.LogWarning("Status of message {MessageHandler} for task {TaskId} set to {Status} after its release; ignored, the message is released with status {ReleasedStatus}",
                                  MessageId,
                                  TaskId,
                                  value,
                                  Status);
    }
  }

  /// <inheritdoc />
  public DateTime ReceptionDateTime { get; init; }

  /// <inheritdoc />
  public async ValueTask DisposeAsync()
  {
    lock (lock_)
    {
      if (disposed_)
      {
        return;
      }

      disposed_ = true;

      // Freeze the status: the adapter reads it when it releases the message
      inner_.Status = status_;
    }

    await releaser_.ReleaseAsync(inner_)
                   .ConfigureAwait(false);
  }
}
