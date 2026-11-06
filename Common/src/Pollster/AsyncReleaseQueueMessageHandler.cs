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

namespace ArmoniK.Core.Common.Pollster;

/// <summary>
///   Wrapper around a <see cref="IQueueMessageHandler" /> provided by a queue adapter
///   whose disposal hands over the release of the message to a <see cref="QueueMessageReleaser" />.
/// </summary>
/// <remarks>
///   <para>
///     Disposing the wrapper returns as soon as the message has been handed over:
///     the acknowledgment (or negative acknowledgment) is sent in background.
///     If the hand over fails (release queue full or closed), the message is released synchronously.
///   </para>
///   <para>
///     All the other members are forwarded to the wrapped message, in particular <see cref="Status" />
///     which is read by the adapter upon release.
///   </para>
/// </remarks>
public sealed class AsyncReleaseQueueMessageHandler : IQueueMessageHandler
{
  private readonly IQueueMessageHandler inner_;
  private readonly QueueMessageReleaser releaser_;
  private          int                  disposed_;

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
    ReceptionDateTime = inner.ReceptionDateTime;
  }

  /// <inheritdoc />
  [Obsolete("ArmoniK now manages loss of link with the queue")]
  public CancellationToken CancellationToken
  {
    get => inner_.CancellationToken;
    set => inner_.CancellationToken = value;
  }

  /// <inheritdoc />
  public string MessageId
    => inner_.MessageId;

  /// <inheritdoc />
  public string TaskId
    => inner_.TaskId;

  /// <inheritdoc />
  public QueueMessageStatus Status
  {
    get => inner_.Status;
    set => inner_.Status = value;
  }

  /// <inheritdoc />
  public DateTime ReceptionDateTime { get; init; }

  /// <inheritdoc />
  public async ValueTask DisposeAsync()
  {
    if (Interlocked.Exchange(ref disposed_,
                             1) != 0)
    {
      return;
    }

    if (releaser_.TryEnqueue(inner_))
    {
      return;
    }

    await inner_.DisposeAsync()
                .ConfigureAwait(false);
  }
}
