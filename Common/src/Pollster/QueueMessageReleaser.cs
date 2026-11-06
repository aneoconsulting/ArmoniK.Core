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
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Common.Meter;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Pollster;

/// <summary>
///   Background service that releases (acknowledges or negatively acknowledges) queue messages
///   outside of the critical path of the pollster pipeline.
/// </summary>
/// <remarks>
///   <para>
///     Messages are handed over through <see cref="TryEnqueue" /> by <see cref="AsyncReleaseQueueMessageHandler" />.
///     They are released concurrently, with at most <see cref="Injection.Options.Pollster.MessageReleaseParallelism" />
///     releases in flight.
///   </para>
///   <para>
///     When the internal queue is full or closed, <see cref="TryEnqueue" /> returns <c>false</c>
///     and the caller keeps the ownership of the message: it has to release it itself.
///   </para>
///   <para>
///     The internal queue is closed when <see cref="ExceptionManager.LateCancellationToken" /> is triggered,
///     that is once all the stages of the pipeline are stopped or after the grace delay.
///     The messages already enqueued are then released before the service stops.
///   </para>
/// </remarks>
public sealed class QueueMessageReleaser : BackgroundService
{
  private readonly Channel<IQueueMessageHandler> channel_;
  private readonly ExceptionManager              exceptionManager_;
  private readonly Counter<int>                  inlineReleaseCounter_;
  private readonly ILogger<QueueMessageReleaser> logger_;
  private readonly int                           parallelism_;
  private readonly Histogram<double>             releaseDuration_;

  /// <summary>
  ///   Initializes a new instance of the <see cref="QueueMessageReleaser" /> class.
  /// </summary>
  /// <param name="pollsterOptions">Pollster options containing the release queue capacity and parallelism.</param>
  /// <param name="exceptionManager">The manager handling exceptions and cancellation.</param>
  /// <param name="meterHolder">Holder for metrics collection.</param>
  /// <param name="logger">The logger for this class.</param>
  /// <exception cref="ArgumentOutOfRangeException">Capacity or parallelism is less than 1.</exception>
  public QueueMessageReleaser(Injection.Options.Pollster    pollsterOptions,
                              ExceptionManager              exceptionManager,
                              MeterHolder                   meterHolder,
                              ILogger<QueueMessageReleaser> logger)
  {
    if (pollsterOptions.MessageReleaseQueueCapacity < 1)
    {
      throw new ArgumentOutOfRangeException(nameof(pollsterOptions),
                                            $"The minimum value for {nameof(Injection.Options.Pollster.MessageReleaseQueueCapacity)} is 1.");
    }

    if (pollsterOptions.MessageReleaseParallelism < 1)
    {
      throw new ArgumentOutOfRangeException(nameof(pollsterOptions),
                                            $"The minimum value for {nameof(Injection.Options.Pollster.MessageReleaseParallelism)} is 1.");
    }

    exceptionManager_ = exceptionManager;
    logger_           = logger;
    parallelism_      = pollsterOptions.MessageReleaseParallelism;
    channel_ = Channel.CreateBounded<IQueueMessageHandler>(new BoundedChannelOptions(pollsterOptions.MessageReleaseQueueCapacity)
                                                           {
                                                             FullMode     = BoundedChannelFullMode.Wait,
                                                             SingleReader = true,
                                                             SingleWriter = false,
                                                           });

    meterHolder.Meter.CreateObservableGauge("MessageReleaseQueueSize",
                                            () => channel_.Reader.Count,
                                            "Messages",
                                            "Number of queue messages waiting to be released",
                                            meterHolder.Tags);

    inlineReleaseCounter_ = meterHolder.Meter.CreateCounter<int>("MessageReleaseInline",
                                                                 "Messages",
                                                                 "Number of queue messages released by the caller because the release queue was full or closed",
                                                                 meterHolder.Tags);

    releaseDuration_ = meterHolder.Meter.CreateHistogram<double>("MessageReleaseDuration",
                                                                 "Milliseconds",
                                                                 "Duration of the release of a queue message by the queue adapter",
                                                                 meterHolder.Tags);
  }

  /// <summary>
  ///   Try to hand over the release of <paramref name="message" /> to the background service.
  /// </summary>
  /// <param name="message">The message to release</param>
  /// <returns>
  ///   <c>true</c> if the background service took the ownership of the message;
  ///   <c>false</c> if the queue is full or closed, in which case the caller remains responsible for releasing it.
  /// </returns>
  public bool TryEnqueue(IQueueMessageHandler message)
  {
    if (channel_.Writer.TryWrite(message))
    {
      return true;
    }

    inlineReleaseCounter_.Add(1);
    return false;
  }

  /// <inheritdoc />
  protected override async Task ExecuteAsync(CancellationToken stoppingToken)
  {
    // Close the queue once the pipeline is stopped (or the grace delay expired).
    // Messages released afterward are released by their caller.
    await using var closeOnLate = exceptionManager_.LateCancellationToken.Register(() => channel_.Writer.TryComplete());

    logger_.LogDebug("Start queue message releaser with a parallelism of {Parallelism}",
                     parallelism_);

    // The reading is not cancelled: the loop ends when the queue is closed and empty,
    // so that every enqueued message is released.
    await Parallel.ForEachAsync(channel_.Reader.ReadAllAsync(CancellationToken.None),
                                new ParallelOptions
                                {
                                  MaxDegreeOfParallelism = parallelism_,
                                },
                                async (message, _) =>
                                {
                                  var start = Stopwatch.GetTimestamp();
                                  await message.DisposeIgnoreErrorAsync(logger_)
                                               .ConfigureAwait(false);
                                  releaseDuration_.Record(Stopwatch.GetElapsedTime(start)
                                                                   .TotalMilliseconds);
                                })
                  .ConfigureAwait(false);

    logger_.LogDebug("End of queue message releaser; no more messages will be released in background");
  }
}
