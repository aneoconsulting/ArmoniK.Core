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

using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Base;
using ArmoniK.Core.Common.Meter;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;
using ArmoniK.Utils;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Pollster;

/// <summary>
///   Hosted service that releases (acknowledges or negatively acknowledges) queue messages
///   outside of the critical path of the pollster pipeline.
/// </summary>
/// <remarks>
///   <para>
///     Messages are written by <see cref="ReleaseAsync" /> into a bounded queue of
///     <see cref="Injection.Options.Pollster.MessageReleaseQueueCapacity" /> messages,
///     which is drained by at most <see cref="Injection.Options.Pollster.MessageReleaseParallelism" /> concurrent
///     releases. When the queue is full, <see cref="ReleaseAsync" /> waits for a free place.
///     Once the queue is closed, <see cref="ReleaseAsync" /> releases the message directly.
///   </para>
///   <para>
///     The queue is closed by <see cref="CloseWriter" />, called by the last stage of the pipeline
///     (<see cref="PostProcessor" />) when it stops, or by <see cref="ExceptionManager.LateCancellationToken" />.
///     The messages already in the queue are released before the service stops.
///   </para>
///   <para>
///     As the other stages of the pipeline, the service is registered in the <see cref="ExceptionManager" />:
///     it unregisters once its queue is closed and drained.
///   </para>
/// </remarks>
public sealed class QueueMessageReleaser : BackgroundService
{
  private readonly Channel<IQueueMessageHandler> channel_;
  private readonly ExceptionManager              exceptionManager_;
  private readonly Counter<int>                  inlineReleaseCounter_;
  private readonly int                           parallelism_;
  private readonly Histogram<double>             releaseDuration_;

  /// <summary>
  ///   Initializes a new instance of the <see cref="QueueMessageReleaser" /> class.
  /// </summary>
  /// <param name="pollsterOptions">Pollster options containing the capacity of the queue and the release parallelism.</param>
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
    Logger            = logger;
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
                                            "Number of queue messages waiting to be released in background",
                                            meterHolder.Tags);

    inlineReleaseCounter_ = meterHolder.Meter.CreateCounter<int>("MessageReleaseInline",
                                                                 "Messages",
                                                                 "Number of queue messages released by the caller because the release queue was closed",
                                                                 meterHolder.Tags);

    releaseDuration_ = meterHolder.Meter.CreateHistogram<double>("MessageReleaseDuration",
                                                                 "Milliseconds",
                                                                 "Duration of the release of a queue message by the queue adapter in background",
                                                                 meterHolder.Tags);
  }

  /// <summary>
  ///   Logger used for the releases performed in background
  /// </summary>
  internal ILogger Logger { get; }

  /// <summary>
  ///   Release <paramref name="message" />: write it into the queue to be released in background,
  ///   waiting for a free place if the queue is full, or release it directly if the queue is closed.
  /// </summary>
  /// <param name="message">The message to release, with its final status</param>
  /// <returns>
  ///   A task completing when the message has been written into the queue,
  ///   or when it has been released if the queue is closed
  /// </returns>
  public async ValueTask ReleaseAsync(IQueueMessageHandler message)
  {
    try
    {
      // Not cancellable: closing the queue wakes up the writers waiting for a free place
      await channel_.Writer.WriteAsync(message,
                                       CancellationToken.None)
                    .ConfigureAwait(false);
      return;
    }
    catch (ChannelClosedException)
    {
      // The message has not been written into the queue: the caller still owns it
    }

    inlineReleaseCounter_.Add(1);
    await message.DisposeAsync()
                 .ConfigureAwait(false);
  }

  /// <summary>
  ///   Close the queue: the messages released afterward are released directly by the caller.
  ///   The messages already in the queue are still released in background.
  /// </summary>
  public void CloseWriter()
    => channel_.Writer.TryComplete();

  /// <inheritdoc />
  protected override async Task ExecuteAsync(CancellationToken stoppingToken)
  {
    exceptionManager_.Register();

    // The queue is normally closed by the post processor when the pipeline stops.
    // The late cancellation token closes it if the pipeline did not stop in time.
    await using var closeOnLate = exceptionManager_.LateCancellationToken.Register(CloseWriter);

    Logger.LogDebug("Start queue message releaser with a parallelism of {Parallelism}",
                    parallelism_);

    // Neither the reading nor the releases are cancelled: the enumeration ends once the queue is closed and empty,
    // so that every message written into the queue is released. Releases are unordered so that a slow release
    // does not hold back the following ones.
    await channel_.Reader.ReadAllAsync(CancellationToken.None)
                  .ParallelForEach(new ParallelTaskOptions(true,
                                                           parallelism_,
                                                           CancellationToken.None),
                                   ReleaseInBackgroundAsync)
                  .ConfigureAwait(false);

    exceptionManager_.UnregisterAndStop(Logger,
                                        "End of queue message releaser; no more messages will be released in background");
  }

  private async Task ReleaseInBackgroundAsync(IQueueMessageHandler message)
  {
    using var _ = Logger.BeginPropertyScope(("messageHandler", message.MessageId),
                                            ("taskId", message.TaskId));
    var start = Stopwatch.GetTimestamp();

    // Errors are logged and never thrown: a failing release must not stop the enumeration
    await message.DisposeIgnoreErrorAsync(Logger)
                 .ConfigureAwait(false);

    releaseDuration_.Record(Stopwatch.GetElapsedTime(start)
                                     .TotalMilliseconds);
  }
}
