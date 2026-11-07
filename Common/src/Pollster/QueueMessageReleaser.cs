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
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Base;
using ArmoniK.Core.Common.Meter;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Pollster;

/// <summary>
///   Service that releases (acknowledges or negatively acknowledges) queue messages
///   outside of the critical path of the pollster pipeline.
/// </summary>
/// <remarks>
///   <para>
///     There is no waiting queue: a message is accepted through <see cref="TryStart" /> only if one of the
///     <see cref="Injection.Options.Pollster.MessageReleaseParallelism" /> release slots is free,
///     in which case its release starts immediately in background.
///     Otherwise, <see cref="TryStart" /> returns <c>false</c> without waiting
///     and the caller keeps the ownership of the message: it has to release it itself.
///   </para>
///   <para>
///     The service is closed when <see cref="ExceptionManager.LateCancellationToken" /> is triggered,
///     that is once all the stages of the pipeline are stopped or after the grace delay.
///     No release is accepted afterward, and the service waits for the releases in flight before stopping.
///   </para>
/// </remarks>
public sealed class QueueMessageReleaser : BackgroundService
{
  private readonly ExceptionManager  exceptionManager_;
  private readonly Counter<int>      inlineReleaseCounter_;
  private readonly object            lock_ = new();
  private readonly int               parallelism_;
  private readonly Histogram<double> releaseDuration_;

  [SuppressMessage("Usage",
                   "CA2213:Disposable fields should be disposed",
                   Justification = "A release in flight must be able to give its slot back after the host is disposed; " +
                                   "SemaphoreSlim holds no unmanaged resource as long as AvailableWaitHandle is not used")]
  private readonly SemaphoreSlim slots_;

  private bool closed_;

  /// <summary>
  ///   Initializes a new instance of the <see cref="QueueMessageReleaser" /> class.
  /// </summary>
  /// <param name="pollsterOptions">Pollster options containing the release parallelism.</param>
  /// <param name="exceptionManager">The manager handling exceptions and cancellation.</param>
  /// <param name="meterHolder">Holder for metrics collection.</param>
  /// <param name="logger">The logger for this class.</param>
  /// <exception cref="ArgumentOutOfRangeException">Parallelism is less than 1.</exception>
  public QueueMessageReleaser(Injection.Options.Pollster    pollsterOptions,
                              ExceptionManager              exceptionManager,
                              MeterHolder                   meterHolder,
                              ILogger<QueueMessageReleaser> logger)
  {
    if (pollsterOptions.MessageReleaseParallelism < 1)
    {
      throw new ArgumentOutOfRangeException(nameof(pollsterOptions),
                                            $"The minimum value for {nameof(Injection.Options.Pollster.MessageReleaseParallelism)} is 1.");
    }

    exceptionManager_ = exceptionManager;
    Logger            = logger;
    parallelism_      = pollsterOptions.MessageReleaseParallelism;
    slots_ = new SemaphoreSlim(parallelism_,
                               parallelism_);

    meterHolder.Meter.CreateObservableGauge("MessageReleaseInFlight",
                                            () => parallelism_ - slots_.CurrentCount,
                                            "Messages",
                                            "Number of queue messages being released in background",
                                            meterHolder.Tags);

    inlineReleaseCounter_ = meterHolder.Meter.CreateCounter<int>("MessageReleaseInline",
                                                                 "Messages",
                                                                 "Number of queue messages released by the caller because no release slot was free or the releaser was closed",
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
  ///   Try to start the release of <paramref name="message" /> in background. This method never waits.
  /// </summary>
  /// <param name="message">The message to release, with its final status</param>
  /// <returns>
  ///   <c>true</c> if the release has started in background: the service took the ownership of the message;
  ///   <c>false</c> if no release slot is free or the service is closed,
  ///   in which case the caller remains responsible for releasing the message.
  /// </returns>
  public bool TryStart(IQueueMessageHandler message)
  {
    // The lock makes the check of `closed_` and the acquisition of a slot atomic with respect to `Close`:
    // once `Close` returns, no slot can be acquired anymore, so waiting for all the slots waits for every release.
    lock (lock_)
    {
      if (closed_ || !slots_.Wait(0))
      {
        inlineReleaseCounter_.Add(1);
        return false;
      }
    }

    // Task.Run flows the execution context: the logging scopes of the caller (task id, session id...)
    // are kept for the logs emitted during the release.
    _ = Task.Run(() => ReleaseAsync(message));
    return true;
  }

  private async Task ReleaseAsync(IQueueMessageHandler message)
  {
    try
    {
      using var _ = Logger.BeginPropertyScope(("messageHandler", message.MessageId),
                                              ("taskId", message.TaskId));
      var start = Stopwatch.GetTimestamp();
      await message.DisposeIgnoreErrorAsync(Logger)
                   .ConfigureAwait(false);
      releaseDuration_.Record(Stopwatch.GetElapsedTime(start)
                                       .TotalMilliseconds);
    }
    finally
    {
      slots_.Release();
    }
  }

  /// <summary>
  ///   Stop accepting new releases. Releases already started are not affected.
  /// </summary>
  private void Close()
  {
    lock (lock_)
    {
      closed_ = true;
    }
  }

  /// <inheritdoc />
  protected override async Task ExecuteAsync(CancellationToken stoppingToken)
  {
    Logger.LogDebug("Start queue message releaser with a parallelism of {Parallelism}",
                    parallelism_);

    // Releases are accepted until the pipeline is stopped (or the grace delay expired).
    try
    {
      await Task.Delay(Timeout.InfiniteTimeSpan,
                       exceptionManager_.LateCancellationToken)
                .ConfigureAwait(false);
    }
    catch (OperationCanceledException)
    {
    }

    Close();

    // No slot can be acquired anymore: acquiring all of them waits for the end of every release in flight.
    // This wait is not cancelled; it is bounded by the shutdown timeout of the host.
    for (var i = 0; i < parallelism_; i++)
    {
      await slots_.WaitAsync(CancellationToken.None)
                  .ConfigureAwait(false);
    }

    Logger.LogDebug("End of queue message releaser; no more messages will be released in background");
  }
}
