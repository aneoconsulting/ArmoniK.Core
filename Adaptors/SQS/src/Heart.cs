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

namespace ArmoniK.Core.Adapters.SQS;

/// <summary>
///   Represents a heartbeat mechanism that executes a specified function at regular intervals.
/// </summary>
public class Heart
{
  private readonly TimeSpan          beatPeriod_;
  private readonly CancellationToken cancellationToken_;

  private readonly Func<CancellationToken, Task> pulse_;

  private Task? runningTask_;

  private CancellationTokenSource? stoppedHeartCts_;

  /// <summary>
  /// </summary>
  /// <param name="pulse">
  ///   The function to execute at each beat
  ///   It returns a predicate indicating if the heart must continue beating
  /// </param>
  /// <param name="beatPeriod">Defines the timespan between two heartbeats</param>
  /// <param name="cancellationToken"></param>
  /// <returns></returns>
  public Heart(Func<CancellationToken, Task> pulse,
               TimeSpan                      beatPeriod,
               CancellationToken             cancellationToken = default)
  {
    cancellationToken_ = cancellationToken;
    pulse_             = pulse;
    beatPeriod_        = beatPeriod;
  }

  /// <summary>
  ///   Stops the heart
  /// </summary>
  /// <returns>A task finishing with the last heartbeat</returns>
  public async Task Stop()
  {
    await (stoppedHeartCts_?.CancelAsync() ?? Task.CompletedTask).ConfigureAwait(false);
    try
    {
      if (runningTask_ != null)
      {
        await runningTask_;
      }
    }
    catch (OperationCanceledException)
    {
    }
    catch (AggregateException ae)
    {
      ae.Handle(exception => exception is not OperationCanceledException);
    }
    finally
    {
      stoppedHeartCts_?.Dispose();
      stoppedHeartCts_ = null;
    }
  }

  /// <summary>
  ///   Start the heart. If the heart is beating, it has no effect.
  /// </summary>
  /// <param name="firstBeatDelay">
  ///   Delay before the first beat; the following beats are separated by the beat period.
  ///   A zero or negative value triggers the first beat immediately.
  /// </param>
  public void Start(TimeSpan firstBeatDelay)
  {
    if (stoppedHeartCts_ is not null) // already running with infinite loop
    {
      return;
    }

    stoppedHeartCts_ = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken_);

    runningTask_ = Task<Task>.Factory.StartNew(async () =>
                                               {
                                                 var delay = firstBeatDelay;
                                                 while (!stoppedHeartCts_.IsCancellationRequested)
                                                 {
                                                   if (delay > TimeSpan.Zero)
                                                   {
                                                     await Task.Delay(delay,
                                                                      stoppedHeartCts_.Token)
                                                               .ConfigureAwait(false);
                                                   }

                                                   await pulse_(cancellationToken_)
                                                     .ConfigureAwait(false);

                                                   delay = beatPeriod_;
                                                 }
                                               },
                                               CancellationToken.None,
                                               TaskCreationOptions.LongRunning,
                                               TaskScheduler.Current)
                             .Unwrap();
  }
}
