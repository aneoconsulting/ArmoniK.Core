// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
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

namespace ArmoniK.Core.Common.Utils;

public class Heart
{
  private readonly TimeSpan          beatPeriod_;
  private readonly CancellationToken cancellationToken_;

  private readonly Func<CancellationToken, Task<bool>> pulse_;

  private CancellationTokenSource? combinedSource_;

  private Task? runningTask_;

  private CancellationTokenSource stoppedHeartCts_ = new();

  /// <summary>
  /// </summary>
  /// <typeparam name="T"></typeparam>
  /// <param name="pulse">
  ///   The function to execute at each beat
  ///   It returns a predicate indicating if the heart must continue beating
  /// </param>
  /// <param name="beatPeriod">Defines the timespan between two heartbeats</param>
  /// <param name="cancellationToken"></param>
  /// <returns></returns>
  public Heart(Func<CancellationToken, Task<bool>> pulse,
               TimeSpan                            beatPeriod,
               CancellationToken                   cancellationToken = default)
  {
    cancellationToken_ = cancellationToken;
    pulse_             = pulse;
    beatPeriod_        = beatPeriod;
    stoppedHeartCts_.Cancel();
  }

  /// <summary>
  /// </summary>
  /// <typeparam name="T"></typeparam>
  /// <param name="pulse">
  ///   The function to execute at each beat
  ///   It returns a predicate indicating if the heart must continue beating
  /// </param>
  /// <param name="beatPeriod">Defines the timespan between two heartbeats</param>
  /// <param name="cancellationToken"></param>
  /// <returns></returns>
  public Heart(Func<bool>        pulse,
               TimeSpan          beatPeriod,
               CancellationToken cancellationToken = default) :
    this(token => Task.FromResult(pulse()),
         beatPeriod,
         cancellationToken)
  {
  }

  /// <summary>
  ///   Triggered when the heart stops
  /// </summary>
  public CancellationToken HeartStopped => stoppedHeartCts_.Token;

  /// <summary>
  ///   Stops the heart
  /// </summary>
  /// <returns>A task finishing with the last heartbeat</returns>
  public async Task Stop()
  {
    stoppedHeartCts_.Cancel();
    try
    {
      await runningTask_;
    }
    catch (TaskCanceledException)
    {
    }
    catch (AggregateException ae)
    {
      ae.Handle(exception => exception is not TaskCanceledException);
    }
  }

  /// <summary>
  ///   Start the heart. If the heart is beating, it has no effect.
  /// </summary>
  public void Start()
  {
    if (!stoppedHeartCts_.IsCancellationRequested) // already running with infinite loop
      return;

    stoppedHeartCts_ = new();
    combinedSource_ = CancellationTokenSource.CreateLinkedTokenSource(stoppedHeartCts_.Token,
                                                                      cancellationToken_);

    runningTask_ = Task<Task>.Factory
                             .StartNew(async () =>
                                       {
                                         await Task.Delay(beatPeriod_,
                                                          combinedSource_.Token);
                                         while (!stoppedHeartCts_.IsCancellationRequested)
                                           await FullCycle();
                                       },
                                       cancellationToken_,
                                       TaskCreationOptions.LongRunning,
                                       TaskScheduler.Current)
                             .Unwrap();
  }

  private async Task FullCycle()
  {
    var delayTask = Task.Delay(beatPeriod_,
                               combinedSource_.Token);
    if (!await pulse_(cancellationToken_))
    {
      stoppedHeartCts_.Cancel();
      return;
    }

    await delayTask;
  }
}