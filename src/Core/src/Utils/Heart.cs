using System;
using System.Threading;
using System.Threading.Tasks;

namespace ArmoniK.Core.Utils
{
  public class Heart
  {
    private readonly CancellationToken cancellationToken_;

    private CancellationTokenSource stoppedHeartCts_ = new();

    private CancellationTokenSource combinedSource_;

    private Task runningTask_;

    private readonly Func<CancellationToken, Task<bool>> pulse_;

    private readonly TimeSpan beatPeriod;

    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="pulse">The function to execute at each beat
    /// It returns a predicate indicating if the heart must continue beating</param>
    /// <param name="beatPeriod">Defines the timespan between two heartbeats</param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Heart(Func<CancellationToken, Task<bool>> pulse,
                 TimeSpan                            beatPeriod,
                 CancellationToken                   cancellationToken = default)
    {
      cancellationToken_ = cancellationToken;
      pulse_             = pulse;
      this.beatPeriod    = beatPeriod;
      stoppedHeartCts_.Cancel();
    }

    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="pulse">The function to execute at each beat
    /// It returns a predicate indicating if the heart must continue beating</param>
    /// <param name="beatPeriod">Defines the timespan between two heartbeats</param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public Heart(Func<bool>        pulse,
                 TimeSpan          beatPeriod,
                 CancellationToken cancellationToken = default) :
      this(token => Task.FromResult(pulse()), beatPeriod, cancellationToken)
    {
    }

    /// <summary>
    /// Stops the heart
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
    /// Start the heart. If the heart is beating, it has no effect.
    /// </summary>
    public void Start()
    {
      if (!stoppedHeartCts_.IsCancellationRequested) // already running with infinite loop
      {
        return;
      }

      stoppedHeartCts_ = new CancellationTokenSource();
      combinedSource_ = CancellationTokenSource.CreateLinkedTokenSource(stoppedHeartCts_.Token,
                                                                        cancellationToken_);

      runningTask_ = Task<Task>.Factory
                         .StartNew(async () =>
                                   {
                                     await Task.Delay(beatPeriod,
                                                      combinedSource_.Token);
                                     while (!stoppedHeartCts_.IsCancellationRequested)
                                     {
                                       await FullCycle();
                                     }
                                   },
                                   cancellationToken_,
                                   TaskCreationOptions.LongRunning,
                                   TaskScheduler.Current)
                         .Unwrap();
    }

    /// <summary>
    /// Triggered when the heart stops
    /// </summary>
    public CancellationToken HeartStopped => stoppedHeartCts_.Token;

    private async Task FullCycle()
    {
      var delayTask = Task.Delay(beatPeriod,
                                 combinedSource_.Token);
      if (!await pulse_(cancellationToken_))
      {
        stoppedHeartCts_.Cancel();
        return;
      }

      await delayTask;
    }
  }
}