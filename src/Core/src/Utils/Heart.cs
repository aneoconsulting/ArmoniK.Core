using System;
using System.Threading;
using System.Threading.Tasks;

namespace ArmoniK.Core.Utils
{
  public class Heart
  {
    private bool IsStarted => nbPulsations_ >= 0;

    private int nbPulsations_ = -1;

    private readonly CancellationToken cancellationToken_;

    private CancellationTokenSource stoppedHeartCts_ = new();

    private Task runningTask_;

    private readonly Func<CancellationToken, Task<bool>> pulse_;

    protected readonly TimeSpan BeatPeriod;

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
      BeatPeriod        = beatPeriod;
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
    public Task Stop()
    {
      nbPulsations_ = 2;
      return runningTask_;
    }

    /// <summary>
    /// Start the heart. If the heart is beating, it has no effect.
    /// </summary>
    /// <param name="nbPulsations">Number of cycles to do. Set 0 for an infinite loop.</param>
    public void Start(uint nbPulsations=0)
    {
      if (nbPulsations_ == 0) // already running with infinite loop
      {
        return;
      }


      if (nbPulsations_ > 0) // already running
      {
        if (nbPulsations_ < nbPulsations || nbPulsations == 0) // new value is longer than current
        {
          nbPulsations_ = (int)nbPulsations;
        }

        return;
      }

      stoppedHeartCts_ = new CancellationTokenSource();
      nbPulsations_    = (int)nbPulsations;


      runningTask_ = Task.Factory.StartNew(async () =>
                                           {
                                             while (IsStarted)
                                             {
                                               NextPulseWaiter = FullCycle();
                                               await NextPulseWaiter;
                                               if (nbPulsations_ == 1)
                                               {
                                                 nbPulsations_ = -1;
                                                 stoppedHeartCts_.Cancel();
                                               }

                                               if (nbPulsations_ > 1) --nbPulsations_;
                                             }
                                           }, cancellationToken_, TaskCreationOptions.LongRunning, TaskScheduler.Current);
    }

    /// <summary>
    /// A task that finished with the next pulse
    /// </summary>
    public Task NextPulseWaiter { get; private set; }

    /// <summary>
    /// Triggered when the heart stops
    /// </summary>
    public CancellationToken HeartStopped => stoppedHeartCts_.Token;

    private async Task FullCycle()
    {
      using var delayTask = Task.Delay(BeatPeriod, cancellationToken_);
      if (!await pulse_(cancellationToken_))
      {
        stoppedHeartCts_.Cancel();
        nbPulsations_ = -1;
        return;
      }

      await delayTask;
      return;
    }
  }
}
