using System;

using AmqpNetLite = Amqp;

namespace ArmoniK.Adapters.Amqp
{
  namespace Options
  {
    public class Amqp
    {
      public string   Address                { get; set; }
      public int   MaxPriority            { get; set; }
      public TimeSpan LockRefreshPeriodicity { get; set; }
      public TimeSpan LockRefreshExtension   { get; set; }
    }
  }


  //public class QueueStorage : IQueueStorage, IDisposable
  //{
  //  private readonly ILogger<QueueStorage>                                               logger_;
  //  private readonly AsyncLazy<AmqpNetLite.ISenderLink>[]                                senders_;
  //  private readonly AsyncLazy<AmqpNetLite.IReceiverLink>[]                              receivers_;
  //  private readonly Dictionary<string, (AmqpNetLite.Message, AmqpNetLite.IReceiverLink)> messages_;
  //  private readonly AsyncLazy<AmqpNetLite.ISenderLink>                                  dlq_;

  //  public QueueStorage(IOptions<Options.Amqp> options, SessionProvider sessionProvider, ILogger<QueueStorage> logger)
  //  {
  //    LockRefreshPeriodicity = options.Value.LockRefreshPeriodicity;
  //    LockRefreshExtension   = options.Value.LockRefreshExtension;
  //    MaxPriority            = options.Value.MaxPriority;
  //    logger_ = logger;

  //    var nbLinks = (MaxPriority + 3) / 3;

  //    senders_ = Enumerable.Range(0, nbLinks)
  //                         .Select(i => new AsyncLazy<AmqpNetLite.ISenderLink>(async ()
  //                                                                               => new
  //                                                                                 AmqpNetLite.SenderLink(await sessionProvider.GetAsync(),
  //                                                                                                        $"SenderLink{i}",
  //                                                                                                        $"q{i}")))
  //                         .ToArray();

  //    receivers_ = Enumerable.Range(0, nbLinks)
  //                         .Select(i => new AsyncLazy<AmqpNetLite.IReceiverLink>(async ()
  //                                                                               => new
  //                                                                                 AmqpNetLite.ReceiverLink(await sessionProvider.GetAsync(),
  //                                                                                                          $"ReceiverLink{i}",
  //                                                                                                          $"q{i}")))
  //                         .ToArray();
  //    dlq_ = new AsyncLazy<AmqpNetLite.ISenderLink>(async ()
  //                                                    => new
  //                                                      AmqpNetLite.SenderLink(await sessionProvider.GetAsync(),
  //                                                                             "ReceiverLink-dlq",
  //                                                                             "dlq"));

  //  }

  //  /// <inheritdoc />
  //  public TimeSpan LockRefreshPeriodicity { get; }

  //  /// <inheritdoc />
  //  public TimeSpan LockRefreshExtension { get; }

  //  /// <inheritdoc />
  //  public int MaxPriority { get; } = 9;

  //  /// <inheritdoc />
  //  public async IAsyncEnumerable<QueueMessage> PullAsync(int nbMessages, CancellationToken cancellationToken = default)
  //  {
  //    using var _ = logger_.LogFunction();
  //    var nbPulledMessage = 0;

  //    while(nbPulledMessage < nbMessages)
  //    {
  //      var currentNbMessages = nbPulledMessage;
  //      for (var i = receivers_.Length - 1; i >= 0; --i)
  //      {
  //        var receiver = await receivers_[i];
  //        var message  = await receiver.ReceiveAsync(TimeSpan.FromSeconds(5));
  //        if (message is null) continue;


  //        if (message.Body is not TaskId taskId)
  //        {
  //          logger_.LogError("Body of message with Id={id} is not a TaskId", message.Properties.MessageId);
  //          continue;
  //        }

  //        nbPulledMessage++;
  //        messages_[message.Properties.MessageId] = (message, receiver);
  //        yield return new QueueMessage(message.Properties.MessageId, taskId);
          
  //        break;
  //      }

  //      if (nbPulledMessage == currentNbMessages) break;
  //    }
  //  }

  //  /// <inheritdoc />
  //  public async Task DeleteAsync(string id, CancellationToken cancellationToken = default)
  //  {
  //    var (message, receiver) = messages_[id];
  //    receiver.Release(message);
      
  //  }

  //  /// <inheritdoc />
  //  public async Task<bool> RenewLockAsync(string id, CancellationToken cancellationToken = default) => TODO_IMPLEMENT_ME;

  //  /// <inheritdoc />
  //  public async Task UnlockAsync(string id, CancellationToken cancellationToken = default) => TODO_IMPLEMENT_ME;

  //  /// <inheritdoc />
  //  public IAsyncEnumerable<string> EnqueueMessagesAsync(IEnumerable<QueueMessage> messages,
  //                                                       int priority = 1,
  //                                                       CancellationToken cancellationToken = default) => TODO_IMPLEMENT_ME;

  //  /// <inheritdoc />
  //  public async Task<string> RequeueMessage(QueueMessage message, CancellationToken cancellationToken = default) => TODO_IMPLEMENT_ME;
  //}

}
