using AmqpNetLite = Amqp;

namespace ArmoniK.Adapters.Amqp
{

  //public class QueueStorage : IQueueStorage
  //{
  //  private readonly AmqpNetLite.Session   session_;
  //  private readonly ILogger<QueueStorage> logger_;
  //  AmqpNetLite.SenderLink                 sender_;
  //  private AmqpNetLite.ReceiverLink       receiver_;

  //  public QueueStorage(TimeSpan lockRefreshPeriodicity, TimeSpan lockRefreshExtension, AmqpNetLite.Session session)
  //  {
  //    LockRefreshPeriodicity = lockRefreshPeriodicity;
  //    LockRefreshExtension   = lockRefreshExtension;
  //    session_               = session;
      
      
  //    sender_ = new AmqpNetLite.SenderLink(session, "sender-link", "q1");

  //    receiver_ = new AmqpNetLite.ReceiverLink(session, "receiver_-link", "q1");


  //    //sender.Close();
  //    //session.Close();
  //    //connection.Close();
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
  //    var message = await receiver_.ReceiveAsync(LockRefreshPeriodicity);

  //    yield return message.Body as QueueMessage;
  //  }

  //  /// <inheritdoc />
  //  public async Task DeleteAsync(string id, CancellationToken cancellationToken = default)
  //  {
  //    receiver_.
  //  }

  //  /// <inheritdoc />
  //  public async Task<bool> RenewLockAsync(string id, CancellationToken cancellationToken = default) => TODO_IMPLEMENT_ME;

  //  /// <inheritdoc />
  //  public async Task UnlockAsync(string id, CancellationToken cancellationToken = default) => TODO_IMPLEMENT_ME;

  //  /// <inheritdoc />
  //  public IAsyncEnumerable<string> EnqueueMessagesAsync(IEnumerable<QueueMessage> messages,
  //                                                       int                       priority          = 1,
  //                                                       CancellationToken         cancellationToken = default) => TODO_IMPLEMENT_ME;

  //  /// <inheritdoc />
  //  public async Task<string> RequeueMessage(QueueMessage message, CancellationToken cancellationToken = default) => TODO_IMPLEMENT_ME;
  //}

}
