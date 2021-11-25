using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using AmqpNetLite = Amqp;

using ArmoniK.Core.Storage;

namespace ArmoniK.Adapters.Amqp
{
  public class QueueStorage : IQueueStorage
  {
    private readonly AmqpNetLite.Session session_;
    public QueueStorage(TimeSpan lockRefreshPeriodicity, TimeSpan lockRefreshExtension, AmqpNetLite.Session session)
    {
      LockRefreshPeriodicity = lockRefreshPeriodicity;
      LockRefreshExtension   = lockRefreshExtension;
      session_          = session;
      
      

      var message = new AmqpNetLite.Message("Hello AMQP!");
      var sender  = new AmqpNetLite.SenderLink(session, "sender-link", "q1");
      sender.Send(message);
      Console.WriteLine("Sent Hello AMQP!");

      var receiver = new AmqpNetLite.ReceiverLink(session, "receiver", "q1");

      receiver.




      sender.Close();
      session.Close();
      connection.Close();
    }

    /// <inheritdoc />
    public TimeSpan LockRefreshPeriodicity { get; }

    /// <inheritdoc />
    public TimeSpan LockRefreshExtension { get; }

    /// <inheritdoc />
    public IAsyncEnumerable<QueueMessage> PullAsync(int nbMessages, CancellationToken cancellationToken = default)
      => TODO_IMPLEMENT_ME;

    /// <inheritdoc />
    public async Task<QueueMessage> ReadAsync(string id, CancellationToken cancellationToken = default) => TODO_IMPLEMENT_ME;

    /// <inheritdoc />
    public async Task DeleteAsync(string id, CancellationToken cancellationToken = default) => TODO_IMPLEMENT_ME;

    /// <inheritdoc />
    public async Task<bool> RenewLockAsync(string id, CancellationToken cancellationToken = default) => TODO_IMPLEMENT_ME;

    /// <inheritdoc />
    public async Task UnlockAsync(string id, CancellationToken cancellationToken = default) => TODO_IMPLEMENT_ME;

    /// <inheritdoc />
    public IAsyncEnumerable<string> EnqueueMessagesAsync(IEnumerable<QueueMessage> messages,
                                                         int                       priority          = 1,
                                                         CancellationToken         cancellationToken = default) => TODO_IMPLEMENT_ME;

    /// <inheritdoc />
    public async Task<string> RequeueMessage(QueueMessage message, CancellationToken cancellationToken = default) => TODO_IMPLEMENT_ME;
  }
}
