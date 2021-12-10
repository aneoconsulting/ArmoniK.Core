using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using Amqp;
using Amqp.Framing;

using ArmoniK.Core;
using ArmoniK.Core.gRPC.V1;
using ArmoniK.Core.Storage;
using ArmoniK.Core.Utils;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ArmoniK.Adapters.Amqp
{
  public class QueueStorage : IQueueStorage
  {
    private readonly ILogger<QueueStorage>                                                                 logger_;
    private readonly AsyncLazy<ISenderLink>[]                                                              senders_;
    private readonly AsyncLazy<IReceiverLink>[]                                                            receivers_;

    public QueueStorage(IOptions<Options.Amqp> options, SessionProvider sessionProvider, ILogger<QueueStorage> logger)
    {
      MaxPriority = options.Value.MaxPriority;
      logger_     = logger;

      var nbLinks = (MaxPriority + 3) / 3;

      senders_ = Enumerable.Range(0, nbLinks)
                           .Select(i => new AsyncLazy<ISenderLink>(async ()
                                                                     => new
                                                                       SenderLink(await sessionProvider.GetAsync(),
                                                                                  $"SenderLink{i}",
                                                                                  $"q{i}")))
                           .ToArray();

      receivers_ = Enumerable.Range(0, nbLinks)
                             .Select(i => new AsyncLazy<IReceiverLink>(async ()
                                                                         => new
                                                                           ReceiverLink(await sessionProvider.GetAsync(),
                                                                                        $"ReceiverLink{i}",
                                                                                        $"q{i}")))
                             .ToArray();
    }

    /// <inheritdoc />
    public int MaxPriority { get; }

    /// <inheritdoc />
    public async IAsyncEnumerable<IQueueMessage> PullAsync(int nbMessages, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
      using var _ = logger_.LogFunction();
      var nbPulledMessage = 0;

      while (nbPulledMessage < nbMessages)
      {
        var currentNbMessages = nbPulledMessage;
        for (var i = receivers_.Length - 1; i >= 0; --i)
        {
          cancellationToken.ThrowIfCancellationRequested();
          var receiver = await receivers_[i];
          var message = await receiver.ReceiveAsync(TimeSpan.FromSeconds(5));
          if (message is null) continue;


          if (message.Body is not TaskId taskId)
          {
            logger_.LogError("Body of message with Id={id} is not a TaskId", message.Properties.MessageId);
            continue;
          }

          nbPulledMessage++;

          yield return new QueueMessage(message,
                                        await senders_[i],
                                        receiver,
                                        taskId,
                                        logger_,
                                        cancellationToken);

          break;
        }

        if (nbPulledMessage == currentNbMessages) break;
      }
    }

    /// <inheritdoc />
    public async Task EnqueueMessagesAsync(IEnumerable<TaskId> messages,
                                     int                 priority          = 1,
                                     CancellationToken   cancellationToken = default)
    {
      var sender = await senders_[priority / 3];
      await Task.WhenAll(messages.Select(id => sender.SendAsync(new Message(id)
                                                                {
                                                                  Header = new Header { Priority = (byte)((priority % 3)*4) },
                                                                })));
    }
  }
}