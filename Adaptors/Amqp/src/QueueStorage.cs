// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Amqp;
using Amqp.Framing;

using ArmoniK.Api.Worker.Utils;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Amqp;

public class QueueStorage : IQueueStorage
{
  private const int MaxInternalQueuePriority = 10;

  private readonly ILogger<QueueStorage>      logger_;
  private readonly AsyncLazy<IReceiverLink>[] receivers_;
  private readonly AsyncLazy<ISenderLink>[]   senders_;

  private bool isInitialized_;

  public QueueStorage(Options.Amqp          options,
                      SessionProvider       sessionProvider,
                      ILogger<QueueStorage> logger)
  {
    if (string.IsNullOrEmpty(options.Host))
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"{nameof(Options.Amqp.Host)} is not defined.");
    }

    if (string.IsNullOrEmpty(options.User))
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"{nameof(Options.Amqp.User)} is not defined.");
    }

    if (string.IsNullOrEmpty(options.Password))
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"{nameof(Options.Amqp.Password)} is not defined.");
    }

    if (options.Port == 0)
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"{nameof(Options.Amqp.Port)} is not defined.");
    }

    if (options.MaxPriority < 1)
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"Minimum value for {nameof(Options.Amqp.MaxPriority)} is 1.");
    }


    MaxPriority = options.MaxPriority;
    logger_     = logger;

    var nbLinks = (MaxPriority + MaxInternalQueuePriority - 1) / MaxInternalQueuePriority;

    senders_ = Enumerable.Range(0,
                                nbLinks)
                         .Select(i => new AsyncLazy<ISenderLink>(() => new SenderLink(sessionProvider.Get(),
                                                                                      $"SenderLink{i}",
                                                                                      $"q{i}")))
                         .ToArray();

    receivers_ = Enumerable.Range(0,
                                  nbLinks)
                           .Select(i => new AsyncLazy<IReceiverLink>(() => new ReceiverLink(sessionProvider.Get(),
                                                                                            $"ReceiverLink{i}",
                                                                                            $"q{i}")))
                           .ToArray();
  }

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      var senders   = Task.WhenAll(senders_.Select(async lazy => await lazy));
      var receivers = Task.WhenAll(receivers_.Select(async lazy => await lazy));
      await Task.WhenAll(senders,
                         receivers)
                .ConfigureAwait(false);
      isInitialized_ = true;
    }
  }

  /// <inheritdoc />
  public ValueTask<bool> Check(HealthCheckTag tag)
    => ValueTask.FromResult(isInitialized_);

  /// <inheritdoc />
  public int MaxPriority { get; }

  /// <inheritdoc />
  public async IAsyncEnumerable<IQueueMessageHandler> PullAsync(int                                        nbMessages,
                                                                [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    using var _               = logger_.LogFunction();
    var       nbPulledMessage = 0;

    while (nbPulledMessage < nbMessages)
    {
      var currentNbMessages = nbPulledMessage;
      for (var i = receivers_.Length - 1; i >= 0; --i)
      {
        cancellationToken.ThrowIfCancellationRequested();
        var receiver = await receivers_[i];
        var message = await receiver.ReceiveAsync(TimeSpan.FromMilliseconds(100))
                                    .ConfigureAwait(false);
        if (message is null)
        {
          logger_.LogTrace("Message is null receiver {i}",
                           i);
          continue;
        }

        nbPulledMessage++;

        yield return new QueueMessageHandler(message,
                                             await senders_[i],
                                             receiver,
                                             Encoding.UTF8.GetString(message.Body as byte[] ?? throw new InvalidOperationException("Error while deserializing message")),
                                             logger_,
                                             cancellationToken);

        break;
      }

      if (nbPulledMessage == currentNbMessages)
      {
        break;
      }
    }
  }

  /// <inheritdoc />
  public async Task EnqueueMessagesAsync(IEnumerable<string> messages,
                                         int                 priority          = 1,
                                         CancellationToken   cancellationToken = default)
  {
    using var _ = logger_.LogFunction();

    logger_.LogDebug("Priority is {priority} ; will use queue #{queueId} with internal priority {internal priority}",
                     priority,
                     priority / MaxInternalQueuePriority,
                     priority % MaxInternalQueuePriority);

    var sender = await senders_[priority / MaxInternalQueuePriority];
    await Task.WhenAll(messages.Select(id => sender.SendAsync(new Message(Encoding.UTF8.GetBytes(id))
                                                              {
                                                                Header = new Header
                                                                         {
                                                                           Priority = (byte)(priority % MaxInternalQueuePriority),
                                                                         },
                                                                Properties = new Properties(),
                                                              })))
              .ConfigureAwait(false);
  }
}
