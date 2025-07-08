// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;

using Google.Cloud.PubSub.V1;

using Microsoft.Extensions.Logging;

using Encoding = System.Text.Encoding;

namespace ArmoniK.Core.Adapters.PubSub;

internal class QueueMessageHandler : IQueueMessageHandler
{
  private readonly int                        ackDeadlinePeriod_;
  private readonly string                     ackId_;
  private readonly Heart                      autoExtendAckDeadline_;
  private readonly ILogger                    logger_;
  private readonly SubscriberServiceApiClient subscriberServiceApiClient_;
  private readonly SubscriptionName           subscriptionName_;
  private          StackTrace?                stackTrace_;

  public QueueMessageHandler(ReceivedMessage            message,
                             SubscriberServiceApiClient subscriberServiceApiClient,
                             SubscriptionName           subscriptionName,
                             int                        ackDeadlinePeriod,
                             int                        ackExtendDeadlineStep,
                             ILogger                    logger)
  {
    subscriberServiceApiClient_ = subscriberServiceApiClient;
    subscriptionName_           = subscriptionName;
    MessageId                   = message.Message.MessageId;
    TaskId                      = Encoding.UTF8.GetString(message.Message.Data.ToByteArray());
    ReceptionDateTime           = DateTime.UtcNow;
    Status                      = QueueMessageStatus.Waiting;
    ackId_                      = message.AckId;
    ackDeadlinePeriod_          = ackDeadlinePeriod;
    logger_                     = logger;
    stackTrace_                 = new StackTrace(true);
    autoExtendAckDeadline_ = new Heart(ModifyAckDeadline,
                                       TimeSpan.FromSeconds(ackExtendDeadlineStep),
                                       CancellationToken);

    autoExtendAckDeadline_.Start();
  }

  public async ValueTask DisposeAsync()
  {
    stackTrace_ = null;

    await autoExtendAckDeadline_.Stop()
                                .ConfigureAwait(false);

    switch (Status)
    {
      case QueueMessageStatus.Waiting:
      case QueueMessageStatus.Failed:
      case QueueMessageStatus.Running:
      case QueueMessageStatus.Postponed:
        await subscriberServiceApiClient_.ModifyAckDeadlineAsync(subscriptionName_,
                                                                 new[]
                                                                 {
                                                                   ackId_,
                                                                 },
                                                                 0)
                                         .ConfigureAwait(false);

        break;
      case QueueMessageStatus.Cancelled:
      case QueueMessageStatus.Processed:
      case QueueMessageStatus.Poisonous:
        await subscriberServiceApiClient_.AcknowledgeAsync(subscriptionName_,
                                                           new[]
                                                           {
                                                             ackId_,
                                                           })
                                         .ConfigureAwait(false);


        break;
      default:
        throw new ArgumentOutOfRangeException();
    }

    GC.SuppressFinalize(this);
  }

  public CancellationToken  CancellationToken { get; set; }
  public string             MessageId         { get; }
  public string             TaskId            { get; }
  public QueueMessageStatus Status            { get; set; }
  public DateTime           ReceptionDateTime { get; init; }

  public Task ModifyAckDeadline(CancellationToken cancellationToken)
    => subscriberServiceApiClient_.ModifyAckDeadlineAsync(subscriptionName_,
                                                          new[]
                                                          {
                                                            ackId_,
                                                          },
                                                          ackDeadlinePeriod_);

  ~QueueMessageHandler()
  {
    if (stackTrace_ is null)
    {
      return;
    }

    logger_.LogError("QueueMessageHandler for Message {MessageId} and Task {TaskId} was not disposed: Created {MessageCreationStackTrace}",
                     MessageId,
                     TaskId,
                     stackTrace_);

    DisposeAsync()
      .AsTask()
      .GetAwaiter()
      .GetResult();
  }
}
