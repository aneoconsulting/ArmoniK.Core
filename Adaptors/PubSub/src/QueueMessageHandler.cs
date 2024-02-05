// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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

using ArmoniK.Core.Base;

using Google.Cloud.PubSub.V1;

using Encoding = System.Text.Encoding;

namespace ArmoniK.Core.Adapters.PubSub;

internal class QueueMessageHandler : IQueueMessageHandler
{
  private readonly int                        ackDeadlinePeriod_;
  private readonly string                     ackId_;
  private readonly Heart                      autoExtendAckDeadline_;
  private readonly SubscriberServiceApiClient subscriberServiceApiClient_;
  private readonly SubscriptionName           subscriptionName_;

  public QueueMessageHandler(ReceivedMessage            message,
                             SubscriberServiceApiClient subscriberServiceApiClient,
                             SubscriptionName           subscriptionName,
                             int                        ackDeadlinePeriod,
                             int                        ackExtendDeadlineStep)
  {
    subscriberServiceApiClient_ = subscriberServiceApiClient;
    subscriptionName_           = subscriptionName;
    MessageId                   = message.Message.MessageId;
    TaskId                      = Encoding.UTF8.GetString(message.Message.Data.ToByteArray());
    ReceptionDateTime           = DateTime.UtcNow;
    Status                      = QueueMessageStatus.Running;
    ackId_                      = message.AckId;
    ackDeadlinePeriod_          = ackDeadlinePeriod;
    autoExtendAckDeadline_ = new Heart(ModifyAckDeadline,
                                       TimeSpan.FromSeconds(ackExtendDeadlineStep),
                                       CancellationToken);

    autoExtendAckDeadline_.Start();
  }

  public async ValueTask DisposeAsync()
  {
    await autoExtendAckDeadline_.Stop();

    switch (Status)
    {
      case QueueMessageStatus.Waiting:
      case QueueMessageStatus.Failed:
      case QueueMessageStatus.Running:
      case QueueMessageStatus.Postponed:
      case QueueMessageStatus.Poisonous:
        await subscriberServiceApiClient_.ModifyAckDeadlineAsync(subscriptionName_,
                                                                 new[]
                                                                 {
                                                                   ackId_,
                                                                 },
                                                                 0);

        break;
      case QueueMessageStatus.Cancelled:
      case QueueMessageStatus.Processed:
        await subscriberServiceApiClient_.AcknowledgeAsync(subscriptionName_,
                                                           new[]
                                                           {
                                                             ackId_,
                                                           });


        break;
      default:
        throw new ArgumentOutOfRangeException();
    }
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
}
