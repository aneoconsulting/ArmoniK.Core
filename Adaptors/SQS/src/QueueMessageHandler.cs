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

using Amazon.SQS;
using Amazon.SQS.Model;

using ArmoniK.Core.Base;

namespace ArmoniK.Core.Adapters.SQS;

internal class QueueMessageHandler : IQueueMessageHandler
{
  private readonly int             ackDeadlinePeriod_;
  private readonly Heart           autoExtendAckDeadline_;
  private readonly AmazonSQSClient client_;
  private readonly string          queueUrl_;
  private readonly string          receiptHandle_;

  public QueueMessageHandler(Message         message,
                             AmazonSQSClient client,
                             string          queueUrl,
                             int             ackDeadlinePeriod,
                             int             ackExtendDeadlineStep)
  {
    MessageId          = message.MessageId;
    TaskId             = message.Body;
    ReceptionDateTime  = DateTime.UtcNow;
    Status             = QueueMessageStatus.Running;
    client_            = client;
    queueUrl_          = queueUrl;
    receiptHandle_     = message.ReceiptHandle;
    ackDeadlinePeriod_ = ackDeadlinePeriod;
    autoExtendAckDeadline_ = new Heart(ModifyAckDeadline,
                                       TimeSpan.FromSeconds(ackExtendDeadlineStep),
                                       CancellationToken);

    autoExtendAckDeadline_.Start();
  }

  /// <inheritdoc />
  public async ValueTask DisposeAsync()
  {
    await autoExtendAckDeadline_.Stop()
                                .ConfigureAwait(false);

    switch (Status)
    {
      case QueueMessageStatus.Waiting:
      case QueueMessageStatus.Failed:
      case QueueMessageStatus.Running:
      case QueueMessageStatus.Postponed:
        await client_.ChangeMessageVisibilityAsync(new ChangeMessageVisibilityRequest
                                                   {
                                                     VisibilityTimeout = 0,
                                                     QueueUrl          = queueUrl_,
                                                     ReceiptHandle     = receiptHandle_,
                                                   },
                                                   CancellationToken.None)
                     .ConfigureAwait(false);

        break;
      case QueueMessageStatus.Cancelled:
      case QueueMessageStatus.Processed:
      case QueueMessageStatus.Poisonous:
        await client_.DeleteMessageAsync(new DeleteMessageRequest
                                         {
                                           QueueUrl      = queueUrl_,
                                           ReceiptHandle = receiptHandle_,
                                         },
                                         CancellationToken.None)
                     .ConfigureAwait(false);


        break;
      default:
        throw new ArgumentOutOfRangeException();
    }
  }

  /// <inheritdoc />
  public CancellationToken CancellationToken { get; set; }

  /// <inheritdoc />
  public string MessageId { get; }

  /// <inheritdoc />
  public string TaskId { get; }

  /// <inheritdoc />
  public QueueMessageStatus Status { get; set; }

  /// <inheritdoc />
  public DateTime ReceptionDateTime { get; init; }

  private Task ModifyAckDeadline(CancellationToken cancellationToken)
    => client_.ChangeMessageVisibilityAsync(new ChangeMessageVisibilityRequest
                                            {
                                              VisibilityTimeout = ackDeadlinePeriod_,
                                              QueueUrl          = queueUrl_,
                                              ReceiptHandle     = receiptHandle_,
                                            },
                                            cancellationToken);
}
