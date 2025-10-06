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
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

using Amazon.SQS;
using Amazon.SQS.Model;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.Exceptions;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.SQS;

internal class QueueMessageHandler : IQueueMessageHandler
{
  private readonly int             ackDeadlinePeriod_;
  private readonly Heart           autoExtendAckDeadline_;
  private readonly AmazonSQSClient client_;
  private readonly ILogger         logger_;
  private readonly SQS             options_;
  private readonly string          queueUrl_;
  private readonly string          receiptHandle_;
  private readonly IDisposable?    scope_;
  private          StackTrace?     stackTrace_;

  public QueueMessageHandler(Message         message,
                             AmazonSQSClient client,
                             string          queueUrl,
                             SQS             options,
                             ILogger         logger)
  {
    MessageId          = message.MessageId;
    TaskId             = message.Body;
    ReceptionDateTime  = DateTime.UtcNow;
    client_            = client;
    queueUrl_          = queueUrl;
    receiptHandle_     = message.ReceiptHandle;
    ackDeadlinePeriod_ = options.AckDeadlinePeriod;
    logger_            = logger;
    options_           = options;
    stackTrace_        = new StackTrace(true);
    autoExtendAckDeadline_ = new Heart(ModifyAckDeadline,
                                       TimeSpan.FromSeconds(options.AckExtendDeadlineStep),
                                       CancellationToken);

    var d = new Dictionary<string, object>
            {
              {
                "messageHandler", MessageId
              },
              {
                "taskId", TaskId
              },
            };

    scope_ = logger_.BeginScope(d);

    autoExtendAckDeadline_.Start();
  }

  /// <inheritdoc />
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
        await Retry(() => client_.ChangeMessageVisibilityAsync(new ChangeMessageVisibilityRequest
                                                               {
                                                                 VisibilityTimeout = 0,
                                                                 QueueUrl          = queueUrl_,
                                                                 ReceiptHandle     = receiptHandle_,
                                                               },
                                                               CancellationToken.None),
                    options_.MaxRetries,
                    "set message visibility timeout to 0",
                    FilterException,
                    CancellationToken.None)
          .ConfigureAwait(false);

        break;
      case QueueMessageStatus.Cancelled:
      case QueueMessageStatus.Processed:
      case QueueMessageStatus.Poisonous:
        await Retry(() => client_.DeleteMessageAsync(new DeleteMessageRequest
                                                     {
                                                       QueueUrl      = queueUrl_,
                                                       ReceiptHandle = receiptHandle_,
                                                     },
                                                     CancellationToken.None),
                    options_.MaxRetries,
                    "delete message",
                    FilterException,
                    CancellationToken.None)
          .ConfigureAwait(false);

        break;
      default:
        throw new ArgumentOutOfRangeException();
    }

    scope_?.Dispose();
    GC.SuppressFinalize(this);
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

  private async Task<T> Retry<T>(Func<Task<T>>         function,
                                 int                   maxRetries,
                                 string                exceptionMessage,
                                 Func<Exception, bool> retryFilter,
                                 CancellationToken     cancellationToken = default)
  {
    Exception? e = null;
    for (var i = 0; i < maxRetries + 1; i++)
    {
      try
      {
        return await function()
                 .ConfigureAwait(false);
      }
      catch (Exception ex) when (retryFilter(ex))
      {
        e = ex;
        logger_.LogWarning(e,
                           "Failed to {Message} {Retry}/{MaxRetries}",
                           exceptionMessage,
                           i,
                           maxRetries + 1);
        await Task.Delay((i + 1) * (i + 1) * 100,
                         cancellationToken)
                  .ConfigureAwait(false);
      }
    }

    throw new ArmoniKException($"Number of retries exceeded when trying to {exceptionMessage}",
                               e);
  }

  private static bool FilterException(Exception ex)
    => ex is HttpRequestException or AmazonSQSException or SocketException or IOException;

  private Task ModifyAckDeadline(CancellationToken cancellationToken)
    => Retry(() => client_.ChangeMessageVisibilityAsync(new ChangeMessageVisibilityRequest
                                                        {
                                                          VisibilityTimeout = ackDeadlinePeriod_,
                                                          QueueUrl          = queueUrl_,
                                                          ReceiptHandle     = receiptHandle_,
                                                        },
                                                        CancellationToken.None),
             options_.MaxRetries,
             "extend SQS message deadline",
             FilterException,
             cancellationToken);

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
