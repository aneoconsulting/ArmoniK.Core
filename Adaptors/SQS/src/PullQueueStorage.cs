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
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using Amazon.SQS;
using Amazon.SQS.Model;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.SQS;

internal class PullQueueStorage : IPullQueueStorage
{
  private readonly int             ackDeadlinePeriod_;
  private readonly int             ackExtendDeadlineStep_;
  private readonly AmazonSQSClient client_;

  // ReSharper disable once NotAccessedField.Local
  private readonly ILogger<PullQueueStorage> logger_;

  private readonly string                     queueName_;
  private readonly Dictionary<string, string> tags_;
  private          bool                       isInitialized_;
  private          string?                    queueUrl_;

  public PullQueueStorage(AmazonSQSClient           client,
                          SQS                       options,
                          ILogger<PullQueueStorage> logger)
  {
    client_    = client;
    logger_    = logger;
    queueName_ = client.GetQueueName(options);
    tags_      = options.Tags;

    ackDeadlinePeriod_     = options.AckDeadlinePeriod;
    ackExtendDeadlineStep_ = options.AckExtendDeadlineStep;
  }

  public async IAsyncEnumerable<IQueueMessageHandler> PullMessagesAsync(int                                        nbMessages,
                                                                        [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      throw new InvalidOperationException($"{nameof(PullQueueStorage)} should be initialized before calling this method.");
    }

    var messages = await client_.ReceiveMessageAsync(new ReceiveMessageRequest
                                                     {
                                                       QueueUrl            = queueUrl_!,
                                                       MaxNumberOfMessages = nbMessages,
                                                       VisibilityTimeout   = ackDeadlinePeriod_,
                                                     },
                                                     cancellationToken)
                                .ConfigureAwait(false);

    foreach (var message in messages.Messages)
    {
      cancellationToken.ThrowIfCancellationRequested();
      yield return new QueueMessageHandler(message,
                                           client_,
                                           queueUrl_!,
                                           ackDeadlinePeriod_,
                                           ackExtendDeadlineStep_);
    }
  }

  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(isInitialized_
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy("Plugin is not yet initialized."));

  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      queueUrl_ = await client_.GetOrCreateQueueUrlAsync(queueName_,
                                                         tags_,
                                                         cancellationToken)
                               .ConfigureAwait(false);

      isInitialized_ = true;
    }
  }

  public int MaxPriority
    => int.MaxValue;
}
