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
using System.Linq;
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
  private readonly AmazonSQSClient client_;

  // ReSharper disable once NotAccessedField.Local
  private readonly ILogger<PullQueueStorage> logger_;

  private readonly SQS      options_;
  private readonly string[] queueUrls_;
  private          bool     isInitialized_;

  public PullQueueStorage(AmazonSQSClient           client,
                          SQS                       options,
                          ILogger<PullQueueStorage> logger)
  {
    options_ = options;
    client_  = client;
    logger_  = logger;
    queueUrls_ = new string[int.Max(options.MaxPriority,
                                    1)];
    logger_.LogDebug("Created SQS PullQueueStorage with options {@SqsOptions}",
                     options_);
  }

  public async IAsyncEnumerable<IQueueMessageHandler> PullMessagesAsync(int                                        nbMessages,
                                                                        [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      throw new InvalidOperationException($"{nameof(PullQueueStorage)} should be initialized before calling this method.");
    }


    foreach (var queueUrl in queueUrls_.Reverse())
    {
      logger_.LogDebug("Try pulling {NbMessages} from {QueueUrl} with options {@SqsOptions}",
                       nbMessages,
                       queueUrl,
                       options_);

      var messages = await client_.ReceiveMessageAsync(new ReceiveMessageRequest
                                                       {
                                                         QueueUrl            = queueUrl,
                                                         MaxNumberOfMessages = nbMessages,
                                                         VisibilityTimeout   = options_.AckDeadlinePeriod,
                                                         WaitTimeSeconds     = options_.WaitTimeSeconds,
                                                       },
                                                       cancellationToken)
                                  .ConfigureAwait(false);

      if (messages?.Messages?.Count is null or 0)
      {
        continue;
      }

      foreach (var message in messages.Messages)
      {
        cancellationToken.ThrowIfCancellationRequested();
        yield return new QueueMessageHandler(message,
                                             client_,
                                             queueUrl,
                                             options_.AckDeadlinePeriod,
                                             options_.WaitTimeSeconds);
      }

      yield break;
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
      for (var i = 0; i < queueUrls_.Length; i++)
      {
        logger_.LogDebug("Initialize queue #{SqsPriority} with options {@SqsOptions}",
                         i,
                         options_);
        var queueName = client_.GetQueueName(options_,
                                             i + 1,
                                             options_.PartitionId);
        queueUrls_[i] = await client_.GetOrCreateQueueUrlAsync(queueName,
                                                               options_.Tags,
                                                               options_.Attributes,
                                                               cancellationToken)
                                     .ConfigureAwait(false);
      }

      isInitialized_ = true;
    }
  }

  public int MaxPriority
    => int.Max(options_.MaxPriority,
               1);
}
