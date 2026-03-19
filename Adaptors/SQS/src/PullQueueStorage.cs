// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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

using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.SQS;

internal class PullQueueStorage : IPullQueueStorage
{
  private readonly MemoryCache     cache_;
  private readonly AmazonSQSClient client_;

  private readonly ILogger logger_;

  private readonly SQS  options_;
  private          bool isInitialized_;

  public PullQueueStorage(AmazonSQSClient           client,
                          SQS                       options,
                          ILogger<PullQueueStorage> logger)
  {
    options_ = options;
    client_  = client;
    logger_  = logger;
    cache_   = new MemoryCache(new MemoryCacheOptions());

    logger_.LogDebug("Created SQS PullQueueStorage with options {@SqsOptions}",
                     options_);
  }

  public async IAsyncEnumerable<IQueueMessageHandler> PullMessagesAsync(string                                     partitionId,
                                                                        int                                        nbMessages,
                                                                        [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      throw new InvalidOperationException($"{nameof(PullQueueStorage)} should be initialized before calling this method.");
    }

    var queueInfos = new List<(string queueUrl, string queueName, int priority)>();

    var maxPriority = int.Max(options_.MaxPriority,
                              1);
    for (var i = maxPriority; i >= 1; i--)
    {
      var priority = i;
      logger_.LogDebug("Getting queue for priority #{SqsPriority} with options {@SqsOptions}",
                       priority,
                       options_);

      var queueName = client_.GetQueueName(options_,
                                           priority,
                                           partitionId);

      // Use cache to get or create queue URL
      var queueUrl = await cache_.GetOrCreateAsync(queueName,
                                                   _ => client_.GetOrCreateQueueUrlAsync(queueName,
                                                                                         options_.Tags,
                                                                                         options_.Attributes,
                                                                                         cancellationToken))
                                 .ConfigureAwait(false);

      queueInfos.Add((queueUrl, queueName, priority));
    }

    foreach (var (queueUrl, queueName, priority) in queueInfos)
    {
      logger_.LogDebug("Try pulling {NbMessages} from {QueueUrl} (priority {Priority}) with options {@SqsOptions}",
                       nbMessages,
                       queueUrl,
                       priority,
                       options_);

      ReceiveMessageResponse? messages = null;

      try
      {
        messages = await client_.ReceiveMessageAsync(new ReceiveMessageRequest
                                                     {
                                                       QueueUrl            = queueUrl,
                                                       MaxNumberOfMessages = nbMessages,
                                                       VisibilityTimeout   = options_.AckDeadlinePeriod,
                                                       WaitTimeSeconds     = options_.WaitTimeSeconds,
                                                     },
                                                     cancellationToken)
                                .ConfigureAwait(false);
      }
      catch (Exception ex) when (ShouldRetryWithRefreshedUrl(ex))
      {
        logger_.LogWarning(ex,
                           "Failed to receive messages from queue {QueueUrl} (priority {Priority}), refreshing cache and retrying",
                           queueUrl,
                           priority);

        // Invalidate cache and get fresh URL
        cache_.Remove(queueName);
        var refreshedQueueUrl = await client_.GetOrCreateQueueUrlAsync(queueName,
                                                                       options_.Tags,
                                                                       options_.Attributes,
                                                                       cancellationToken)
                                             .ConfigureAwait(false);
        cache_.Set(queueName,
                   refreshedQueueUrl);

        // Retry with refreshed URL
        try
        {
          messages = await client_.ReceiveMessageAsync(new ReceiveMessageRequest
                                                       {
                                                         QueueUrl            = refreshedQueueUrl,
                                                         MaxNumberOfMessages = nbMessages,
                                                         VisibilityTimeout   = options_.AckDeadlinePeriod,
                                                         WaitTimeSeconds     = options_.WaitTimeSeconds,
                                                       },
                                                       cancellationToken)
                                  .ConfigureAwait(false);

          logger_.LogDebug("Successfully retried receiving messages from refreshed queue {QueueUrl} (priority {Priority})",
                           refreshedQueueUrl,
                           priority);
        }
        catch (Exception retryEx)
        {
          logger_.LogError(retryEx,
                           "Failed to receive messages even after refreshing queue URL for {QueueName} (priority {Priority})",
                           queueName,
                           priority);
          continue;
        }
      }

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
                                             options_,
                                             logger_);
      }

      yield break;
    }
  }

  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(isInitialized_
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy("Plugin is not yet initialized."));

  public Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      isInitialized_ = true;
    }

    return Task.CompletedTask;
  }

  public int MaxPriority
    => int.Max(options_.MaxPriority,
               1);

  /// <summary>
  ///   Determines if an exception should trigger a retry with a refreshed queue URL
  /// </summary>
  /// <param name="ex">The exception that occurred</param>
  /// <returns>True if the operation should be retried with a refreshed URL</returns>
  private static bool ShouldRetryWithRefreshedUrl(Exception ex)
    => ex switch
       {
         QueueDoesNotExistException                                                                 => true,
         AmazonSQSException sqsEx when sqsEx.ErrorCode == "AWS.SimpleQueueService.NonExistentQueue" => true,
         AmazonSQSException sqsEx when sqsEx.ErrorCode == "InvalidParameterValue"                   => true,
         _                                                                                          => false,
       };
}
