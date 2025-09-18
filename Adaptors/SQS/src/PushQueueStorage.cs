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
using System.Threading;
using System.Threading.Tasks;

using Amazon.SQS;
using Amazon.SQS.Model;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Base.Exceptions;
using ArmoniK.Utils;

using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.SQS;

internal class PushQueueStorage : IPushQueueStorage
{
  private readonly MemoryCache               cache_;
  private readonly AmazonSQSClient           client_;
  private readonly ILogger<PushQueueStorage> logger_;
  private readonly SQS                       options_;
  private          bool                      isInitialized_;

  public PushQueueStorage(AmazonSQSClient           client,
                          SQS                       options,
                          ILogger<PushQueueStorage> logger)
  {
    client_  = client;
    options_ = options;
    logger_  = logger;

    cache_ = new MemoryCache(new MemoryCacheOptions());
  }

  public async Task PushMessagesAsync(IEnumerable<MessageData> messages,
                                      string                   partitionId,
                                      CancellationToken        cancellationToken = new())
  {
    if (!isInitialized_)
    {
      throw new InvalidOperationException($"{nameof(PushQueueStorage)} should be initialized before calling this method.");
    }

    await messages.GroupBy(m => m.Options.Priority)
                  .ToAsyncEnumerable()
                  // SQS supports a maximum of 10 messages per batch request, see quotas
                  .SelectMany(group => group.Chunk(10)
                                            .ToAsyncEnumerable()
                                            .SelectAwait(async chunk =>
                                                         {
                                                           var queueName = client_.GetQueueName(options_,
                                                                                                group.Key,
                                                                                                partitionId);
                                                           var queueUrl = await cache_.GetOrCreateAsync(queueName,
                                                                                                        _ => client_.GetOrCreateQueueUrlAsync(queueName,
                                                                                                                                              options_.Tags,
                                                                                                                                              options_.Attributes,
                                                                                                                                              cancellationToken))
                                                                                      .ConfigureAwait(false);
                                                           return (queueUrl, chunk);
                                                         }))
                  .ParallelForEach(new ParallelTaskOptions(options_.DegreeOfParallelism),
                                   async entries =>
                                   {
                                     var (queueUrl, chunk) = entries;
                                     var remainingEntries = chunk.Select(data => new SendMessageBatchRequestEntry
                                                                                 {
                                                                                   Id = Guid.NewGuid()
                                                                                            .ToString(),
                                                                                   MessageBody = data.TaskId,
                                                                                 })
                                                                 .ToList();
                                     var retry = 0;
                                     while (remainingEntries.Any())
                                     {
                                       retry++;
                                       var response = await client_.SendMessageBatchAsync(new SendMessageBatchRequest
                                                                                          {
                                                                                            QueueUrl = queueUrl,
                                                                                            Entries  = remainingEntries,
                                                                                          },
                                                                                          cancellationToken)
                                                                   .ConfigureAwait(false);

                                       if (logger_.IsEnabled(LogLevel.Debug))
                                       {
                                         logger_.LogDebug("pushed {Messages} for {TaskIds} onto {QueueUrl}, {statusCode}, {responseMetadata}",
                                                          remainingEntries.Select(entry => entry.Id)
                                                                          .ToList(),
                                                          remainingEntries.Select(entry => entry.MessageBody)
                                                                          .ToList(),
                                                          queueUrl,
                                                          response.HttpStatusCode,
                                                          response.ResponseMetadata);
                                       }

                                       if (response.Failed.Any())
                                       {
                                         var failed = response.Failed.ToDictionary(entry => entry.Id);

                                         remainingEntries.RemoveAll(entry => !failed.ContainsKey(entry.Id));

                                         var failedData = remainingEntries.Select(entry => new
                                                                                           {
                                                                                             entry.Id,
                                                                                             entry.MessageBody,
                                                                                             failed[entry.Id].Code,
                                                                                             failed[entry.Id].Message,
                                                                                             failed[entry.Id].SenderFault,
                                                                                           })
                                                                          .ToList();

                                         logger_.LogWarning("failed messages : {failed}",
                                                            failedData);

                                         if (response.Failed.Any(entry => entry.SenderFault))
                                         {
                                           throw new
                                             QueueInsertionFailedException($"Some messages were not pushed due to sender-related issues. \nMessages: {failedData}");
                                         }

                                         if (retry > 4)
                                         {
                                           throw new
                                             QueueInsertionFailedException($"Some messages were not pushed and retries number was exceeded. \nMessages: {failedData}");
                                         }
                                       }
                                       else
                                       {
                                         remainingEntries = new List<SendMessageBatchRequestEntry>();
                                       }
                                     }
                                   })
                  .ConfigureAwait(false);
  }

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

  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(isInitialized_
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy("Plugin is not yet initialized."));
}
