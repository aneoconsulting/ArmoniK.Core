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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Amazon.SQS;
using Amazon.SQS.Model;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
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

    var queueName = $"a{options_.Prefix}-{partitionId}";

    var queueUrl = await cache_.GetOrCreateAsync(queueName,
                                                 _ => client_.GetOrCreateQueueUrlAsync(queueName,
                                                                                       cancellationToken))
                               .ConfigureAwait(false);

    await messages.Select(data => new SendMessageBatchRequestEntry
                                  {
                                    Id = Guid.NewGuid()
                                             .ToString(),
                                    MessageBody = data.TaskId,
                                  })
                  .Chunk(10)
                  .ParallelForEach(async entries =>
                                   {
                                     var entriesList = entries.ToList();
                                     var response = await client_.SendMessageBatchAsync(new SendMessageBatchRequest
                                                                                        {
                                                                                          QueueUrl = queueUrl,
                                                                                          Entries  = entriesList,
                                                                                        },
                                                                                        cancellationToken)
                                                                 .ConfigureAwait(false);

                                     if (logger_.IsEnabled(LogLevel.Debug))
                                     {
                                       logger_.LogDebug("pushed {messages}, {statusCode}, {responseMetadata}",
                                                        entriesList.Select(entry => entry.Id)
                                                                   .ToList(),
                                                        response.HttpStatusCode,
                                                        response.ResponseMetadata);
                                     }

                                     if (response.Failed.Any())
                                     {
                                       logger_.LogWarning("failed messages : {failed}",
                                                          response.Failed.Select(entry => new
                                                                                          {
                                                                                            entry.Id,
                                                                                            entry.Code,
                                                                                            entry.Message,
                                                                                            entry.SenderFault,
                                                                                          })
                                                                  .ToList());
                                       throw new InvalidOperationException("Some message were not pushed");
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
    => int.MaxValue;

  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(isInitialized_
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy("Plugin is not yet initialized."));
}
