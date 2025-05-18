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

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Amazon.SQS;
using Amazon.SQS.Model;

namespace ArmoniK.Core.Adapters.SQS;

internal static class AmazonSqsClientExt
{
  public static async Task<string> GetOrCreateQueueUrlAsync(this AmazonSQSClient       client,
                                                            string                     queueName,
                                                            Dictionary<string, string> tags,
                                                            Dictionary<string, string> attributes,
                                                            CancellationToken          cancellationToken)
  {
    try
    {
      return (await client.GetQueueUrlAsync(queueName,
                                            cancellationToken)
                          .ConfigureAwait(false)).QueueUrl;
    }
    catch (QueueDoesNotExistException)
    {
      return (await client.CreateQueueAsync(new CreateQueueRequest
                                            {
                                              QueueName  = queueName,
                                              Tags       = tags,
                                              Attributes = attributes,
                                            },
                                            cancellationToken)
                          .ConfigureAwait(false)).QueueUrl;
    }
  }

  public static string GetQueueName(this AmazonSQSClient client,
                                    SQS                  options,
                                    int                  priority,
                                    string?              partition = null)
  {
    _ = client;

    if (string.IsNullOrEmpty(partition))
    {
      partition = options.PartitionId;
    }

    return (options.Prefix, options.MaxPriority) switch
           {
             (null or "", < 1) => partition,
             (null or "", _)   => $"{partition}-{priority}",
             (var prefix, < 1) => $"{prefix}-{partition}",
             var (prefix, _)   => $"{prefix}-{partition}-{priority}",
           };
  }
}
