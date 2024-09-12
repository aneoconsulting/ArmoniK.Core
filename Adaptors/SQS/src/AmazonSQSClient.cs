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

using System.Threading;
using System.Threading.Tasks;

using Amazon.SQS;
using Amazon.SQS.Model;

namespace ArmoniK.Core.Adapters.SQS;

internal static class AmazonSqsClientExt
{
  public static async Task<string> GetOrCreateQueueUrlAsync(this AmazonSQSClient client,
                                                            string               queueName,
                                                            CancellationToken    cancellationToken)
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
                                              QueueName = queueName,
                                            },
                                            cancellationToken)
                          .ConfigureAwait(false)).QueueUrl;
    }
  }
}
