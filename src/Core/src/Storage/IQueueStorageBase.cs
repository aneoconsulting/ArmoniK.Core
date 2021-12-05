// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.gRPC.V1;

namespace ArmoniK.Core.Storage
{
  public interface IQueueStorageBase
  { int MaxPriority { get; }

    IAsyncEnumerable<QueueMessage> PullAsync(int nbMessages, CancellationToken cancellationToken = default);

    /// <summary>
    /// Indicates that the message was successfully processed
    /// </summary>
    /// <param name="id"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    Task MessageProcessedAsync(string id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Indicates that the message is poisonous
    /// </summary>
    /// <param name="id"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    Task MessageRejectedAsync(string id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Submit new messages
    /// </summary>
    /// <param name="messages"></param>
    /// <param name="priority"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    Task EnqueueMessagesAsync(IEnumerable<TaskId> messages,
                              int                 priority          = 1,
                              CancellationToken   cancellationToken = default);

    /// <summary>
    /// Places the message in the back of the queue
    /// </summary>
    /// <param name="id"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    Task RequeueMessageAsync(string id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Places the message in the front of the queue
    /// </summary>
    /// <param name="id"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    Task ReleaseMessageAsync(string id, CancellationToken cancellationToken = default);
  }
}
