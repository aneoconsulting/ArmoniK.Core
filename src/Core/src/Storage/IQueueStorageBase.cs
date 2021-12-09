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

    IAsyncEnumerable<IQueueMessage> PullAsync(int nbMessages, CancellationToken cancellationToken = default);

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
  }
}
