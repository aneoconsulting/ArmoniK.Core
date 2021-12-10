// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;

using ArmoniK.Core.gRPC.V1;

using System.Threading;
using System.Threading.Tasks;

namespace ArmoniK.Core.Storage
{
  public enum QueueMessageStatus
  {
    Failed,
    Waiting = Failed,
    Running = Failed,
    Postponed,
    Processed,
    Cancelled = Processed,
    Poisonous,
  }

  public interface IQueueMessage : IAsyncDisposable
  {
    CancellationToken CancellationToken { get; }
    string MessageId { get; }
    TaskId TaskId { get; }

    QueueMessageStatus Status { get; set; }
  }
}