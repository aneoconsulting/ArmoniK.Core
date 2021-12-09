// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.gRPC.V1;

using JetBrains.Annotations;

namespace ArmoniK.Core.Storage
{
  [PublicAPI]
  public class QueueMessage : IAsyncDisposable
  {
    private readonly Func<Task> disposeFunc_;

    public QueueMessage(string            messageId,
                        TaskId            taskId,
                        Func<Task>        disposeFunc,
                        CancellationToken cancellationToken)
    {
      disposeFunc_         = disposeFunc;
      MessageId         = messageId;
      TaskId            = taskId;
      CancellationToken = cancellationToken;
    }

    public string            MessageId         { get; init; }
    public TaskId            TaskId            { get; init; }
    public CancellationToken CancellationToken { get; init; }
    
    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
      await disposeFunc_();
      GC.SuppressFinalize(this);
    }
  }
}