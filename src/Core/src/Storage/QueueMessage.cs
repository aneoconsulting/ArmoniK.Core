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

    public QueueMessage(string            MessageId,
                        TaskId            TaskId,
                        Func<Task>        disposeFunc,
                        CancellationToken CancellationToken)
    {
      disposeFunc_         = disposeFunc;
      this.MessageId         = MessageId;
      this.TaskId            = TaskId;
      this.CancellationToken = CancellationToken;
    }

    public string            MessageId         { get; init; }
    public TaskId            TaskId            { get; init; }
    public CancellationToken CancellationToken { get; init; }

    public void Deconstruct(out string            MessageId,
                            out TaskId            TaskId,
                            out CancellationToken CancellationToken)
    {
      MessageId         = this.MessageId;
      TaskId            = this.TaskId;
      CancellationToken = this.CancellationToken;
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync() => await disposeFunc_();
  }
}