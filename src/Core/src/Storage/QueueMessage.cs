// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.gRPC.V1;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Storage
{
  [PublicAPI]
  public class QueueMessage : IAsyncDisposable
  {
    private readonly Func<Task> disposeFunc_;
    private readonly ILogger    logger;

    public QueueMessage(string            messageId,
                        TaskId            taskId,
                        Func<Task>        disposeFunc,
                        ILogger logger,
                        CancellationToken cancellationToken)
    {
      disposeFunc_      = disposeFunc;
      this.logger       = logger;
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
      using var _ = logger.LogFunction(MessageId,
                         functionName: $"{nameof(QueueMessage)}.{nameof(DisposeAsync)}");
      await disposeFunc_();
#pragma warning disable CA1816 // Dispose methods should call SuppressFinalize
      GC.SuppressFinalize(this);
#pragma warning restore CA1816 // Dispose methods should call SuppressFinalize
    }
  }
}