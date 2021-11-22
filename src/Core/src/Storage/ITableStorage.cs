using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.gRPC.V1;

using JetBrains.Annotations;

using TaskStatus = ArmoniK.Core.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Storage
{
  [PublicAPI]
  public interface ITableStorage
  {
    TimeSpan PollingDelay { get; } 

    Task<SessionId> CreateSessionAsync(SessionOptions sessionOptions, CancellationToken cancellationToken = default);

    Task CloseSessionAsync(SessionId sessionId, CancellationToken cancellationToken = default);

    Task CancelSessionAsync(SessionId sessionId, CancellationToken cancellationToken = default);

    Task<bool> IsSessionCancelledAsync(SessionId sessionId, CancellationToken cancellationToken = default);

    Task<bool> IsSessionClosedAsync(SessionId sessionId, CancellationToken cancellationToken = default);

    Task<SessionOptions> GetSessionOptions(SessionId sessionId, CancellationToken cancellationToken = default);

    Task<TaskId> InitializeTaskCreation(SessionId session, TaskOptions options, CancellationToken cancellationToken = default);

    Task FinalizeTaskCreation(TaskId taskId, CancellationToken cancellationToken = default);

    Task<TaskData> ReadTaskAsync(TaskId id, CancellationToken cancellationToken = default);

    Task UpdateTaskStatusAsync(TaskId id, TaskStatus status, CancellationToken cancellationToken = default);

    Task<TaskId> CancelTask(TaskId id, CancellationToken cancellationToken = default);

    Task<TaskId> CancelTask(TaskFilter filter, CancellationToken cancellationToken = default);

    Task IncreaseRetryCounterAsync(TaskId id, CancellationToken cancellationToken = default);

    Task DeleteTaskAsync(TaskId id, CancellationToken cancellationToken = default);

    IAsyncEnumerable<TaskId> ListTasksAsync(TaskFilter filter, CancellationToken cancellationToken = default);
    Task<int> CountTasksAsync(TaskFilter filter, CancellationToken cancellationToken = default);
    
  }
}
