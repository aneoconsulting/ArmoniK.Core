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
    Task<string> CreateSessionAsync(SessionOptions sessionOptions, CancellationToken cancellationToken = default);

    Task CloseSessionAsync(string sessionId, CancellationToken cancellationToken = default);

    Task CancelSessionAsync(string sessionId, CancellationToken cancellationToken = default);

    Task<bool> IsSessionCancelledAsync(string sessionId, CancellationToken cancellationToken = default);

    Task<SessionOptions> GetSessionOptions(string sessionId, CancellationToken cancellationToken = default);

    Task<TaskId> CreateTask(SessionId session, string payloadKey, TaskOptions options, CancellationToken cancellationToken = default);

    Task<TaskData> ReadTaskAsync(TaskId id, CancellationToken cancellationToken = default);

    Task UpdateTaskStatusAsync(TaskId id, TaskStatus status, CancellationToken cancellationToken = default);

    Task DeleteTaskAsync(TaskId id, CancellationToken cancellationToken = default);

    IAsyncEnumerable<TaskId> ListTasksAsync(SessionId id, CancellationToken cancellationToken = default);

    IAsyncEnumerable<TaskId> ListTasksAsync(TaskFilter filter, CancellationToken cancellationToken = default);
    
  }
}
