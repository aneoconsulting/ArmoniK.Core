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

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Common.Storage;

using Grpc.Core;

using Output = ArmoniK.Core.Common.Storage.Output;
using TaskOptions = ArmoniK.Core.Base.DataStructures.TaskOptions;

namespace ArmoniK.Core.Common.gRPC.Services;

/// <summary>
///   Represents a request to create a task, including expected output keys, data dependencies, and payload chunks.
/// </summary>
/// <param name="ExpectedOutputKeys">The keys for expected outputs of the task.</param>
/// <param name="DataDependencies">The data dependencies required by the task.</param>
/// <param name="PayloadChunks">The payload data for the task, provided as a stream of byte chunks.</param>
public record TaskRequest(IEnumerable<string>                    ExpectedOutputKeys,
                          IEnumerable<string>                    DataDependencies,
                          IAsyncEnumerable<ReadOnlyMemory<byte>> PayloadChunks);

/// <summary>
///   Defines the contract for submitting, managing, and retrieving tasks and sessions in the ArmoniK system.
/// </summary>
/// <remarks>
///   This interface provides asynchronous methods for session and task lifecycle management,
///   result retrieval, and configuration queries.
/// </remarks>
public interface ISubmitter
{
  /// <summary>
  ///   Cancels the specified session and all associated tasks.
  /// </summary>
  /// <param name="sessionId">The ID of the session to cancel.</param>
  /// <param name="cancellationToken">Token to cancel the operation.</param>
  Task CancelSession(string            sessionId,
                     CancellationToken cancellationToken);

  /// <summary>
  ///   Creates a new session with the specified partition IDs and default task options.
  /// </summary>
  /// <param name="partitionIds">The list of partition IDs for the session.</param>
  /// <param name="defaultTaskOptions">The default options to use for tasks in this session.</param>
  /// <param name="cancellationToken">Token to cancel the operation.</param>
  /// <returns>A reply containing session creation details.</returns>
  Task<CreateSessionReply> CreateSession(IList<string>     partitionIds,
                                         TaskOptions       defaultTaskOptions,
                                         CancellationToken cancellationToken);

  /// <summary>
  ///   Creates multiple tasks in a session.
  /// </summary>
  /// <param name="sessionId">The ID of the session to add tasks to.</param>
  /// <param name="parentTaskId">The parent task ID, if any.</param>
  /// <param name="options">Optional task options to override the session defaults.</param>
  /// <param name="taskRequests">A stream of task requests containing payloads and dependencies.</param>
  /// <param name="cancellationToken">Token to cancel the operation.</param>
  /// <returns>A collection of task creation requests.</returns>
  Task<ICollection<TaskCreationRequest>> CreateTasks(string                        sessionId,
                                                     string                        parentTaskId,
                                                     TaskOptions?                  options,
                                                     IAsyncEnumerable<TaskRequest> taskRequests,
                                                     CancellationToken             cancellationToken);

  /// <summary>
  ///   Finalizes the creation of tasks after they have been submitted.
  /// </summary>
  /// <param name="requests">The collection of task creation requests to finalize.</param>
  /// <param name="sessionData">The session data associated with the tasks.</param>
  /// <param name="parentTaskId">The parent task ID</param>
  /// <param name="cancellationToken">Token to cancel the operation.</param>
  Task FinalizeTaskCreation(IEnumerable<TaskCreationRequest> requests,
                            SessionData                      sessionData,
                            string                           parentTaskId,
                            CancellationToken                cancellationToken);

  /// <summary>
  ///   Retrieves the service configuration.
  /// </summary>
  /// <param name="request">The configuration request (empty).</param>
  /// <param name="cancellationToken">Token to cancel the operation.</param>
  /// <returns>The current service configuration.</returns>
  Task<Configuration> GetServiceConfiguration(Empty             request,
                                              CancellationToken cancellationToken);

  /// <summary>
  ///   Attempts to retrieve the result for a given request and streams it to the client.
  /// </summary>
  /// <param name="request">The result request specifying the task and result key.</param>
  /// <param name="responseStream">The server stream writer to send result replies.</param>
  /// <param name="cancellationToken">Token to cancel the operation.</param>
  Task TryGetResult(ResultRequest                    request,
                    IServerStreamWriter<ResultReply> responseStream,
                    CancellationToken                cancellationToken);

  /// <summary>
  ///   Waits for the completion of the specified tasks or session.
  /// </summary>
  /// <param name="request">The wait request specifying which tasks or session to wait for.</param>
  /// <param name="cancellationToken">Token to cancel the operation.</param>
  /// <returns>A count indicating how many tasks have completed.</returns>
  Task<Count> WaitForCompletion(WaitRequest       request,
                                CancellationToken cancellationToken);

  /// <summary>
  ///   Marks a task as completed and optionally resubmits it if required.
  /// </summary>
  /// <param name="taskData">The task data to complete.</param>
  /// <param name="sessionData">The session data associated with the task.</param>
  /// <param name="resubmit">Whether to resubmit the task after completion.</param>
  /// <param name="output">The output of the completed task.</param>
  /// <param name="cancellationToken">Token to cancel the operation.</param>
  Task CompleteTaskAsync(TaskData          taskData,
                         SessionData       sessionData,
                         bool              resubmit,
                         Output            output,
                         CancellationToken cancellationToken = default);

  /// <summary>
  ///   Waits for the availability of a result for a given request.
  /// </summary>
  /// <param name="request">The result request specifying the task and result key.</param>
  /// <param name="contextCancellationToken">Token to cancel the operation.</param>
  /// <returns>A reply indicating the availability of the result.</returns>
  Task<AvailabilityReply> WaitForAvailabilityAsync(ResultRequest     request,
                                                   CancellationToken contextCancellationToken);

  /// <summary>
  ///   Sets the result for a given task and key by streaming the result chunks.
  /// </summary>
  /// <param name="sessionId">The session ID associated with the result.</param>
  /// <param name="ownerTaskId">The task ID that owns the result.</param>
  /// <param name="key">The result key.</param>
  /// <param name="chunks">A stream of result data chunks.</param>
  /// <param name="cancellationToken">Token to cancel the operation.</param>
  Task SetResult(string                                 sessionId,
                 string                                 ownerTaskId,
                 string                                 key,
                 IAsyncEnumerable<ReadOnlyMemory<byte>> chunks,
                 CancellationToken                      cancellationToken);
}
