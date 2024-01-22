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

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Agent;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Storage;

namespace ArmoniK.Core.Common.gRPC.Services;

/// <summary>
///   Interface for implementing methods for the agent gRPC service that receives requests from the worker
/// </summary>
public interface IAgent : IDisposable
{
  /// <summary>
  ///   Unique token to identify agent and requests it should process
  /// </summary>
  string Token { get; }

  /// <summary>
  ///   Folder in which data are sent between agent and worker
  /// </summary>
  string Folder { get; }

  /// <summary>
  ///   Id of the session from the task associated to the agent
  /// </summary>
  string SessionId { get; }

  /// <summary>
  ///   Finalize child task creation after the parent task succeeds
  /// </summary>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  Task FinalizeTaskCreation(CancellationToken cancellationToken);

  /// <summary>
  ///   Get Common data from data storage as file in shared folder
  /// </summary>
  /// <param name="token">Worker token for request validation</param>
  /// <param name="resultId">Result id to retrieve</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Id of the result sent to the worker
  /// </returns>
  Task<string> GetCommonData(string            token,
                             string            resultId,
                             CancellationToken cancellationToken);

  /// <summary>
  ///   Get Direct data from user as file in shared folder
  /// </summary>
  /// <param name="token">Worker token for request validation</param>
  /// <param name="resultId">Result id to retrieve</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Id of the result sent to the worker
  /// </returns>
  Task<string> GetDirectData(string            token,
                             string            resultId,
                             CancellationToken cancellationToken);

  /// <summary>
  ///   Get Resource data from data storage as file in shared folder
  /// </summary>
  /// <param name="token">Worker token for request validation</param>
  /// <param name="resultId">Result id to retrieve</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Id of the result sent to the worker
  /// </returns>
  Task<string> GetResourceData(string            token,
                               string            resultId,
                               CancellationToken cancellationToken);

  /// <summary>
  ///   Create results metadata
  /// </summary>
  /// <param name="request">Requests containing the results to create</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Reply sent to the worker with the created results
  /// </returns>
  Task<CreateResultsMetaDataResponse> CreateResultsMetaData(CreateResultsMetaDataRequest request,
                                                            CancellationToken            cancellationToken);

  /// <summary>
  ///   Submit tasks with payload already existing
  /// </summary>
  /// <param name="requests">Requests containing the tasks to submit</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Submitted tasks to send to the worker
  /// </returns>
  Task<ICollection<TaskCreationRequest>> SubmitTasks(ICollection<TaskSubmissionRequest> requests,
                                                     TaskOptions?                       taskOptions,
                                                     string                             sessionId,
                                                     string                             token,
                                                     CancellationToken                  cancellationToken);

  /// <summary>
  ///   Create a result (with data and metadata)
  /// </summary>
  /// <param name="request">Requests containing the result to create and the data</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Reply sent to the worker with the id of the created result
  /// </returns>
  Task<CreateResultsResponse> CreateResults(CreateResultsRequest request,
                                            CancellationToken    cancellationToken);

  /// <summary>
  ///   Put the results created as a file in the task into object storage
  /// </summary>
  /// <param name="token">Worker token for request validation</param>
  /// <param name="resultIds">Results to put in the object storage</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Results which notification is successful
  /// </returns>
  Task<ICollection<string>> NotifyResultData(string              token,
                                             ICollection<string> resultIds,
                                             CancellationToken   cancellationToken);

  /// <summary>
  ///   Cancel child tasks created by the current task in processing
  /// </summary>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  Task CancelChildTasks(CancellationToken cancellationToken);
}
