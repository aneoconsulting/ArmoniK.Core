// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Agent;

using Grpc.Core;

namespace ArmoniK.Core.Common.gRPC.Services;

/// <summary>
///   Interface for implementing methods for the agent gRPC service that receives requests from the worker
/// </summary>
public interface IAgent : IDisposable
{
  /// <summary>
  ///   Finalize child task creation after the parent task succeeds
  /// </summary>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  Task FinalizeTaskCreation(CancellationToken cancellationToken);

  /// <summary>
  ///   Process requests for creating child tasks
  /// </summary>
  /// <param name="requestStream">Collection of requests that represents the child tasks</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Reply sent to the worker describing the status of the child task creation
  /// </returns>
  Task<CreateTaskReply> CreateTask(IAsyncStreamReader<CreateTaskRequest> requestStream,
                                   CancellationToken                     cancellationToken);

  /// <summary>
  ///   Get Common data from data storage as file in shared folder
  /// </summary>
  /// <param name="request">Request specifying the data to retrieve</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Response to send to the worker
  /// </returns>
  Task<DataResponse> GetCommonData(DataRequest       request,
                                   CancellationToken cancellationToken);

  /// <summary>
  ///   Get Direct data from user as file in shared folder
  /// </summary>
  /// <param name="request">Request specifying the data to retrieve</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Response to send to the worker
  /// </returns>
  Task<DataResponse> GetDirectData(DataRequest       request,
                                   CancellationToken cancellationToken);

  /// <summary>
  ///   Get Resource data from data storage as file in shared folder
  /// </summary>
  /// <param name="request">Request specifying the data to retrieve</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Response to send to the worker
  /// </returns>
  Task<DataResponse> GetResourceData(DataRequest       request,
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
  /// <param name="request">Requests containing the tasks to submit</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Reply sent to the worker with the submitted tasks
  /// </returns>
  Task<SubmitTasksResponse> SubmitTasks(SubmitTasksRequest request,
                                        CancellationToken  cancellationToken);

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
  /// <param name="request">Requests containing the results</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Reply sent to the worker describing the status of the execution of the received requests
  /// </returns>
  Task<NotifyResultDataResponse> NotifyResultData(NotifyResultDataRequest request,
                                                  CancellationToken       cancellationToken);
}
