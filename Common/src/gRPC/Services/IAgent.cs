// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-$CURRENT_YEAR$. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY

using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Agent;

using Grpc.Core;

using Result = ArmoniK.Api.gRPC.V1.Agent.Result;

namespace ArmoniK.Core.Common.gRPC.Services;

/// <summary>
/// Interface for implementing methods for the agent gRPC service that receives requests from the worker
/// </summary>
public interface IAgent
{
  /// <summary>
  /// Finalize child task creation after the parent task succeeds
  /// </summary>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// Task representing the asynchronous execution of the method
  /// </returns>
  Task FinalizeTaskCreation(CancellationToken cancellationToken);

  /// <summary>
  /// Process requests for creating child tasks
  /// </summary>
  /// <param name="requestStream">Collection of requests that represents the child tasks</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// Reply sent to the worker describing the status of the child task creation
  /// </returns>
  Task<CreateTaskReply> CreateTask(IAsyncStreamReader<CreateTaskRequest> requestStream,
                                   CancellationToken                     cancellationToken);

  /// <summary>
  /// Get Common data from data storage
  /// </summary>
  /// <param name="request">Request specifying the data to retrieve</param>
  /// <param name="responseStream">Response containing the data that will be sent to the worker</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// Task representing the asynchronous execution of the method
  /// </returns>
  Task GetCommonData(DataRequest                    request,
                     IServerStreamWriter<DataReply> responseStream,
                     CancellationToken              cancellationToken);

  /// <summary>
  /// Get Direct data from user
  /// </summary>
  /// <param name="request">Request specifying the data to retrieve</param>
  /// <param name="responseStream">Response containing the data that will be sent to the worker</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// Task representing the asynchronous execution of the method
  /// </returns>
  Task GetDirectData(DataRequest                    request,
                     IServerStreamWriter<DataReply> responseStream,
                     CancellationToken              cancellationToken);

  /// <summary>
  /// Get Resource data from data storage
  /// </summary>
  /// <param name="request">Request specifying the data to retrieve</param>
  /// <param name="responseStream">Response containing the data that will be sent to the worker</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// Task representing the asynchronous execution of the method
  /// </returns>
  Task GetResourceData(DataRequest                    request,
                       IServerStreamWriter<DataReply> responseStream,
                       CancellationToken              cancellationToken);

  /// <summary>
  /// Put the results created in the task into data storage and mark them as available in data table
  /// </summary>
  /// <param name="requestStream">Requests containing the results</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// Reply sent to the worker describing the status of the execution of the received requests
  /// </returns>
  Task<ResultReply> SendResult(IAsyncStreamReader<Result> requestStream,
                               CancellationToken          cancellationToken);
}