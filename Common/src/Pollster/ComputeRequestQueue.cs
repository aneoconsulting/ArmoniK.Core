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
using System.Collections.Generic;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Worker;
using ArmoniK.Core.Common.StateMachines;

using Google.Protobuf;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Pollster;

/// <summary>
///   Queue to identify input data with a <see cref="ProcessRequest.Types.ComputeRequest" /> while verifying
///   the request ordering with state machines
/// </summary>
public class ComputeRequestQueue
{
  private readonly Queue<ProcessRequest.Types.ComputeRequest> computeRequests_;

  private readonly ILogger                    logger_;
  private readonly ComputeRequestStateMachine machine_;

  /// <summary>
  ///   Initializes a queue that stores <see cref="ProcessRequest.Types.ComputeRequest" />
  /// </summary>
  /// <param name="logger"></param>
  public ComputeRequestQueue(ILogger logger)
  {
    logger_          = logger;
    computeRequests_ = new Queue<ProcessRequest.Types.ComputeRequest>();
    machine_         = new ComputeRequestStateMachine(logger_);
  }

  /// <summary>
  ///   Create the init computation request with the given parameters
  /// </summary>
  /// <param name="dataChunkMaxSize">The maximum size of a data chunk</param>
  /// <param name="sessionId">The session identifier</param>
  /// <param name="taskId">The task identifier</param>
  /// <param name="taskOptions">The options of the task</param>
  /// <param name="payload">The input data of the task</param>
  /// <param name="expectedOutputKeys">Collection of data ids for the expected outputs of the task</param>
  /// <exception cref="InvalidOperationException">Invalid request according to the state machine</exception>
  public void Init(int           dataChunkMaxSize,
                   string        sessionId,
                   string        taskId,
                   TaskOptions   taskOptions,
                   ByteString?   payload,
                   IList<string> expectedOutputKeys)
  {
    machine_.InitRequest();
    computeRequests_.Enqueue(new ProcessRequest.Types.ComputeRequest
                             {
                               InitRequest = new ProcessRequest.Types.ComputeRequest.Types.InitRequest
                                             {
                                               Configuration = new Configuration
                                                               {
                                                                 DataChunkMaxSize = dataChunkMaxSize,
                                                               },
                                               TaskId      = taskId,
                                               SessionId   = sessionId,
                                               TaskOptions = taskOptions,
                                               Payload = payload is not null
                                                           ? new DataChunk
                                                             {
                                                               Data = payload,
                                                             }
                                                           : new DataChunk(),
                                               ExpectedOutputKeys =
                                               {
                                                 expectedOutputKeys,
                                               },
                                             },
                             });
  }

  /// <summary>
  ///   Add the given payload chunk to the request queue
  /// </summary>
  /// <param name="chunk">Data chunk</param>
  /// <exception cref="InvalidOperationException">Invalid request according to the state machine</exception>
  public void AddPayloadChunk(ByteString chunk)
  {
    machine_.AddPayloadChunk();
    computeRequests_.Enqueue(new ProcessRequest.Types.ComputeRequest
                             {
                               Payload = new DataChunk
                                         {
                                           Data = chunk,
                                         },
                             });
  }

  /// <summary>
  ///   Add a request representing the end of the payload in the queue
  /// </summary>
  /// <exception cref="InvalidOperationException">Invalid request according to the state machine</exception>
  public void CompletePayload()
  {
    machine_.CompletePayload();
    computeRequests_.Enqueue(new ProcessRequest.Types.ComputeRequest
                             {
                               Payload = new DataChunk
                                         {
                                           DataComplete = true,
                                         },
                             });
  }

  /// <summary>
  ///   Add a request representing the start of a data dependency in the queue
  /// </summary>
  /// <param name="key">The identifier of the data dependency</param>
  /// <exception cref="InvalidOperationException">Invalid request according to the state machine</exception>
  public void InitDataDependency(string key)
  {
    machine_.InitDataDependency();
    computeRequests_.Enqueue(new ProcessRequest.Types.ComputeRequest
                             {
                               InitData = new ProcessRequest.Types.ComputeRequest.Types.InitData
                                          {
                                            Key = key,
                                          },
                             });
  }

  /// <summary>
  ///   Add a request containing the given data chunk for the data dependency in the queue
  /// </summary>
  /// <param name="chunk">Data chunk</param>
  /// <exception cref="InvalidOperationException">Invalid request according to the state machine</exception>
  public void AddDataDependencyChunk(ByteString chunk)
  {
    machine_.AddDataDependencyChunk();
    computeRequests_.Enqueue(new ProcessRequest.Types.ComputeRequest
                             {
                               Data = new DataChunk
                                      {
                                        Data = chunk,
                                      },
                             });
  }

  /// <summary>
  ///   Add a request representing the end of the data dependency in the queue
  /// </summary>
  /// <exception cref="InvalidOperationException">Invalid request according to the state machine</exception>
  public void CompleteDataDependency()
  {
    machine_.CompleteDataDependency();
    computeRequests_.Enqueue(new ProcessRequest.Types.ComputeRequest
                             {
                               Data = new DataChunk
                                      {
                                        DataComplete = true,
                                      },
                             });
  }

  /// <summary>
  ///   Get the queue with the complete compute request
  /// </summary>
  /// <returns>
  ///   The queue with the complete compute request
  /// </returns>
  /// <exception cref="InvalidOperationException">Invalid request according to the state machine</exception>
  public Queue<ProcessRequest.Types.ComputeRequest> GetQueue()
  {
    machine_.CompleteRequest();
    computeRequests_.Enqueue(new ProcessRequest.Types.ComputeRequest
                             {
                               InitData = new ProcessRequest.Types.ComputeRequest.Types.InitData
                                          {
                                            LastData = true,
                                          },
                             });
    return computeRequests_;
  }
}
