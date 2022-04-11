// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
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
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System.Collections.Generic;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.StateMachines;

using Google.Protobuf;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Pollster;

public class ComputeRequestQueue
{
  private readonly Queue<ProcessRequest.Types.ComputeRequest> computeRequests_;

  private readonly ILogger                    logger_;
  private readonly ComputeRequestStateMachine machine_;

  public ComputeRequestQueue(ILogger logger)
  {
    logger_          = logger;
    computeRequests_ = new Queue<ProcessRequest.Types.ComputeRequest>();
    machine_         = new ComputeRequestStateMachine(logger_);
  }

  public void Init(int                         dataChunkMaxSize,
                   string                      sessionId,
                   string                      taskId,
                   IDictionary<string, string> taskOptions,
                   ByteString?                 payload,
                   IList<string>               expectedOutputKeys)
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
                                               TaskId    = taskId,
                                               SessionId = sessionId,
                                               TaskOptions =
                                               {
                                                 taskOptions,
                                               },
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
