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
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.StateMachines;

using Google.Protobuf;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Pollster;

public class ComputeRequestQueue
{

  private readonly ILogger                                    logger_;
  private readonly Queue<ProcessRequest.Types.ComputeRequest> computeRequests_;
  private readonly ComputeRequestStateMachine                 machine_;

  public ComputeRequestQueue(ILogger logger)
  {
    logger_          = logger;
    computeRequests_ = new Queue<ProcessRequest.Types.ComputeRequest>();
    machine_         = new ComputeRequestStateMachine(logger_);
  }

  public async Task Init(int dataChunkMaxSize, string sessionId, string taskId, IDictionary<string, string> taskOptions, ByteString? payload, IList<string> expectedOutputKeys)
  {
    await machine_.InitRequestAsync();
    computeRequests_.Enqueue(new()
    {
      InitRequest = new()
      {
        Configuration = new()
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

  public async Task AddPayloadChunk(ByteString chunk)
  {
    await machine_.AddPayloadChunkAsync();
    computeRequests_.Enqueue(new()
    {
      Payload = new()
      {
        Data = chunk,
      },
    });
  }

  public async Task CompletePayload()
  {
    await machine_.CompletePayloadAsync();
    computeRequests_.Enqueue(new()
    {
      Payload = new()
      {
        DataComplete = true,
      },
    });
  }

  public async Task InitDataDependency(string key)
  {
    await machine_.InitDataDependencyAsync();
    computeRequests_.Enqueue(new()
    {
      InitData = new()
      {
        Key = key,
      },
    });
  }


  public async Task AddDataDependencyChunk(ByteString chunk)
  {
    await machine_.AddDataDependencyChunkAsync();
    computeRequests_.Enqueue(new()
    {
      Data = new()
      {
        Data = chunk,
      },
    });
  }

  public async Task CompleteDataDependency()
  {
    await machine_.CompleteDataDependencyAsync();
    computeRequests_.Enqueue(new()
    {
      Data = new()
      {
        DataComplete = true,
      },
    });
  }

  public async Task<Queue<ProcessRequest.Types.ComputeRequest>> GetQueue()
  {
    await machine_.CompleteRequestAsync();
    computeRequests_.Enqueue(new()
    {
      InitData = new()
      {
        LastData = true,
      },
    });
    return computeRequests_;
  }
}
