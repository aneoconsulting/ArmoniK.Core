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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;

using Google.Protobuf;

using Microsoft.Extensions.Logging;

using Stateless;
using Stateless.Graph;

namespace ArmoniK.Core.Common.StateMachines;

public class ComputeRequestStateMachine
{
  public enum State
  {
    Init,
    InitRequest,
    PayloadData,
    PayloadComplete,
    DataInit,
    DataComplete,
    Data,
    DataLast,
  }

  public enum Triggers
  {
    CompleteRequest,
    CompleteDataDependency,
    AddDataDependencyChunk,
    InitDataDependency,
    CompletePayload,
    AddPayloadChunk,
    InitRequest,
  }

  private readonly StateMachine<State, Triggers>              machine_;
  private readonly ILogger                                    logger_;
  private readonly Queue<ProcessRequest.Types.ComputeRequest> computeRequests_;

  public ComputeRequestStateMachine(ILogger logger)
  {
    logger_  = logger;
    computeRequests_ = new Queue<ProcessRequest.Types.ComputeRequest>();
    machine_ = new StateMachine<State, Triggers>(State.Init);

    machine_.Configure(State.Init)
            .Permit(Triggers.InitRequest,
                      State.InitRequest);

    machine_.Configure(State.InitRequest)
            .Permit(Triggers.AddPayloadChunk,
                      State.PayloadData)
            .Permit(Triggers.CompletePayload,
                      State.PayloadComplete);

    machine_.Configure(State.PayloadData)
            .PermitReentry(Triggers.AddPayloadChunk)
            .Permit(Triggers.CompletePayload,
                      State.PayloadComplete);

    machine_.Configure(State.PayloadComplete)
            .Permit(Triggers.InitDataDependency,
                      State.DataInit)
            .Permit(Triggers.CompleteRequest,
                    State.DataLast);

    machine_.Configure(State.DataInit)
            .Permit(Triggers.AddDataDependencyChunk,
                      State.Data);

    machine_.Configure(State.Data)
            .PermitReentry(Triggers.AddDataDependencyChunk)
            .Permit(Triggers.CompleteDataDependency,
                      State.DataComplete);

    machine_.Configure(State.DataComplete)
            .Permit(Triggers.CompleteRequest,
                      State.DataLast)
            .Permit(Triggers.InitDataDependency,
                      State.DataInit);

    if (logger_.IsEnabled(LogLevel.Debug))
      machine_.OnTransitioned(t => logger_.LogDebug("OnTransitioned: {Source} -> {Destination}",
                                                    t.Source,
                                                    t.Destination));
  }

  public void Register(State state, Func<ProcessRequest.Types.ComputeRequest, Task> func) => machine_.Configure(state)
                                                                                                     .OnEntryAsync(transition => func(transition.Parameters.Single() as
                                                                                                                                        ProcessRequest.Types.
                                                                                                                                        ComputeRequest ??
                                                                                                                                      throw new
                                                                                                                                        InvalidOperationException()));

  public void Init(int dataChunkMaxSize, string sessionId, string taskId, IDictionary<string, string> taskOptions, ByteString? payload, IList<string> expectedOutputKeys)
  {
    machine_.Fire(Triggers.InitRequest);
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

  public void AddPayloadChunk(ByteString chunk)
  {
    machine_.Fire(Triggers.AddPayloadChunk);
    computeRequests_.Enqueue(new()
    {
      Payload = new()
      {
        Data = chunk,
      },
    });
  }

  public void CompletePayload()
  {
    machine_.Fire(Triggers.CompletePayload);
    computeRequests_.Enqueue(new()
    {
      Payload = new()
      {
        DataComplete = true,
      },
    });
  }

  public void InitDataDependency(string key)
  {
    machine_.Fire(Triggers.InitDataDependency);
    computeRequests_.Enqueue(new()
    {
      InitData = new()
      {
        Key = key,
      },
    });
  }


  public void AddDataDependencyChunk(ByteString chunk)
  {
    machine_.Fire(Triggers.AddDataDependencyChunk);
    computeRequests_.Enqueue(new()
    {
      Data = new()
      {
        Data = chunk,
      },
    });
  }

  public void CompleteDataDependency()
  {
    machine_.Fire(Triggers.CompleteDataDependency);
    computeRequests_.Enqueue(new()
    {
      Data = new()
      {
        DataComplete = true,
      },
    });
  }

  public Queue<ProcessRequest.Types.ComputeRequest> GetQueue()
  {
    machine_.Fire(Triggers.CompleteRequest);
    computeRequests_.Enqueue(new()
    {
      InitData = new()
      {
        LastData = true,
      },
    });
    return computeRequests_;
  }

  public string GenerateGraph() =>
    UmlDotGraph.Format(machine_.GetInfo());

  public State GetState() => machine_.State;
}
