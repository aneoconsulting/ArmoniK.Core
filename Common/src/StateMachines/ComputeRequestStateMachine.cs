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
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;

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

  public ComputeRequestStateMachine(ILogger logger)
  {
    logger_  = logger;
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

  public async Task InitRequestAsync() =>
    await machine_.FireAsync(Triggers.InitRequest);

  public async Task AddPayloadChunkAsync() =>
    await machine_.FireAsync(Triggers.AddPayloadChunk);

  public async Task CompletePayloadAsync() =>
    await machine_.FireAsync(Triggers.CompletePayload);

  public async Task InitDataDependencyAsync() =>
    await machine_.FireAsync(Triggers.InitDataDependency);

  public async Task AddDataDependencyChunkAsync() =>
    await machine_.FireAsync(Triggers.AddDataDependencyChunk);

  public async Task CompleteDataDependencyAsync() => await machine_.FireAsync(Triggers.CompleteDataDependency);

  public async Task CompleteRequestAsync() => await machine_.FireAsync(Triggers.CompleteRequest);

  public string GenerateGraph() =>
    UmlDotGraph.Format(machine_.GetInfo());

  public State GetState() => machine_.State;
}
