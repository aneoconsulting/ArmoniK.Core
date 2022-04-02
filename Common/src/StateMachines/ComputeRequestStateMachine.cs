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
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Exceptions;

using Microsoft.Extensions.Logging;

using Stateless;

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
    ReceiveRequest,
  }

  private readonly StateMachine<State, Triggers>                                                            machine_;
  private readonly StateMachine<State, Triggers>.TriggerWithParameters<ProcessRequest.Types.ComputeRequest> changedNeededParameters_;
  private readonly ILogger<ComputeRequestStateMachine>                                                      logger_;

  public ComputeRequestStateMachine(ILogger<ComputeRequestStateMachine> logger)
  {
    logger_  = logger;
    machine_ = new StateMachine<State, Triggers>(State.Init);

    changedNeededParameters_ = machine_.SetTriggerParameters<ProcessRequest.Types.ComputeRequest>(Triggers.ReceiveRequest);

    machine_.Configure(State.Init)
            .PermitIf(changedNeededParameters_,
                      State.InitRequest,
                      request => request.TypeCase == ProcessRequest.Types.ComputeRequest.TypeOneofCase.InitRequest,
                      "Transition from Init to InitRequest");

    machine_.Configure(State.InitRequest)
            .PermitIf(changedNeededParameters_,
                      State.PayloadData,
                      request => request.TypeCase == ProcessRequest.Types.ComputeRequest.TypeOneofCase.Payload &&
                                 request.Payload.TypeCase == DataChunk.TypeOneofCase.Data)
            .PermitIf(changedNeededParameters_,
                      State.PayloadComplete,
                      request => request.TypeCase == ProcessRequest.Types.ComputeRequest.TypeOneofCase.Payload &&
                                 request.Payload.TypeCase == DataChunk.TypeOneofCase.DataComplete &&
                                 request.Payload.DataComplete);

    machine_.Configure(State.PayloadData)
            .PermitReentryIf(changedNeededParameters_,
                             request => request.TypeCase == ProcessRequest.Types.ComputeRequest.TypeOneofCase.Payload &&
                                        request.Payload.TypeCase == DataChunk.TypeOneofCase.Data)
            .PermitIf(changedNeededParameters_,
                      State.PayloadComplete,
                      request => request.TypeCase == ProcessRequest.Types.ComputeRequest.TypeOneofCase.Payload &&
                                 request.Payload.TypeCase == DataChunk.TypeOneofCase.DataComplete &&
                                 request.Payload.DataComplete);

    machine_.Configure(State.PayloadComplete)
            .PermitIf(changedNeededParameters_,
                      State.DataInit,
                      request => request.TypeCase == ProcessRequest.Types.ComputeRequest.TypeOneofCase.InitData &&
                                 request.InitData.TypeCase == ProcessRequest.Types.ComputeRequest.Types.InitData.TypeOneofCase.Key);

    machine_.Configure(State.DataInit)
            .PermitIf(changedNeededParameters_,
                      State.Data,
                      request => request.TypeCase == ProcessRequest.Types.ComputeRequest.TypeOneofCase.Data &&
                                 request.Data.TypeCase == DataChunk.TypeOneofCase.Data)
            .PermitIf(changedNeededParameters_,
                      State.DataComplete,
                      request => request.TypeCase == ProcessRequest.Types.ComputeRequest.TypeOneofCase.Data &&
                                 request.Data.TypeCase == DataChunk.TypeOneofCase.DataComplete &&
                                 request.Data.DataComplete);

    machine_.Configure(State.Data)
            .PermitReentryIf(changedNeededParameters_,
                             request => request.TypeCase == ProcessRequest.Types.ComputeRequest.TypeOneofCase.Data &&
                                        request.Data.TypeCase == DataChunk.TypeOneofCase.Data)
            .PermitIf(changedNeededParameters_,
                      State.DataComplete,
                      request => request.TypeCase == ProcessRequest.Types.ComputeRequest.TypeOneofCase.Data &&
                                 request.Data.TypeCase == DataChunk.TypeOneofCase.DataComplete &&
                                 request.Data.DataComplete);

    machine_.Configure(State.DataComplete)
            .PermitIf(changedNeededParameters_,
                      State.DataLast,
                      request => request.TypeCase == ProcessRequest.Types.ComputeRequest.TypeOneofCase.InitData &&
                                 request.InitData.TypeCase == ProcessRequest.Types.ComputeRequest.Types.InitData.TypeOneofCase.LastData &&
                                 request.InitData.LastData)
            .PermitIf(changedNeededParameters_,
                      State.DataInit,
                      request => request.TypeCase == ProcessRequest.Types.ComputeRequest.TypeOneofCase.InitData &&
                                 request.InitData.TypeCase == ProcessRequest.Types.ComputeRequest.Types.InitData.TypeOneofCase.Key);

    if (logger_.IsEnabled(LogLevel.Debug))
      machine_.OnTransitioned(t => logger_.LogDebug("OnTransitioned: {Source} -> {Destination}", t.Source, t.Destination));
  }

  public async Task ReceiveRequest(ProcessRequest.Types.ComputeRequest request) => await machine_.FireAsync(changedNeededParameters_,
                                                                                                             request);

  public void Register(State state, Func<StateMachine<State, Triggers>.Transition, Task> func) => machine_.Configure(state).OnEntryAsync(func);

}
