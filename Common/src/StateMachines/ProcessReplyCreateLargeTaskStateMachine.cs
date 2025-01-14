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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;

using Microsoft.Extensions.Logging;

using Stateless;
using Stateless.Graph;

namespace ArmoniK.Core.Common.StateMachines;

/// <summary>
///   Utility class for the Final State Machine from <see cref="CreateLargeTaskRequest" />
/// </summary>
public class ProcessReplyCreateLargeTaskStateMachine
{
  /// <summary>
  ///   States for the Final State Machine
  /// </summary>
  public enum State
  {
    /// <summary>
    ///   Initial state of the Final State Machine
    /// </summary>
    Init,

    /// <summary>
    ///   State after triggering <see cref="Triggers.InitRequest" />
    /// </summary>
    InitRequest,

    /// <summary>
    ///   State after triggering <see cref="Triggers.AddHeader" />
    /// </summary>
    TaskRequestHeader,

    /// <summary>
    ///   State after triggering <see cref="Triggers.AddDataChunk" />
    /// </summary>
    DataChunk,

    /// <summary>
    ///   State after triggering <see cref="Triggers.CompleteData" />
    /// </summary>
    DataChunkComplete,

    /// <summary>
    ///   State after triggering <see cref="Triggers.CompleteRequest" />
    /// </summary>
    InitTaskRequestLast,
  }

  /// <summary>
  ///   Transitions for the Final State Machine
  /// </summary>
  public enum Triggers
  {
    /// <summary>
    ///   Correspond to receive request <see cref="CreateLargeTaskRequest.TypeOneofCase.InitRequest" />
    /// </summary>
    InitRequest,

    /// <summary>
    ///   Correspond to receive request <see cref="InitTaskRequest.TypeOneofCase.Header" />
    /// </summary>
    AddHeader,

    /// <summary>
    ///   Correspond to receive request <see cref="DataChunk.TypeOneofCase.Data" />
    /// </summary>
    AddDataChunk,

    /// <summary>
    ///   Correspond to receive request <see cref="DataChunk.TypeOneofCase.DataComplete" />
    /// </summary>
    CompleteData,

    /// <summary>
    ///   Correspond to receive request <see cref="InitTaskRequest.TypeOneofCase.LastTask" />
    /// </summary>
    CompleteRequest,
  }

  private readonly ILogger logger_;

  private readonly StateMachine<State, Triggers> machine_;

  /// <summary>
  ///   Constructor that initializes the Final State Machine
  /// </summary>
  /// <param name="logger">Logger used to produce logs for this class</param>
  public ProcessReplyCreateLargeTaskStateMachine(ILogger logger)
  {
    logger_  = logger;
    machine_ = new StateMachine<State, Triggers>(State.Init);

    machine_.Configure(State.Init)
            .Permit(Triggers.InitRequest,
                    State.InitRequest);

    machine_.Configure(State.InitRequest)
            .Permit(Triggers.AddHeader,
                    State.TaskRequestHeader);

    machine_.Configure(State.TaskRequestHeader)
            .Permit(Triggers.AddDataChunk,
                    State.DataChunk);

    machine_.Configure(State.DataChunk)
            .PermitReentry(Triggers.AddDataChunk)
            .Permit(Triggers.CompleteData,
                    State.DataChunkComplete);

    machine_.Configure(State.DataChunkComplete)
            .Permit(Triggers.AddHeader,
                    State.TaskRequestHeader)
            .Permit(Triggers.CompleteRequest,
                    State.InitTaskRequestLast);

    if (logger_.IsEnabled(LogLevel.Debug))
    {
      machine_.OnTransitioned(t => logger_.LogDebug("OnTransitioned {FSM}: {Source} -> {Destination}",
                                                    nameof(ProcessReplyCreateLargeTaskStateMachine),
                                                    t.Source,
                                                    t.Destination));
    }
  }

  /// <summary>
  ///   Function used when using <see cref="Triggers.InitRequest" /> transition
  /// </summary>
  /// <exception cref="InvalidOperationException">Invalid transition</exception>
  public void InitRequest()
    => machine_.Fire(Triggers.InitRequest);

  /// <summary>
  ///   Function used when using <see cref="Triggers.AddHeader" /> transition
  /// </summary>
  /// <exception cref="InvalidOperationException">Invalid transition</exception>
  public void AddHeader()
    => machine_.Fire(Triggers.AddHeader);

  /// <summary>
  ///   Function used when using <see cref="Triggers.AddDataChunk" /> transition
  /// </summary>
  /// <exception cref="InvalidOperationException">Invalid transition</exception>
  public void AddDataChunk()
    => machine_.Fire(Triggers.AddDataChunk);

  /// <summary>
  ///   Function used when using <see cref="Triggers.CompleteData" /> transition
  /// </summary>
  /// <exception cref="InvalidOperationException">Invalid transition</exception>
  public void CompleteData()
    => machine_.Fire(Triggers.CompleteData);

  /// <summary>
  ///   Function used when using <see cref="Triggers.CompleteRequest" /> transition
  /// </summary>
  /// <exception cref="InvalidOperationException">Invalid transition</exception>
  public void CompleteRequest()
    => machine_.Fire(Triggers.CompleteRequest);

  /// <summary>
  ///   Check if the Final State Machine is in its completed state
  /// </summary>
  /// <returns>
  ///   A bool representing if the final state machine is in a completed state
  /// </returns>
  public bool IsComplete()
    => machine_.State == State.InitTaskRequestLast;

  /// <summary>
  ///   Generate a dot graph representing the Final State Machine
  /// </summary>
  /// <returns>
  ///   A string containing the graph in dot format
  /// </returns>
  public string GenerateGraph()
    => UmlDotGraph.Format(machine_.GetInfo());

  /// <summary>
  ///   Get the current state of the Final State Machine
  /// </summary>
  /// <returns>
  ///   The current state of the Final State Machine
  /// </returns>
  public State GetState()
    => machine_.State;
}
