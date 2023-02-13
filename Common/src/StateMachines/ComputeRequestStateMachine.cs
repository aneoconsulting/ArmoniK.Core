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
using System.Linq;
using System.Text;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Worker;

using Microsoft.Extensions.Logging;

using Stateless;
using Stateless.Graph;

namespace ArmoniK.Core.Common.StateMachines;

/// <summary>
///   Utility class for the Final State Machine from <see cref="ProcessRequest.Types.ComputeRequest" />
/// </summary>
public class ComputeRequestStateMachine
{
  /// <summary>
  ///   States for the Final State Machine
  /// </summary>
  public enum State
  {
    /// <summary>
    ///   Initial state of the Final State Machine.
    /// </summary>
    Init,

    /// <summary>
    ///   State after receiving <see cref="Triggers.InitRequest" />.
    /// </summary>
    InitRequest,

    /// <summary>
    ///   State when receiving payload chunk.
    /// </summary>
    PayloadData,

    /// <summary>
    ///   State after reception of the last payload chunk.
    /// </summary>
    PayloadComplete,

    /// <summary>
    ///   State for initiating the reception of the data for a data dependency.
    /// </summary>
    DataInit,

    /// <summary>
    ///   State when receiving the last data chunk of the given data dependency.
    /// </summary>
    DataComplete,

    /// <summary>
    ///   State when receiving a data chunk.
    /// </summary>
    Data,

    /// <summary>
    ///   State when receiving the last data dependency.
    /// </summary>
    DataLast,
  }

  /// <summary>
  ///   Transitions for the Final State Machine
  /// </summary>
  public enum Triggers
  {
    /// <summary>
    ///   Correspond to receive last request <see cref="ProcessRequest.Types.ComputeRequest.TypeOneofCase.InitData" />
    /// </summary>
    CompleteRequest,

    /// <summary>
    ///   Correspond to receive request <see cref="DataChunk.TypeOneofCase.DataComplete" /> as
    ///   <see cref="ProcessRequest.Types.ComputeRequest.TypeOneofCase.Data" />
    /// </summary>
    CompleteDataDependency,

    /// <summary>
    ///   Correspond to receive request <see cref="DataChunk.TypeOneofCase.Data" /> as
    ///   <see cref="ProcessRequest.Types.ComputeRequest.TypeOneofCase.Data" />
    /// </summary>
    AddDataDependencyChunk,

    /// <summary>
    ///   Correspond to receive request <see cref="ProcessRequest.Types.ComputeRequest.TypeOneofCase.InitData" />
    /// </summary>
    InitDataDependency,

    /// <summary>
    ///   Correspond to receive request <see cref="DataChunk.TypeOneofCase.DataComplete" /> as
    ///   <see cref="ProcessRequest.Types.ComputeRequest.TypeOneofCase.Payload" />
    /// </summary>
    CompletePayload,

    /// <summary>
    ///   Correspond to receive request <see cref="DataChunk.TypeOneofCase.Data" /> as
    ///   <see cref="ProcessRequest.Types.ComputeRequest.TypeOneofCase.Payload" />
    /// </summary>
    AddPayloadChunk,

    /// <summary>
    ///   Correspond to receive request <see cref="ProcessRequest.Types.ComputeRequest.TypeOneofCase.InitRequest" />
    /// </summary>
    InitRequest,
  }

  private readonly ILogger logger_;

  private readonly StateMachine<State, Triggers> machine_;

  /// <summary>
  ///   Constructor that initializes the Final State Machine
  /// </summary>
  /// <param name="logger">Logger used to produce logs for this class</param>
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
    {
      machine_.OnTransitioned(t => logger_.LogDebug("OnTransitioned {FSM}: {Source} -> {Destination}",
                                                    nameof(ComputeRequestStateMachine),
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
  ///   Function used when using <see cref="Triggers.AddPayloadChunk" /> transition
  /// </summary>
  /// <exception cref="InvalidOperationException">Invalid transition</exception>
  public void AddPayloadChunk()
    => machine_.Fire(Triggers.AddPayloadChunk);

  /// <summary>
  ///   Function used when using <see cref="Triggers.CompletePayload" /> transition
  /// </summary>
  /// <exception cref="InvalidOperationException">Invalid transition</exception>
  public void CompletePayload()
    => machine_.Fire(Triggers.CompletePayload);

  /// <summary>
  ///   Function used when using <see cref="Triggers.InitDataDependency" /> transition
  /// </summary>
  /// <exception cref="InvalidOperationException">Invalid transition</exception>
  public void InitDataDependency()
    => machine_.Fire(Triggers.InitDataDependency);

  /// <summary>
  ///   Function used when using <see cref="Triggers.AddDataDependencyChunk" /> transition
  /// </summary>
  /// <exception cref="InvalidOperationException">Invalid transition</exception>
  public void AddDataDependencyChunk()
    => machine_.Fire(Triggers.AddDataDependencyChunk);

  /// <summary>
  ///   Function used when using <see cref="Triggers.CompleteDataDependency" /> transition
  /// </summary>
  /// <exception cref="InvalidOperationException">Invalid transition</exception>
  public void CompleteDataDependency()
    => machine_.Fire(Triggers.CompleteDataDependency);

  /// <summary>
  ///   Function used when using <see cref="Triggers.CompleteRequest" /> transition
  /// </summary>
  /// <exception cref="InvalidOperationException">Invalid transition</exception>
  public void CompleteRequest()
    => machine_.Fire(Triggers.CompleteRequest);

  /// <summary>
  ///   Generate a dot graph representing the Final State Machine
  /// </summary>
  /// <returns>
  ///   A string containing the graph in dot format
  /// </returns>
  public string GenerateGraph()
    => UmlDotGraph.Format(machine_.GetInfo());

  /// <summary>
  ///   Generate a Mermaid graph representing the Final State Machine
  /// </summary>
  /// <returns>
  ///   A string containing the graph in Mermaid format
  /// </returns>
  public string GenerateMermaidGraph()
  {
    var str = UmlMermaidGraph.Format(machine_.GetInfo());

    // Manually fix the footer; the last
    // 3 lines should be disposed
    var lines = str.Split(new[]
                          {
                            Environment.NewLine,
                          },
                          StringSplitOptions.None);
    str = string.Join(Environment.NewLine,
                      lines.Take(lines.Length - 3));

    // Enclose in markers for markdown
    var bld = new StringBuilder(str);
    bld.Insert(0,
               "```mermaid\n");
    bld.Append("\n```\n");

    return bld.ToString();
  }

  /// <summary>
  ///   Get the current state of the Final State Machine
  /// </summary>
  /// <returns>
  ///   The current state of the Final State Machine
  /// </returns>
  public State GetState()
    => machine_.State;
}
