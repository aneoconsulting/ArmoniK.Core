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
using System.Text;

using ArmoniK.Api.gRPC.V1;

using Microsoft.Extensions.Logging;

using Stateless;
using Stateless.Graph;

namespace ArmoniK.Core.Common.StateMachines;

/// <summary>
/// Utility class for the Final State Machine from <see cref="ProcessReply.Types.Result"/>
/// </summary>
public class ProcessReplyResultStateMachine
{
  /// <summary>
  /// States for the Final State Machine
  /// </summary>
  public enum State
  {
    /// <summary>
    /// Initial state of the Final State Machine
    /// </summary>
    Init,

    /// <summary>
    /// State after triggering <see cref="Triggers.InitKeyedData"/>
    /// </summary>
    InitKeyedData,

    /// <summary>
    /// State after triggering <see cref="Triggers.AddDataChunk"/>
    /// </summary>
    Data,

    /// <summary>
    /// State after triggering <see cref="Triggers.CompleteData"/>
    /// </summary>
    DataComplete,

    /// <summary>
    /// State after triggering <see cref="Triggers.CompleteRequest"/>
    /// </summary>
    InitKeyedDataLast,
  }

  /// <summary>
  /// Transitions for the Final State Machine
  /// </summary>
  public enum Triggers
  {
    /// <summary>
    /// Correspond to receive request <see cref="InitKeyedDataStream.TypeOneofCase.Key"/>
    /// </summary>
    InitKeyedData,

    /// <summary>
    /// Correspond to receive request <see cref="DataChunk.TypeOneofCase.Data"/>
    /// </summary>
    AddDataChunk,

    /// <summary>
    /// Correspond to receive request <see cref="DataChunk.TypeOneofCase.DataComplete"/>
    /// </summary>
    CompleteData,

    /// <summary>
    /// Correspond to receive request <see cref="InitKeyedDataStream.TypeOneofCase.LastResult"/>
    /// </summary>
    CompleteRequest,
  }

  private readonly ILogger logger_;

  private readonly StateMachine<State, Triggers> machine_;

  /// <summary>
  /// Constructor that initializes the Final State Machine
  /// </summary>
  /// <param name="logger">Logger used to produce logs for this class</param>
  public ProcessReplyResultStateMachine(ILogger logger)
  {
    logger_  = logger;
    machine_ = new StateMachine<State, Triggers>(State.Init);

    machine_.Configure(State.Init)
            .Permit(Triggers.InitKeyedData,
                    State.InitKeyedData);

    machine_.Configure(State.InitKeyedData)
            .Permit(Triggers.AddDataChunk,
                    State.Data);

    machine_.Configure(State.Data)
            .PermitReentry(Triggers.AddDataChunk)
            .Permit(Triggers.CompleteData,
                    State.DataComplete);

    machine_.Configure(State.DataComplete)
            .Permit(Triggers.CompleteRequest,
                    State.InitKeyedDataLast);



    if (logger_.IsEnabled(LogLevel.Debug))
    {
      machine_.OnTransitioned(t => logger_.LogDebug("OnTransitioned {FSM}: {Source} -> {Destination}",
                                                    nameof(ProcessReplyResultStateMachine),
                                                    t.Source,
                                                    t.Destination));
    }
  }

  /// <summary>
  /// Function used when using <see cref="Triggers.InitKeyedData"/> transition
  /// </summary>
  /// <exception cref="InvalidOperationException">Invalid transition</exception>
  public void InitKey()
    => machine_.Fire(Triggers.InitKeyedData);

  /// <summary>
  /// Function used when using <see cref="Triggers.AddDataChunk"/> transition
  /// </summary>
  /// <exception cref="InvalidOperationException">Invalid transition</exception>
  public void AddDataChunk()
    => machine_.Fire(Triggers.AddDataChunk);

  /// <summary>
  /// Function used when using <see cref="Triggers.CompleteData"/> transition
  /// </summary>
  /// <exception cref="InvalidOperationException">Invalid transition</exception>
  public void CompleteData()
    => machine_.Fire(Triggers.CompleteData);

  /// <summary>
  /// Function used when using <see cref="Triggers.CompleteRequest"/> transition
  /// </summary>
  /// <exception cref="InvalidOperationException">Invalid transition</exception>
  public void CompleteRequest()
    => machine_.Fire(Triggers.CompleteRequest);

  /// <summary>
  /// Check if the Final State Machine is in its completed state
  /// </summary>
  /// <returns>
  /// A bool representing if the final state machine is in a completed state
  /// </returns>
  public bool IsComplete()
    => machine_.State == State.InitKeyedDataLast;

  /// <summary>
  /// Generate a dot graph representing the Final State Machine
  /// </summary>
  /// <returns>
  /// A string containing the graph in dot format
  /// </returns>
  public string GenerateGraph()
    => UmlDotGraph.Format(machine_.GetInfo());

  /// <summary>
  /// Generate a Mermaid graph representing the Final State Machine
  /// </summary>
  /// <returns>
  /// A string containing the graph in Mermaid format
  /// </returns>
  public string GenerateMermaidGraph()
  {
    var str = UmlMermaidGraph.Format(machine_.GetInfo());

    // Manually fix the footer; the last
    // 3 lines should be disposed
    var lines = str.Split(new[]
                          {
                            Environment.NewLine
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
  /// Get the current state of the Final State Machine
  /// </summary>
  /// <returns>
  /// The current state of the Final State Machine
  /// </returns>
  public State GetState()
    => machine_.State;
}
