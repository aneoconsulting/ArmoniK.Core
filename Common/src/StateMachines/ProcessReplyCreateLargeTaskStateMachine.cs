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

using Microsoft.Extensions.Logging;

using Stateless;
using Stateless.Graph;

namespace ArmoniK.Core.Common.StateMachines;

public class ProcessReplyCreateLargeTaskStateMachine
{
  public enum State
  {
    Init,
    InitRequest,
    TaskRequestHeader,
    DataChunk,
    DataChunkComplete,
    InitTaskRequestLast,
  }

  public enum Triggers
  {
    InitRequest,
    AddHeader,
    AddDataChunk,
    CompleteData,
    CompleteRequest,
  }

  private readonly ILogger logger_;

  private readonly StateMachine<State, Triggers> machine_;

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
      machine_.OnTransitioned(t => logger_.LogDebug("OnTransitioned: {Source} -> {Destination}",
                                                    t.Source,
                                                    t.Destination));
    }
  }

  public void InitRequest()
    => machine_.Fire(Triggers.InitRequest);

  public void AddHeader()
    => machine_.Fire(Triggers.AddHeader);

  public void AddDataChunk()
    => machine_.Fire(Triggers.AddDataChunk);

  public void CompleteData()
    => machine_.Fire(Triggers.CompleteData);

  public void CompleteRequest()
    => machine_.Fire(Triggers.CompleteRequest);

  public bool IsComplete()
    => machine_.State == State.InitTaskRequestLast;
  public string GenerateGraph()
    => UmlDotGraph.Format(machine_.GetInfo());

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

  public State GetState()
    => machine_.State;
}
