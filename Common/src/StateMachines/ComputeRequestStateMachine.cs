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

  private readonly ILogger logger_;

  private readonly StateMachine<State, Triggers> machine_;

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
      machine_.OnTransitioned(t => logger_.LogDebug("OnTransitioned: {Source} -> {Destination}",
                                                    t.Source,
                                                    t.Destination));
    }
  }

  public void InitRequest()
    => machine_.Fire(Triggers.InitRequest);

  public void AddPayloadChunk()
    => machine_.Fire(Triggers.AddPayloadChunk);

  public void CompletePayload()
    => machine_.Fire(Triggers.CompletePayload);

  public void InitDataDependency()
    => machine_.Fire(Triggers.InitDataDependency);

  public void AddDataDependencyChunk()
    => machine_.Fire(Triggers.AddDataDependencyChunk);

  public void CompleteDataDependency()
    => machine_.Fire(Triggers.CompleteDataDependency);

  public void CompleteRequest()
    => machine_.Fire(Triggers.CompleteRequest);

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
    bld.Insert(0,"```mermaid\n");
    bld.Append("\n```\n");

    return bld.ToString();
  }

  public State GetState()
    => machine_.State;
}
