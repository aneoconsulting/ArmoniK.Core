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
using System.Linq;

using Stateless.Graph;

namespace ArmoniK.Core.Common.StateMachines;

public class UmlDot2Mermaid : GraphStyleBase
{
  /// <inheritdoc />
  public override string GetPrefix()
  {
    return "stateDiagram-v2\ndirection TB\n";
  }

  /// <inheritdoc />
  public override string FormatOneCluster(SuperState stateInfo)
  {
    var stateRepresentationString = "\n" + $"state {stateInfo.NodeName} {{ \n";

    foreach (var subState in stateInfo.SubStates)
    {
      stateRepresentationString += FormatOneState(subState);
    }

    return $"{stateRepresentationString}}}";
  }

  /// <inheritdoc />
  public override string FormatOneState(State state)
  {
    if ((state.EntryActions.Count == 0) && (state.ExitActions.Count == 0))
      return $"{state.StateName}\n";

    var f = $"\nstate {state.StateName}{{\n";

    List<string> es = new List<string>();
    es.AddRange(state.EntryActions.Select(act => "entry:"+act));
    es.AddRange(state.ExitActions.Select(act => "exit:"+act));

    f += string.Join("\n", es);
    f += "\n}\n";

    return f;
  }

  /// <inheritdoc />
  public override string FormatOneTransition(string              sourceNodeName,
                                             string              trigger,
                                             IEnumerable<string> actions,
                                             string              destinationNodeName,
                                             IEnumerable<string> guards)
  {
    string label = trigger ?? "";

    if (actions?.Count() > 0)
      label += " / " + string.Join(", ",
                                   actions);

    if (guards.Any())
    {
      foreach (var info in guards)
      {
        if (label.Length > 0)
          label += " ";

        label += info;
      }
    }

    return FormatOneLine(sourceNodeName,
                         destinationNodeName,
                         label);
  }

  /// <inheritdoc />
  public override string FormatOneDecisionNode(string nodeName,
                                               string label)
  {
    return $"{nodeName}:{label}\n";
  }

  internal string FormatOneLine(string fromNodeName,
                                string toNodeName,
                                string label)
  {
    return $"{fromNodeName} --> {toNodeName}:{label}\n";
  }
}