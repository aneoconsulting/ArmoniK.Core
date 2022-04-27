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

using Stateless.Graph;
using Stateless.Reflection;

namespace ArmoniK.Core.Common.StateMachines;

/// <summary>
/// Class to generate a UML graph in mermaid format
/// </summary>
//
public static class UmlMermaidGraph
{
  /// <summary>
  /// Generate a UML Mermaid graph from the state machine info
  /// The current implementation will output a string whose last
  /// three lines do not correspond to mermaid code and have to be
  /// taken out explicitly. If the output string (OutputString) is to be used in
  /// a markdown document, it's also necessary to enclose it in
  /// proper mermaid markers. e.g., ```mermaid OutputString ```
  /// </summary>
  /// <param name="machineInfo"></param>
  /// <returns></returns>
  public static string Format(StateMachineInfo machineInfo)
  {
    var graph = new StateGraph(machineInfo);

    return graph.ToGraph(new UmlMermaidGraphStyle());
  }
}