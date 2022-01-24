// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
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

namespace ArmoniK.Core.Common.Storage;

public interface IResult
{
  /// <summary>
  /// Id of the session that produces and consumes this data
  /// </summary>
  string SessionId         { get; }
  /// <summary>
  /// When created, the object is only visible to the subtask created by the dispatch that generated it.
  /// When the value is equal to <see cref="SessionId"/>, the object is visible to the whole session.
  /// The value is updated at the end of the execution of the corresponding dispatch.
  /// </summary>
  string DispatchId        { get; }
  /// <summary>
  /// Key to reference and access this result
  /// </summary>
  string Key               { get; }
  /// <summary>
  /// Id of the first task that was supposed to generate this result.
  /// The responsibility of generating this result may have been forwarded to <see cref="Owner"/>
  /// </summary>
  string Creator             { get; }
  /// <summary>
  /// Id of the task that is responsible of generating this result.
  /// </summary>
  string ResponsibilityOwner             { get; }
  /// <summary>
  /// if <value>true</value>, the result is available, either in <see cref="Data"/> or in an other storage
  /// </summary>
  bool   IsResultAvailable { get; }
  /// <summary>
  /// When <see cref="IsResultAvailable"/> is <value>true</value>, <c>Data</c> can contain the result.
  /// If the value is <value>null</value>, the data is stored in the configured <c>resultStorage</c>.
  /// </summary>
  byte[] Data              { get; }
  /// <summary>
  /// Date of creation of the current object.
  /// </summary>
  DateTime CreationDate { get; }
}
