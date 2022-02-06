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
using System.Collections.Generic;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Storage;

namespace ArmoniK.Core.Adapters.Memory;

public class TaskData : ITaskData
{

  /// <inheritdoc />
  public string SessionId { get; init; }

  /// <inheritdoc />
  public string ParentTaskId { get; init; }

  /// <inheritdoc />
  public string DispatchId { get; set; }

  /// <inheritdoc />
  public string TaskId { get; init; }

  public List<string>  DataDependencies { get; } = new();

  /// <inheritdoc />
  public IList<string> ExpectedOutput   { get; set; }

  /// <inheritdoc />
  IList<string> ITaskData.DataDependencies => DataDependencies;

  /// <inheritdoc />
  public bool HasPayload => true;

  public byte[] Payload { get; init; }

  /// <inheritdoc />
  public TaskStatus Status { get; set; }

  /// <inheritdoc />
  public TaskOptions Options { get; init; }

  /// <inheritdoc />
  public DateTime CreationDate { get; } = DateTime.UtcNow;

  public IDispatch Dispatch { get; set; }
}
