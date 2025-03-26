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

using System.Collections.Generic;

using ArmoniK.Core.Base.DataStructures;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Represents a request to create a task with specific options, payload, and dependencies.
/// </summary>
/// <param name="TaskId">The unique identifier for the task.</param>
/// <param name="PayloadId">The identifier for the payload associated with the task.</param>
/// <param name="Options">The options specifying the task's configuration.</param>
/// <param name="ExpectedOutputKeys">
///   The collection of <see cref="Result" /> unique identifier expected as output from the
///   task.
/// </param>
/// <param name="DataDependencies">
///   The collection of <see cref="Result" /> unique identifier dependencies required by the
///   task.
/// </param>
public record TaskCreationRequest(string              TaskId,
                                  string              PayloadId,
                                  TaskOptions         Options,
                                  ICollection<string> ExpectedOutputKeys,
                                  ICollection<string> DataDependencies);
