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
///   Represents a request for task submission, containing all necessary information to create a task.
/// </summary>
/// <remarks>
///   This record encapsulates the data needed for task creation, including its payload identifier,
///   execution options, expected outputs, and data dependencies. It serves as an intermediate
///   representation between client requests and internal task creation.
/// </remarks>
/// <param name="PayloadId">The identifier for the task's payload data.</param>
/// <param name="Options">Optional task options that override session defaults.</param>
/// <param name="ExpectedOutputKeys">Collection of keys for results that this task is expected to produce.</param>
/// <param name="DataDependencies">Collection of data identifiers that this task depends on.</param>
public record TaskSubmissionRequest(string              PayloadId,
                                    TaskOptions?        Options,
                                    ICollection<string> ExpectedOutputKeys,
                                    ICollection<string> DataDependencies);
