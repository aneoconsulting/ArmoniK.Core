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

namespace ArmoniK.Core.Base.DataStructures;

/// <summary>
///   Data structure to hold message data
/// </summary>
/// <param name="TaskId">Unique identifier of the task</param>
/// <param name="SessionId">Unique name of the session to which this message belongs</param>
/// <param name="Options">Task options</param>
public record MessageData(string      TaskId,
                          string      SessionId,
                          TaskOptions Options);
