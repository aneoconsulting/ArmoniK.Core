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

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Task information to identify it
/// </summary>
/// <param name="SessionId">Unique identifier of the session in which the task belongs</param>
/// <param name="TaskId">Unique identifier of the task</param>
/// <param name="MessageId">Unique identifier of the message associated to the current execution of the task</param>
/// <param name="TaskStatus">Status of the task</param>
public record TaskInfo(string     SessionId,
                       string     TaskId,
                       string     MessageId,
                       TaskStatus TaskStatus);
