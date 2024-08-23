// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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

using ArmoniK.Core.Common.Utils;

namespace ArmoniK.Core.Common.Pollster;

/// <summary>
///   Queue to send <see cref="TaskHandler" />
///   from a single producer to a consumer.
/// </summary>
/// <remarks>
///   <para>
///     When a producer writes a task handler,
///     it will wait for a consumer to read the task handler.
///   </para>
///   <para>
///     If the producer successfully writes to the queue,
///     the consumer is guaranteed to have successfully read from the queue.
///   </para>
///   <para>
///     If the producer fails to write to the queue,
///     the consumer is guaranteed to not have read
///     the poduced the task handler.
///   </para>
/// </remarks>
public abstract class TaskQueueBase : RendezvousChannel<TaskHandler>;
