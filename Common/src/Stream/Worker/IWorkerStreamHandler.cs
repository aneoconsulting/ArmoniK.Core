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

using System;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Common.Storage;

using JetBrains.Annotations;

namespace ArmoniK.Core.Common.Stream.Worker;

/// <summary>
///   Defines the interface for handling communication with worker processes.
/// </summary>
[PublicAPI]
public interface IWorkerStreamHandler : IInitializable, IDisposable
{
  /// <summary>
  ///   Sends a task to a worker for processing and waits for its completion.
  /// </summary>
  /// <param name="taskData">The task data containing all information needed to process the task.</param>
  /// <param name="token">Authentication token for the worker.</param>
  /// <param name="dataFolder">The path to the folder where task data can be stored.</param>
  /// <param name="cancellationToken">Token to cancel the task processing.</param>
  /// <returns>The output of the task processing, including status and any error information.</returns>
  public Task<Output> StartTaskProcessing(TaskData          taskData,
                                          string            token,
                                          string            dataFolder,
                                          CancellationToken cancellationToken);
}
