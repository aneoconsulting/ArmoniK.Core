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

using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Pollster;

/// <summary>
///   Interface to create classes that will populate <see cref="IAgent" /> when worker sends requests
/// </summary>
public interface IAgentHandler
{
  /// <summary>
  ///   Stops the handler
  /// </summary>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  Task Stop(CancellationToken cancellationToken);

  /// <summary>
  ///   Starts the handler
  /// </summary>
  /// <param name="token">Token that can be used to differentiate running tasks</param>
  /// <param name="logger">Logger that may be injected into the handler that embed preconfigured scopes</param>
  /// <param name="sessionData">Session metadata</param>
  /// <param name="taskData">Task metadata</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  Task<IAgent> Start(string            token,
                     ILogger           logger,
                     SessionData       sessionData,
                     TaskData          taskData,
                     CancellationToken cancellationToken);
}
