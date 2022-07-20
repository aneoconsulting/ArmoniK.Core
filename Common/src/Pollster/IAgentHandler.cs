// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-$CURRENT_YEAR$. All rights reserved.
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
// but WITHOUT ANY WARRANTY

using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.gRPC.Services;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Pollster;

/// <summary>
/// Interface to create classes that will populate <see cref="IAgent"/> when worker sends requests
/// </summary>
public interface IAgentHandler
{
  /// <summary>
  /// Stops the handler
  /// </summary>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  /// Task representing the asynchronous execution of the method
  /// </returns>
  Task Stop(CancellationToken cancellationToken);

  /// <summary>
  /// Starts the handler
  /// </summary>
  /// <param name="token">Token that can be used to differentiate running tasks</param>
  /// <param name="logger">Logger that may be injected into the handler that embed preconfigured scopes</param>
  /// <param name="taskId"></param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <param name="sessionId"></param>
  /// <returns>
  /// Task representing the asynchronous execution of the method
  /// </returns>
  Task<IAgent> Start(string            token,
                     ILogger           logger,
                     string            sessionId,
                     string            taskId,
                     CancellationToken cancellationToken);
}