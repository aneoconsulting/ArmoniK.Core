// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
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
using ArmoniK.Core.Common.Pollster;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Tests.Helpers;

public class SimpleAgentHandler : IAgentHandler
{
  public IAgent Agent;

  public Task Stop(CancellationToken cancellationToken)
    => Task.CompletedTask;

  public Task<IAgent> Start(string            token,
                            ILogger           logger,
                            string            sessionId,
                            string            taskId,
                            CancellationToken cancellationToken)
  {
    Agent = new SimpleAgent();
    return Task.FromResult(Agent);
  }
}
