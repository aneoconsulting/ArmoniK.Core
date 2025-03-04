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

using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Tests.Helpers;

public class SimpleAgentHandler : IAgentHandler
{
  public IAgent? Agent;

  public Task Stop(CancellationToken cancellationToken)
    => Task.CompletedTask;

  public Task<IAgent> Start(string            token,
                            ILogger           logger,
                            SessionData       sessionData,
                            TaskData          taskData,
                            string            folder,
                            CancellationToken cancellationToken)
  {
    Agent = new SimpleAgent();
    return Task.FromResult(Agent);
  }
}
