// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Agent;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.Helpers;

using Grpc.Core;

using Moq;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Grpc;

[TestFixture(TestOf = typeof(GrpcAgentService))]
public class GrpcAgentServiceTests
{
  [Test]
  public async Task ATaskPartitionOutsideItsSessionIsAnsweredInvalidToTheWorker()
  {
    var agent = new Mock<IAgent>();
    agent.Setup(a => a.SubmitTasks(It.IsAny<ICollection<TaskSubmissionRequest>>(),
                                   It.IsAny<TaskOptions?>(),
                                   It.IsAny<string>(),
                                   It.IsAny<string>(),
                                   It.IsAny<CancellationToken>()))
         .ThrowsAsync(new PartitionNotInSessionException("partition p is not in the session"));

    var service = new GrpcAgentService();
    await service.Start(agent.Object)
                 .ConfigureAwait(false);

    var rpc = Assert.ThrowsAsync<RpcException>(() => service.SubmitTasks(new SubmitTasksRequest
                                                                         {
                                                                           SessionId          = "session",
                                                                           CommunicationToken = "token",
                                                                         },
                                                                         TestServerCallContext.Create()));

    Assert.That(rpc!.StatusCode,
                Is.EqualTo(StatusCode.InvalidArgument));
    Assert.That(rpc.Status.Detail,
                Does.Contain("partition p is not in the session"));
  }
}
