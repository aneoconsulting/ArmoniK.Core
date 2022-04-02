using System;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.StateMachines;

using Microsoft.Extensions.Logging.Abstractions;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.StateMachines;

[TestFixture]
public class ComputeRequestStateMachineTest
{
  [Test]
  public void PayloadFirstShouldFail()
  {
    var sm = new ComputeRequestStateMachine(NullLogger<ComputeRequestStateMachine>.Instance);
    Assert.ThrowsAsync<InvalidOperationException>(async () => await sm.ReceiveRequest(new ProcessRequest.Types.ComputeRequest
    {
      Payload = null,
    }));
  }

  [Test]
  public async Task InitRequestFirstShouldSucceed()
  {
    var sm = new ComputeRequestStateMachine(NullLogger<ComputeRequestStateMachine>.Instance);
    var request = new ProcessRequest.Types.ComputeRequest
    {
      InitRequest = new ProcessRequest.Types.ComputeRequest.Types.InitRequest
      {
        SessionId = "sessionId",
        TaskId    = "taskId",
      },
    };
    await sm.ReceiveRequest(request);
  }
}
