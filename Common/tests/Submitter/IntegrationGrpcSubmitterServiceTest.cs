using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Tests.Helpers;

using Grpc.Core;
using Grpc.Net.Client;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using Moq;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Submitter;

// see https://github.com/dotnet/AspNetCore.Docs/tree/main/aspnetcore/grpc/test-services/sample/Tests/Server/IntegrationTests
// this is an example of how to implement integrated tests for a gRPC server

[TestFixture]
internal class IntegrationGrpcSubmitterServiceTest
{
  private GrpcSubmitterServiceHelper helper;

  [SetUp]
  public void SetUp()
  {
  }

  [OneTimeSetUp]
  public void OneTimeSetUp()
  {
  }

  [TearDown]
  public async Task TearDown()
  {
    await helper.StopServer()
                .ConfigureAwait(false);
    helper.Dispose();
  }

  [Test]
  public async Task GetServiceConfigurationShouldSucceed()
  {
    var mockSubmitter = new Mock<ISubmitter>();
    mockSubmitter.Setup(submitter => submitter.GetServiceConfiguration(It.IsAny<Empty>(),
                                                                       It.IsAny<CancellationToken>()))


                 .Returns(() => Task.FromResult(new Configuration
                 {
                   DataChunkMaxSize = 42,
                 }));

    helper = new GrpcSubmitterServiceHelper(mockSubmitter.Object);
    await helper.StartServer()
                .ConfigureAwait(false);

    var client = new Api.gRPC.V1.Submitter.SubmitterClient(helper.Channel);

    var response = client.GetServiceConfiguration(new Empty());

    Assert.AreEqual(42,
                    response.DataChunkMaxSize);
  }
}