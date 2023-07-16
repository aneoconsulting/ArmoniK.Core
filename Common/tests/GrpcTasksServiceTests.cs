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

using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Core.Base;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Tests.Helpers;

using Grpc.Net.Client;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

[TestFixture(TestOf = typeof(GrpcTasksService))]
public class GrpcTasksServiceTests
{
  [SetUp]
  public void Setup()
    => helper_ = new TestDatabaseProvider(collection => collection.AddSingleton<IPullQueueStorage, SimplePullQueueStorage>()
                                                                  .AddSingleton<IPushQueueStorage, SimplePushQueueStorage>()
                                                                  .AddGrpc(),
                                          builder => builder.UseRouting()
                                                            .UseAuthorization(),
                                          builder => builder.MapGrpcService<GrpcTasksService>(),
                                          true);

  [TearDown]
  public void TearDown()
    => helper_?.Dispose();

  private TestDatabaseProvider? helper_;


  [Test]
  public async Task CancelNotExistingTaskShouldSucceed()
  {
    await helper_!.App.StartAsync()
                  .ConfigureAwait(false);

    var server = helper_.App.GetTestServer();

    var channel = GrpcChannel.ForAddress("http://localhost:9999",
                                         new GrpcChannelOptions
                                         {
                                           HttpHandler = server.CreateHandler(),
                                         });

    var client = new Tasks.TasksClient(channel);

    var response = client.CancelTasks(new CancelTasksRequest
                                      {
                                        TaskIds =
                                        {
                                          "test",
                                        },
                                      });

    Assert.That(response,
                Is.Not.Null);
    Assert.That(response.Tasks.Count,
                Is.EqualTo(0));
  }
}
