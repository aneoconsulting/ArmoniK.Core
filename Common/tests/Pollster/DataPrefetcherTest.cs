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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Worker;
using ArmoniK.Core.Base;
using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using Moq;

using NUnit.Framework;

using Output = ArmoniK.Core.Common.Storage.Output;
using TaskOptions = ArmoniK.Core.Base.TaskOptions;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Tests.Pollster;

[TestFixture]
public class DataPrefetcherTest
{
  [SetUp]
  public void SetUp()
    => activitySource_ = new ActivitySource(nameof(DataPrefetcherTest));

  [TearDown]
  public virtual void TearDown()
  {
  }

  private ActivitySource? activitySource_;

  [Test]
  public async Task EmptyPayloadAndOneDependency()
  {
    var mockObjectStorage = new Mock<IObjectStorage>();
    mockObjectStorage.Setup(x => x.GetValuesAsync(It.IsAny<string>(),
                                                  CancellationToken.None))
                     .Returns((string            _,
                               CancellationToken _) => new List<byte[]>
                                                       {
                                                         Convert.FromBase64String("1111"),
                                                         Convert.FromBase64String("2222"),
                                                         Convert.FromBase64String("3333"),
                                                         Convert.FromBase64String("4444"),
                                                       }.ToAsyncEnumerable());
    var loggerFactory = new LoggerFactory();

    var dataPrefetcher = new DataPrefetcher(mockObjectStorage.Object,
                                            activitySource_,
                                            loggerFactory.CreateLogger<DataPrefetcher>());

    const string sessionId    = "SessionId";
    const string parentTaskId = "ParentTaskId";
    const string taskId       = "TaskId";
    const string output1      = "Output1";
    const string dependency1  = "Dependency1";
    const string podId        = "PodId";
    const string podName      = "PodName";
    const string payloadId    = "PayloadId";
    var res = await dataPrefetcher.PrefetchDataAsync(new TaskData(sessionId,
                                                                  taskId,
                                                                  podId,
                                                                  podName,
                                                                  payloadId,
                                                                  new[]
                                                                  {
                                                                    parentTaskId,
                                                                  },
                                                                  new[]
                                                                  {
                                                                    dependency1,
                                                                  },
                                                                  new[]
                                                                  {
                                                                    output1,
                                                                  },
                                                                  Array.Empty<string>(),
                                                                  TaskStatus.Submitted,
                                                                  new TaskOptions(new Dictionary<string, string>(),
                                                                                  TimeSpan.FromSeconds(100),
                                                                                  5,
                                                                                  1,
                                                                                  "part1",
                                                                                  "applicationName",
                                                                                  "applicationVersion",
                                                                                  "applicationNamespace",
                                                                                  "applicationService",
                                                                                  "engineType"),
                                                                  new Output(true,
                                                                             "")),
                                                     CancellationToken.None)
                                  .ConfigureAwait(false);
    var computeRequests = res.ToArray();
    foreach (var request in computeRequests)
    {
      Console.WriteLine(request);
    }

    Assert.AreEqual(computeRequests[0]
                      .TypeCase,
                    ProcessRequest.Types.ComputeRequest.TypeOneofCase.InitRequest);
    Assert.AreEqual(computeRequests[0]
                    .InitRequest.SessionId,
                    sessionId);
    Assert.AreEqual(computeRequests[0]
                    .InitRequest.TaskId,
                    taskId);
    Assert.AreEqual(computeRequests[0]
                    .InitRequest.ExpectedOutputKeys.First(),
                    output1);

    Assert.AreEqual(computeRequests[1]
                      .TypeCase,
                    ProcessRequest.Types.ComputeRequest.TypeOneofCase.Payload);
    Assert.AreEqual(computeRequests[1]
                    .Payload.TypeCase,
                    DataChunk.TypeOneofCase.Data);

    Assert.AreEqual(computeRequests[2]
                      .TypeCase,
                    ProcessRequest.Types.ComputeRequest.TypeOneofCase.Payload);
    Assert.AreEqual(computeRequests[2]
                    .Payload.TypeCase,
                    DataChunk.TypeOneofCase.Data);

    Assert.AreEqual(computeRequests[3]
                      .TypeCase,
                    ProcessRequest.Types.ComputeRequest.TypeOneofCase.Payload);
    Assert.AreEqual(computeRequests[3]
                    .Payload.TypeCase,
                    DataChunk.TypeOneofCase.Data);

    Assert.AreEqual(computeRequests[4]
                      .TypeCase,
                    ProcessRequest.Types.ComputeRequest.TypeOneofCase.Payload);
    Assert.AreEqual(computeRequests[4]
                    .Payload.TypeCase,
                    DataChunk.TypeOneofCase.DataComplete);
    Assert.IsTrue(computeRequests[4]
                  .Payload.DataComplete);

    Assert.AreEqual(computeRequests[5]
                      .TypeCase,
                    ProcessRequest.Types.ComputeRequest.TypeOneofCase.InitData);
    Assert.AreEqual(computeRequests[5]
                    .InitData.Key,
                    dependency1);

    Assert.AreEqual(computeRequests[6]
                      .TypeCase,
                    ProcessRequest.Types.ComputeRequest.TypeOneofCase.Data);
    Assert.AreEqual(computeRequests[6]
                    .Data.TypeCase,
                    DataChunk.TypeOneofCase.Data);

    Assert.AreEqual(computeRequests[7]
                      .TypeCase,
                    ProcessRequest.Types.ComputeRequest.TypeOneofCase.Data);
    Assert.AreEqual(computeRequests[7]
                    .Data.TypeCase,
                    DataChunk.TypeOneofCase.Data);

    Assert.AreEqual(computeRequests[8]
                      .TypeCase,
                    ProcessRequest.Types.ComputeRequest.TypeOneofCase.Data);
    Assert.AreEqual(computeRequests[8]
                    .Data.TypeCase,
                    DataChunk.TypeOneofCase.Data);

    Assert.AreEqual(computeRequests[9]
                      .TypeCase,
                    ProcessRequest.Types.ComputeRequest.TypeOneofCase.Data);
    Assert.AreEqual(computeRequests[9]
                    .Data.TypeCase,
                    DataChunk.TypeOneofCase.Data);

    Assert.AreEqual(computeRequests[10]
                      .TypeCase,
                    ProcessRequest.Types.ComputeRequest.TypeOneofCase.Data);
    Assert.AreEqual(computeRequests[10]
                    .Data.TypeCase,
                    DataChunk.TypeOneofCase.DataComplete);
    Assert.IsTrue(computeRequests[10]
                  .Data.DataComplete);

    Assert.AreEqual(computeRequests[11]
                      .TypeCase,
                    ProcessRequest.Types.ComputeRequest.TypeOneofCase.InitData);
    Assert.AreEqual(computeRequests[11]
                    .InitData.TypeCase,
                    ProcessRequest.Types.ComputeRequest.Types.InitData.TypeOneofCase.LastData);
    Assert.IsTrue(computeRequests[11]
                  .InitData.LastData);
  }

  [Test]
  public async Task EmptyPayloadAndOneDependencyStateMachine()
  {
    var mockObjectStorage = new Mock<IObjectStorage>();
    mockObjectStorage.Setup(x => x.GetValuesAsync(It.IsAny<string>(),
                                                  CancellationToken.None))
                     .Returns((string            _,
                               CancellationToken _) => new List<byte[]>
                                                       {
                                                         Convert.FromBase64String("1111"),
                                                         Convert.FromBase64String("2222"),
                                                         Convert.FromBase64String("3333"),
                                                         Convert.FromBase64String("4444"),
                                                       }.ToAsyncEnumerable());

    var loggerFactory = new LoggerFactory();

    var dataPrefetcher = new DataPrefetcher(mockObjectStorage.Object,
                                            activitySource_,
                                            loggerFactory.CreateLogger<DataPrefetcher>());

    const string sessionId    = "SessionId";
    const string parentTaskId = "ParentTaskId";
    const string taskId       = "TaskId";
    const string output1      = "Output1";
    const string dependency1  = "Dependency1";
    const string podName      = "PodName";
    const string podId        = "PodId";
    const string payloadId    = "PayloadId";
    var res = await dataPrefetcher.PrefetchDataAsync(new TaskData(sessionId,
                                                                  taskId,
                                                                  podId,
                                                                  podName,
                                                                  payloadId,
                                                                  new[]
                                                                  {
                                                                    parentTaskId,
                                                                  },
                                                                  new[]
                                                                  {
                                                                    dependency1,
                                                                  },
                                                                  new[]
                                                                  {
                                                                    output1,
                                                                  },
                                                                  Array.Empty<string>(),
                                                                  TaskStatus.Submitted,
                                                                  new TaskOptions(new Dictionary<string, string>(),
                                                                                  TimeSpan.FromSeconds(100),
                                                                                  5,
                                                                                  1,
                                                                                  "part1",
                                                                                  "",
                                                                                  "",
                                                                                  "",
                                                                                  "",
                                                                                  ""),
                                                                  new Output(true,
                                                                             "")),
                                                     CancellationToken.None)
                                  .ConfigureAwait(false);
    Assert.AreNotEqual(0,
                       res.Count);
  }

  [Test]
  public async Task EmptyPayloadAndOneDependencyWithDataStateMachine()
  {
    var mockObjectStorage = new Mock<IObjectStorage>();
    mockObjectStorage.Setup(x => x.GetValuesAsync(It.IsAny<string>(),
                                                  CancellationToken.None))
                     .Returns((string            _,
                               CancellationToken _) => new List<byte[]>
                                                       {
                                                         Convert.FromBase64String("1111"),
                                                         Convert.FromBase64String("2222"),
                                                         Convert.FromBase64String("3333"),
                                                         Convert.FromBase64String("4444"),
                                                       }.ToAsyncEnumerable());

    var loggerFactory = new LoggerFactory();

    var dataPrefetcher = new DataPrefetcher(mockObjectStorage.Object,
                                            activitySource_,
                                            loggerFactory.CreateLogger<DataPrefetcher>());

    const string sessionId    = "SessionId";
    const string parentTaskId = "ParentTaskId";
    const string taskId       = "TaskId";
    const string output1      = "Output1";
    const string dependency1  = "Dependency1";
    const string podId        = "PodId";
    const string podName      = "PodName";
    const string payloadId    = "PayloadId";
    var res = await dataPrefetcher.PrefetchDataAsync(new TaskData(sessionId,
                                                                  taskId,
                                                                  podId,
                                                                  podName,
                                                                  payloadId,
                                                                  new[]
                                                                  {
                                                                    parentTaskId,
                                                                  },
                                                                  new[]
                                                                  {
                                                                    dependency1,
                                                                  },
                                                                  new[]
                                                                  {
                                                                    output1,
                                                                  },
                                                                  Array.Empty<string>(),
                                                                  TaskStatus.Submitted,
                                                                  new TaskOptions(new Dictionary<string, string>(),
                                                                                  TimeSpan.FromSeconds(100),
                                                                                  5,
                                                                                  1,
                                                                                  "part1",
                                                                                  "applicationName",
                                                                                  "applicationVersion",
                                                                                  "applicationNamespace",
                                                                                  "applicationService",
                                                                                  "engineType"),
                                                                  new Output(true,
                                                                             "")),
                                                     CancellationToken.None)
                                  .ConfigureAwait(false);
    Assert.AreNotEqual(0,
                       res.Count);
  }

  [Test]
  public async Task PayloadWithDataAndOneDependencyWithDataStateMachine()
  {
    var mockObjectStorage = new Mock<IObjectStorage>();
    mockObjectStorage.Setup(x => x.GetValuesAsync(It.IsAny<string>(),
                                                  CancellationToken.None))
                     .Returns((string            _,
                               CancellationToken _) => new List<byte[]>
                                                       {
                                                         Convert.FromBase64String("1111"),
                                                         Convert.FromBase64String("2222"),
                                                         Convert.FromBase64String("3333"),
                                                         Convert.FromBase64String("4444"),
                                                       }.ToAsyncEnumerable());

    var loggerFactory = new LoggerFactory();

    var dataPrefetcher = new DataPrefetcher(mockObjectStorage.Object,
                                            activitySource_,
                                            loggerFactory.CreateLogger<DataPrefetcher>());

    const string sessionId    = "SessionId";
    const string parentTaskId = "ParentTaskId";
    const string taskId       = "TaskId";
    const string output1      = "Output1";
    const string dependency1  = "Dependency1";
    const string podId        = "PodId";
    const string podName      = "PodName";
    const string payloadId    = "PayloadId";
    var res = await dataPrefetcher.PrefetchDataAsync(new TaskData(sessionId,
                                                                  taskId,
                                                                  podId,
                                                                  podName,
                                                                  payloadId,
                                                                  new[]
                                                                  {
                                                                    parentTaskId,
                                                                  },
                                                                  new[]
                                                                  {
                                                                    dependency1,
                                                                  },
                                                                  new[]
                                                                  {
                                                                    output1,
                                                                  },
                                                                  Array.Empty<string>(),
                                                                  TaskStatus.Submitted,
                                                                  new TaskOptions(new Dictionary<string, string>(),
                                                                                  TimeSpan.FromSeconds(100),
                                                                                  5,
                                                                                  1,
                                                                                  "part1",
                                                                                  "applicationName",
                                                                                  "applicationVersion",
                                                                                  "applicationNamespace",
                                                                                  "applicationService",
                                                                                  "engineType"),
                                                                  new Output(true,
                                                                             "")),
                                                     CancellationToken.None)
                                  .ConfigureAwait(false);
    Assert.AreNotEqual(0,
                       res.Count);
  }

  [Test]
  public async Task EmptyPayloadAndTwoDependenciesStateMachine()
  {
    var mockObjectStorage = new Mock<IObjectStorage>();
    mockObjectStorage.Setup(x => x.GetValuesAsync(It.IsAny<string>(),
                                                  CancellationToken.None))
                     .Returns((string            _,
                               CancellationToken _) => new List<byte[]>
                                                       {
                                                         Convert.FromBase64String("1111"),
                                                         Convert.FromBase64String("2222"),
                                                         Convert.FromBase64String("3333"),
                                                         Convert.FromBase64String("4444"),
                                                       }.ToAsyncEnumerable());

    var loggerFactory = new LoggerFactory();

    var dataPrefetcher = new DataPrefetcher(mockObjectStorage.Object,
                                            activitySource_,
                                            loggerFactory.CreateLogger<DataPrefetcher>());

    const string sessionId    = "SessionId";
    const string parentTaskId = "ParentTaskId";
    const string taskId       = "TaskId";
    const string output1      = "Output1";
    const string dependency1  = "Dependency1";
    const string dependency2  = "Dependency2";
    const string podId        = "PodId";
    const string podName      = "PodName";
    const string payloadId    = "PayloadId";
    var res = await dataPrefetcher.PrefetchDataAsync(new TaskData(sessionId,
                                                                  taskId,
                                                                  podId,
                                                                  podName,
                                                                  payloadId,
                                                                  new[]
                                                                  {
                                                                    parentTaskId,
                                                                  },
                                                                  new[]
                                                                  {
                                                                    dependency1,
                                                                    dependency2,
                                                                  },
                                                                  new[]
                                                                  {
                                                                    output1,
                                                                  },
                                                                  Array.Empty<string>(),
                                                                  TaskStatus.Submitted,
                                                                  new TaskOptions(new Dictionary<string, string>(),
                                                                                  TimeSpan.FromSeconds(100),
                                                                                  5,
                                                                                  1,
                                                                                  "part1",
                                                                                  "applicationName",
                                                                                  "applicationVersion",
                                                                                  "applicationNamespace",
                                                                                  "applicationService",
                                                                                  "engineType"),
                                                                  new Output(true,
                                                                             "")),
                                                     CancellationToken.None)
                                  .ConfigureAwait(false);
    Assert.AreNotEqual(0,
                       res.Count);
  }

  [Test]
  public async Task EmptyPayloadAndNoDependenciesStateMachine()
  {
    var mockObjectStorage = new Mock<IObjectStorage>();
    mockObjectStorage.Setup(x => x.GetValuesAsync(It.IsAny<string>(),
                                                  CancellationToken.None))
                     .Returns((string            _,
                               CancellationToken _) => new List<byte[]>
                                                       {
                                                         Convert.FromBase64String("1111"),
                                                         Convert.FromBase64String("2222"),
                                                         Convert.FromBase64String("3333"),
                                                         Convert.FromBase64String("4444"),
                                                       }.ToAsyncEnumerable());

    var loggerFactory = new LoggerFactory();

    var dataPrefetcher = new DataPrefetcher(mockObjectStorage.Object,
                                            activitySource_,
                                            loggerFactory.CreateLogger<DataPrefetcher>());

    const string sessionId    = "SessionId";
    const string parentTaskId = "ParentTaskId";
    const string taskId       = "TaskId";
    const string output1      = "Output1";
    const string dependency1  = "Dependency1";
    const string dependency2  = "Dependency2";
    const string podId        = "PodId";
    const string podName      = "PodName";
    const string payloadId    = "PayloadId";
    var res = await dataPrefetcher.PrefetchDataAsync(new TaskData(sessionId,
                                                                  taskId,
                                                                  podId,
                                                                  podName,
                                                                  payloadId,
                                                                  new[]
                                                                  {
                                                                    parentTaskId,
                                                                  },
                                                                  new[]
                                                                  {
                                                                    dependency1,
                                                                    dependency2,
                                                                  },
                                                                  new[]
                                                                  {
                                                                    output1,
                                                                  },
                                                                  Array.Empty<string>(),
                                                                  TaskStatus.Submitted,
                                                                  new TaskOptions(new Dictionary<string, string>(),
                                                                                  TimeSpan.FromSeconds(100),
                                                                                  5,
                                                                                  1,
                                                                                  "part1",
                                                                                  "applicationName",
                                                                                  "applicationVersion",
                                                                                  "applicationNamespace",
                                                                                  "applicationService",
                                                                                  "engineType"),
                                                                  new Output(true,
                                                                             "")),
                                                     CancellationToken.None)
                                  .ConfigureAwait(false);

    Assert.AreNotEqual(0,
                       res.Count);
  }

  [Test]
  public async Task InitShouldSucceed()
  {
    var mockObjectStorage = new Mock<IObjectStorage>();
    var loggerFactory     = new LoggerFactory();

    var dataPrefetcher = new DataPrefetcher(mockObjectStorage.Object,
                                            activitySource_,
                                            loggerFactory.CreateLogger<DataPrefetcher>());

    Assert.AreNotEqual(HealthCheckResult.Healthy(),
                       await dataPrefetcher.Check(HealthCheckTag.Liveness)
                                           .ConfigureAwait(false));
    Assert.AreNotEqual(HealthCheckResult.Healthy(),
                       await dataPrefetcher.Check(HealthCheckTag.Readiness)
                                           .ConfigureAwait(false));
    Assert.AreNotEqual(HealthCheckResult.Healthy(),
                       await dataPrefetcher.Check(HealthCheckTag.Startup)
                                           .ConfigureAwait(false));

    await dataPrefetcher.Init(CancellationToken.None)
                        .ConfigureAwait(false);

    Assert.AreEqual(HealthCheckResult.Healthy(),
                    await dataPrefetcher.Check(HealthCheckTag.Liveness)
                                        .ConfigureAwait(false));
    Assert.AreEqual(HealthCheckResult.Healthy(),
                    await dataPrefetcher.Check(HealthCheckTag.Readiness)
                                        .ConfigureAwait(false));
    Assert.AreEqual(HealthCheckResult.Healthy(),
                    await dataPrefetcher.Check(HealthCheckTag.Startup)
                                        .ConfigureAwait(false));
  }
}
