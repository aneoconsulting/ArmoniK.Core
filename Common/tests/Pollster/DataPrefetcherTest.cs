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
// but WITHOUT ANY WARRANTY; without even the implied warranty of
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
using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.StateMachines;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using NUnit.Framework;

using Output = ArmoniK.Core.Common.Storage.Output;
using TaskOptions = ArmoniK.Core.Common.Storage.TaskOptions;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Tests.Pollster;

[TestFixture]
public class DataPrefetcherTest
{
  private ActivitySource             activitySource_;
  private ComputeRequestStateMachine sm_;

  [SetUp]
  public void SetUp()
  {
    activitySource_ = new ActivitySource(nameof(DataPrefetcherTest));
    sm_ = new ComputeRequestStateMachine(NullLogger<ComputeRequestStateMachine>.Instance);
  }

  [TearDown]
  public virtual void TearDown()
  {
  }

  [Test]
  public async Task EmptyPayloadAndOneDependency()
  {
    var mockObjectStorageFactory = new Mock<IObjectStorageFactory>();
    var mockObjectStorage        = new Mock<IObjectStorage>();
    mockObjectStorage.Setup(x => x.TryGetValuesAsync(It.IsAny<string>(),
                                                     CancellationToken.None)).Returns((string key, CancellationToken token) => new List<byte[]>().ToAsyncEnumerable());

    var mockResultStorage = new Mock<IObjectStorage>();
    mockResultStorage.Setup(x => x.TryGetValuesAsync(It.IsAny<string>(),
                                                     CancellationToken.None)).Returns((string key, CancellationToken token) => new List<byte[]>
    {
      Convert.FromBase64String("1111"),
      Convert.FromBase64String("2222"),
      Convert.FromBase64String("3333"),
      Convert.FromBase64String("4444"),
    }.ToAsyncEnumerable());

    mockObjectStorageFactory.Setup(x => x.CreateObjectStorage(It.IsAny<string>())).Returns((string objname) =>
    {
      if (objname.StartsWith("results"))
        return mockResultStorage.Object;
      if (objname.StartsWith("payloads"))
        return mockObjectStorage.Object;
      return null;
    });

    var loggerFactory = new LoggerFactory();

    var dataPrefetcher = new DataPrefetcher(mockObjectStorageFactory.Object,
                                         activitySource_,
                                         loggerFactory.CreateLogger<DataPrefetcher>());

    const string sessionId = "SessionId";
    const string parentTaskId = "ParentTaskId";
    const string dispatchId = "DispatchId";
    const string taskId = "TaskId";
    const string output1 = "Output1";
    const string dependency1 = "Dependency1";
    var res = await dataPrefetcher.PrefetchDataAsync(new TaskData(sessionId,
                                                                   parentTaskId,
                                                                   dispatchId,
                                                                   taskId,
                                                                   new List<string>
                                                                   {
                                                                     dependency1,
                                                                   },
                                                                   new List<string>
                                                                   {
                                                                     output1,
                                                                   },
                                                                   false,
                                                                   Array.Empty<byte>(),
                                                                   TaskStatus.Dispatched,
                                                                   new TaskOptions(new Dictionary<string, string>(),
                                                                                   TimeSpan.FromSeconds(1),
                                                                                   3,
                                                                                   1),
                                                                   new List<string>
                                                                   {
                                                                     "AncestorDispatchId"
                                                                   },
                                                                   new Output(false,
                                                                              "")),
                                                      CancellationToken.None);
    var computeRequests = res.ToArray();
    foreach (var request in computeRequests)
    {
      Console.WriteLine(request);
    }

    Assert.AreEqual(computeRequests[0].TypeCase, ProcessRequest.Types.ComputeRequest.TypeOneofCase.InitRequest);
    Assert.AreEqual(computeRequests[0].InitRequest.SessionId, sessionId);
    Assert.AreEqual(computeRequests[0].InitRequest.TaskId, taskId);
    Assert.AreEqual(computeRequests[0].InitRequest.ExpectedOutputKeys.First(), output1);
    Assert.AreEqual(computeRequests[0].InitRequest.Payload.Data, Array.Empty<byte>());
    Assert.AreEqual(computeRequests[1].TypeCase, ProcessRequest.Types.ComputeRequest.TypeOneofCase.Payload);
    Assert.AreEqual(computeRequests[1].Payload.TypeCase, DataChunk.TypeOneofCase.DataComplete);
    Assert.IsTrue(computeRequests[1].Payload.DataComplete);
    Assert.AreEqual(computeRequests[2].TypeCase, ProcessRequest.Types.ComputeRequest.TypeOneofCase.InitData);
    Assert.AreEqual(computeRequests[2].InitData.Key,
                    dependency1);
    Assert.AreEqual(computeRequests[3].TypeCase, ProcessRequest.Types.ComputeRequest.TypeOneofCase.Data);
    Assert.AreEqual(computeRequests[3].Data.TypeCase, DataChunk.TypeOneofCase.Data);

  }

  [Test]
  public async Task EmptyPayloadAndOneDependencyStateMachine()
  {
    var mockObjectStorageFactory = new Mock<IObjectStorageFactory>();
    var mockObjectStorage = new Mock<IObjectStorage>();
    mockObjectStorage.Setup(x => x.TryGetValuesAsync(It.IsAny<string>(),
                                                     CancellationToken.None)).Returns((string key, CancellationToken token) => new List<byte[]>().ToAsyncEnumerable());

    var mockResultStorage = new Mock<IObjectStorage>();
    mockResultStorage.Setup(x => x.TryGetValuesAsync(It.IsAny<string>(),
                                                     CancellationToken.None)).Returns((string key, CancellationToken token) => new List<byte[]>
    {
      Convert.FromBase64String("1111"),
      Convert.FromBase64String("2222"),
      Convert.FromBase64String("3333"),
      Convert.FromBase64String("4444"),
    }.ToAsyncEnumerable());

    mockObjectStorageFactory.Setup(x => x.CreateObjectStorage(It.IsAny<string>())).Returns((string objname) =>
    {
      if (objname.StartsWith("results"))
        return mockResultStorage.Object;
      if (objname.StartsWith("payloads"))
        return mockObjectStorage.Object;
      return null;
    });

    var loggerFactory = new LoggerFactory();

    var dataPrefetcher = new DataPrefetcher(mockObjectStorageFactory.Object,
                                         activitySource_,
                                         loggerFactory.CreateLogger<DataPrefetcher>());

    const string sessionId = "SessionId";
    const string parentTaskId = "ParentTaskId";
    const string dispatchId = "DispatchId";
    const string taskId = "TaskId";
    const string output1 = "Output1";
    const string dependency1 = "Dependency1";
    var res = await dataPrefetcher.PrefetchDataAsync(new TaskData(sessionId,
                                                                   parentTaskId,
                                                                   dispatchId,
                                                                   taskId,
                                                                   new List<string>
                                                                   {
                                                                     dependency1,
                                                                   },
                                                                   new List<string>
                                                                   {
                                                                     output1,
                                                                   },
                                                                   false,
                                                                   Array.Empty<byte>(),
                                                                   TaskStatus.Dispatched,
                                                                   new TaskOptions(new Dictionary<string, string>(),
                                                                                   TimeSpan.FromSeconds(1),
                                                                                   3,
                                                                                   1),
                                                                   new List<string>
                                                                   {
                                                                     "AncestorDispatchId"
                                                                   },
                                                                   new Output(false,
                                                                              "")),
                                                      CancellationToken.None);
  }

  [Test]
  public async Task EmptyPayloadAndOneDependencyWithDataStateMachine()
  {
    var mockObjectStorageFactory = new Mock<IObjectStorageFactory>();
    var mockObjectStorage = new Mock<IObjectStorage>();
    mockObjectStorage.Setup(x => x.TryGetValuesAsync(It.IsAny<string>(),
                                                     CancellationToken.None)).Returns((string key, CancellationToken token) => new List<byte[]>
    {
      Convert.FromBase64String("AAAA"),
      Convert.FromBase64String("BBBB"),
      Convert.FromBase64String("CCCC"),
      Convert.FromBase64String("DDDD"),
    }.ToAsyncEnumerable());

    var mockResultStorage = new Mock<IObjectStorage>();
    mockResultStorage.Setup(x => x.TryGetValuesAsync(It.IsAny<string>(),
                                                     CancellationToken.None)).Returns((string key, CancellationToken token) => new List<byte[]>
    {
      Convert.FromBase64String("1111"),
      Convert.FromBase64String("2222"),
      Convert.FromBase64String("3333"),
      Convert.FromBase64String("4444"),
    }.ToAsyncEnumerable());

    mockObjectStorageFactory.Setup(x => x.CreateObjectStorage(It.IsAny<string>())).Returns((string objname) =>
    {
      if (objname.StartsWith("results"))
        return mockResultStorage.Object;
      if (objname.StartsWith("payloads"))
        return mockObjectStorage.Object;
      return null;
    });

    var loggerFactory = new LoggerFactory();

    var dataPrefetcher = new DataPrefetcher(mockObjectStorageFactory.Object,
                                         activitySource_,
                                         loggerFactory.CreateLogger<DataPrefetcher>());

    const string sessionId = "SessionId";
    const string parentTaskId = "ParentTaskId";
    const string dispatchId = "DispatchId";
    const string taskId = "TaskId";
    const string output1 = "Output1";
    const string dependency1 = "Dependency1";
    var res = await dataPrefetcher.PrefetchDataAsync(new TaskData(sessionId,
                                                                   parentTaskId,
                                                                   dispatchId,
                                                                   taskId,
                                                                   new List<string>
                                                                   {
                                                                     dependency1,
                                                                   },
                                                                   new List<string>
                                                                   {
                                                                     output1,
                                                                   },
                                                                   false,
                                                                   Array.Empty<byte>(),
                                                                   TaskStatus.Dispatched,
                                                                   new TaskOptions(new Dictionary<string, string>(),
                                                                                   TimeSpan.FromSeconds(1),
                                                                                   3,
                                                                                   1),
                                                                   new List<string>
                                                                   {
                                                                     "AncestorDispatchId"
                                                                   },
                                                                   new Output(false,
                                                                              "")),
                                                      CancellationToken.None);
  }

  [Test]
  public async Task PayloadWithDataAndOneDependencyWithDataStateMachine()
  {
    var mockObjectStorageFactory = new Mock<IObjectStorageFactory>();
    var mockObjectStorage = new Mock<IObjectStorage>();
    mockObjectStorage.Setup(x => x.TryGetValuesAsync(It.IsAny<string>(),
                                                     CancellationToken.None)).Returns((string key, CancellationToken token) => new List<byte[]>
    {
      Convert.FromBase64String("AAAA"),
      Convert.FromBase64String("BBBB"),
      Convert.FromBase64String("CCCC"),
      Convert.FromBase64String("DDDD"),
    }.ToAsyncEnumerable());

    var mockResultStorage = new Mock<IObjectStorage>();
    mockResultStorage.Setup(x => x.TryGetValuesAsync(It.IsAny<string>(),
                                                     CancellationToken.None)).Returns((string key, CancellationToken token) => new List<byte[]>
    {
      Convert.FromBase64String("1111"),
      Convert.FromBase64String("2222"),
      Convert.FromBase64String("3333"),
      Convert.FromBase64String("4444"),
    }.ToAsyncEnumerable());

    mockObjectStorageFactory.Setup(x => x.CreateObjectStorage(It.IsAny<string>())).Returns((string objname) =>
    {
      if (objname.StartsWith("results"))
        return mockResultStorage.Object;
      if (objname.StartsWith("payloads"))
        return mockObjectStorage.Object;
      return null;
    });

    var loggerFactory = new LoggerFactory();

    var dataPrefetcher = new DataPrefetcher(mockObjectStorageFactory.Object,
                                         activitySource_,
                                         loggerFactory.CreateLogger<DataPrefetcher>());

    const string sessionId = "SessionId";
    const string parentTaskId = "ParentTaskId";
    const string dispatchId = "DispatchId";
    const string taskId = "TaskId";
    const string output1 = "Output1";
    const string dependency1 = "Dependency1";
    var res = await dataPrefetcher.PrefetchDataAsync(new TaskData(sessionId,
                                                                   parentTaskId,
                                                                   dispatchId,
                                                                   taskId,
                                                                   new List<string>
                                                                   {
                                                                     dependency1,
                                                                   },
                                                                   new List<string>
                                                                   {
                                                                     output1,
                                                                   },
                                                                   false,
                                                                   Array.Empty<byte>(),
                                                                   TaskStatus.Dispatched,
                                                                   new TaskOptions(new Dictionary<string, string>(),
                                                                                   TimeSpan.FromSeconds(1),
                                                                                   3,
                                                                                   1),
                                                                   new List<string>
                                                                   {
                                                                     "AncestorDispatchId"
                                                                   },
                                                                   new Output(false,
                                                                              "")),
                                                      CancellationToken.None);
  }

  [Test]
  public async Task EmptyPayloadAndTwoDependenciesStateMachine()
  {
    var mockObjectStorageFactory = new Mock<IObjectStorageFactory>();
    var mockObjectStorage = new Mock<IObjectStorage>();
    mockObjectStorage.Setup(x => x.TryGetValuesAsync(It.IsAny<string>(),
                                                     CancellationToken.None)).Returns((string key, CancellationToken token) => new List<byte[]>().ToAsyncEnumerable());

    var mockResultStorage = new Mock<IObjectStorage>();
    mockResultStorage.Setup(x => x.TryGetValuesAsync(It.IsAny<string>(),
                                                     CancellationToken.None)).Returns((string key, CancellationToken token) => new List<byte[]>
    {
      Convert.FromBase64String("1111"),
      Convert.FromBase64String("2222"),
      Convert.FromBase64String("3333"),
      Convert.FromBase64String("4444"),
    }.ToAsyncEnumerable());

    mockObjectStorageFactory.Setup(x => x.CreateObjectStorage(It.IsAny<string>())).Returns((string objname) =>
    {
      if (objname.StartsWith("results"))
        return mockResultStorage.Object;
      if (objname.StartsWith("payloads"))
        return mockObjectStorage.Object;
      return null;
    });

    var loggerFactory = new LoggerFactory();

    var dataPrefetcher = new DataPrefetcher(mockObjectStorageFactory.Object,
                                         activitySource_,
                                         loggerFactory.CreateLogger<DataPrefetcher>());

    const string sessionId = "SessionId";
    const string parentTaskId = "ParentTaskId";
    const string dispatchId = "DispatchId";
    const string taskId = "TaskId";
    const string output1 = "Output1";
    const string dependency1 = "Dependency1";
    const string dependency2 = "Dependency2";
    var res = await dataPrefetcher.PrefetchDataAsync(new TaskData(sessionId,
                                                                  parentTaskId,
                                                                  dispatchId,
                                                                  taskId,
                                                                  new List<string>
                                                                  {
                                                                    dependency1,
                                                                    dependency2,
                                                                  },
                                                                  new List<string>
                                                                  {
                                                                    output1,
                                                                  },
                                                                  false,
                                                                  Array.Empty<byte>(),
                                                                  TaskStatus.Dispatched,
                                                                  new TaskOptions(new Dictionary<string, string>(),
                                                                                  TimeSpan.FromSeconds(1),
                                                                                  3,
                                                                                  1),
                                                                  new List<string>
                                                                  {
                                                                    "AncestorDispatchId"
                                                                  },
                                                                  new Output(false,
                                                                             "")),
                                                     CancellationToken.None);
  }

  [Test]
  public async Task EmptyPayloadAndNoDependenciesStateMachine()
  {
    var mockObjectStorageFactory = new Mock<IObjectStorageFactory>();
    var mockObjectStorage = new Mock<IObjectStorage>();
    mockObjectStorage.Setup(x => x.TryGetValuesAsync(It.IsAny<string>(),
                                                     CancellationToken.None)).Returns((string key, CancellationToken token) => new List<byte[]>().ToAsyncEnumerable());

    var mockResultStorage = new Mock<IObjectStorage>();
    mockResultStorage.Setup(x => x.TryGetValuesAsync(It.IsAny<string>(),
                                                     CancellationToken.None)).Returns((string key, CancellationToken token) => new List<byte[]>
    {
      Convert.FromBase64String("1111"),
      Convert.FromBase64String("2222"),
      Convert.FromBase64String("3333"),
      Convert.FromBase64String("4444"),
    }.ToAsyncEnumerable());

    mockObjectStorageFactory.Setup(x => x.CreateObjectStorage(It.IsAny<string>())).Returns((string objname) =>
    {
      if (objname.StartsWith("results"))
        return mockResultStorage.Object;
      if (objname.StartsWith("payloads"))
        return mockObjectStorage.Object;
      return null;
    });

    var loggerFactory = new LoggerFactory();

    var dataPrefetcher = new DataPrefetcher(mockObjectStorageFactory.Object,
                                         activitySource_,
                                         loggerFactory.CreateLogger<DataPrefetcher>());

    const string sessionId = "SessionId";
    const string parentTaskId = "ParentTaskId";
    const string dispatchId = "DispatchId";
    const string taskId = "TaskId";
    const string output1 = "Output1";
    var res = await dataPrefetcher.PrefetchDataAsync(new TaskData(sessionId,
                                                                  parentTaskId,
                                                                  dispatchId,
                                                                  taskId,
                                                                  new List<string>(),
                                                                  new List<string>
                                                                  {
                                                                    output1,
                                                                  },
                                                                  false,
                                                                  Array.Empty<byte>(),
                                                                  TaskStatus.Dispatched,
                                                                  new TaskOptions(new Dictionary<string, string>(),
                                                                                  TimeSpan.FromSeconds(1),
                                                                                  3,
                                                                                  1),
                                                                  new List<string>
                                                                  {
                                                                    "AncestorDispatchId"
                                                                  },
                                                                  new Output(false,
                                                                             "")),
                                                     CancellationToken.None);
  }

}