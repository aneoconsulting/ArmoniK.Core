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

using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Stream.Worker;
using ArmoniK.Core.Common.Tests.Helpers;

using Microsoft.Extensions.Logging;

using Moq;

using NUnit.Framework;

using Output = ArmoniK.Core.Common.Storage.Output;
using TaskOptions = ArmoniK.Core.Common.Storage.TaskOptions;
using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Tests.Pollster;

[TestFixture]
public class WorkerStreamHandlerTests
{
  private GrpcSubmitterServiceHelper helper_;
  private WorkerStreamHandler        workerStreamHandler_;
  private ActivitySource             activitySource_;

  [SetUp]
  public async Task SetUp()
  {
    activitySource_ = new ActivitySource(nameof(WorkerStreamHandlerTests));

    var mockSubmitter       = new Mock<ISubmitter>();
    var mockChannelProvider = new Mock<IGrpcChannelProvider>();

    helper_ = new GrpcSubmitterServiceHelper(mockSubmitter.Object);

    var channel = await helper_.CreateChannel()
                               .ConfigureAwait(false);

    var mockObjectStorageFactory = new Mock<IObjectStorageFactory>();
    var mockObjectStorage        = new Mock<IObjectStorage>();
    mockObjectStorage.Setup(x => x.GetValuesAsync(It.IsAny<string>(),
                                                  CancellationToken.None))
                     .Returns((string            key,
                               CancellationToken token) => new List<byte[]>().ToAsyncEnumerable());

    var mockResultStorage = new Mock<IObjectStorage>();
    mockResultStorage.Setup(x => x.GetValuesAsync(It.IsAny<string>(),
                                                  CancellationToken.None))
                     .Returns((string            key,
                               CancellationToken token) => new List<byte[]>
                                                           {
                                                             Convert.FromBase64String("1111"),
                                                           }.ToAsyncEnumerable());

    mockObjectStorageFactory.Setup(x => x.CreateObjectStorage(It.IsAny<string>()))
                            .Returns((string objname) =>
                                     {
                                       if (objname.StartsWith("results"))
                                       {
                                         return mockResultStorage.Object;
                                       }

                                       if (objname.StartsWith("payloads"))
                                       {
                                         return mockObjectStorage.Object;
                                       }

                                       return null;
                                     });

    mockChannelProvider.Setup(cp => cp.Get())
                       .Returns(() => channel);

    var loggerFactory = new LoggerFactory();

    var dataPrefetcher = new DataPrefetcher(mockObjectStorageFactory.Object,
                                            activitySource_,
                                            loggerFactory.CreateLogger<DataPrefetcher>());

    workerStreamHandler_ = new WorkerStreamHandler(mockChannelProvider.Object,
                                                   dataPrefetcher);
  }


  [TearDown]
  public async Task TearDown()
  {
    await helper_.StopServer()
                 .ConfigureAwait(false);
    helper_.Dispose();
  }

  [Test]
  public void IntegrationNotInitializedStreamsShouldBeNull()
  {
    Assert.IsNull(workerStreamHandler_.Stream);
    Assert.IsNull(workerStreamHandler_.WorkerRequestStream);
    Assert.IsNull(workerStreamHandler_.WorkerResponseStream);
  }

  [Test]
  public void IntegrationStartTaskProcessingFailsOnInvalidTaskOptions()
  {
    var taskData = new TaskData("SessionId",
                                "TaskId",
                                "PodId",
                                new[]
                                {
                                  "ParentTaskId"
                                },
                                new[]
                                {
                                  "Dependency",
                                },
                                new[]
                                {
                                  "Output",
                                },
                                Array.Empty<string>(),
                                TaskStatus.Creating,
                                "",
                                new TaskOptions(new Dictionary<string, string>(),
                                                TimeSpan.FromSeconds(100),
                                                5,
                                                0), // Zero priority is not a valid option
                                DateTime.Now,
                                DateTime.MinValue,
                                DateTime.MinValue,
                                DateTime.Now,
                                DateTime.Now,
                                new Output(false,
                                           ""));

    Assert.Throws<ArmoniKException>(() =>
                                    {
                                      workerStreamHandler_.StartTaskProcessing(taskData,
                                                                               CancellationToken.None);
                                    });
  }

  [Test]
  public void IntegrationStartTaskProcessingSucceeds()
  {
    var taskData = new TaskData("SessionId",
                                "TaskId",
                                "PodId",
                                new[]
                                {
                                  "ParentTaskId",
                                },
                                new[]
                                {
                                  "Dependency",
                                },
                                new[]
                                {
                                  "Output",
                                },
                                Array.Empty<string>(),
                                TaskStatus.Creating,
                                "",
                                new TaskOptions(new Dictionary<string, string>(),
                                                TimeSpan.FromSeconds(100),
                                                5,
                                                1), 
                                DateTime.Now,
                                DateTime.MinValue,
                                DateTime.MinValue,
                                DateTime.Now,
                                DateTime.Now,
                                new Output(false,
                                           ""));

    workerStreamHandler_.StartTaskProcessing(taskData, CancellationToken.None);

    Assert.IsNotNull(workerStreamHandler_.Stream);
    Assert.IsNotNull(workerStreamHandler_.WorkerRequestStream);
    Assert.IsNotNull(workerStreamHandler_.WorkerResponseStream);
  }
}