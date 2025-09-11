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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.Helpers;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using NUnit.Framework;

using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

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

  private class CustomGetResultTable : IResultTable
  {
    private readonly List<string> resultIds_;
    private readonly string       sessionId_;


    public CustomGetResultTable(string       sessionId,
                                List<string> resultIds)
    {
      sessionId_ = sessionId;
      resultIds_ = resultIds;
    }

    public Task<HealthCheckResult> Check(HealthCheckTag tag)
      => throw new NotImplementedException();

    public Task Init(CancellationToken cancellationToken)
      => throw new NotImplementedException();

    public ILogger Logger
      => NullLogger.Instance;

    public Task ChangeResultOwnership(string                                                 oldTaskId,
                                      IEnumerable<IResultTable.ChangeResultOwnershipRequest> requests,
                                      CancellationToken                                      cancellationToken)
      => throw new NotImplementedException();

    public Task Create(ICollection<Result> results,
                       CancellationToken   cancellationToken = default)
      => throw new NotImplementedException();

    public Task AddTaskDependencies(IDictionary<string, ICollection<string>> dependencies,
                                    CancellationToken                        cancellationToken = default)
      => throw new NotImplementedException();

    public Task DeleteResult(string            key,
                             CancellationToken cancellationToken = default)
      => throw new NotImplementedException();

    public Task DeleteResults(string            sessionId,
                              CancellationToken cancellationToken = default)
      => throw new NotImplementedException();

    public Task DeleteResults(ICollection<string> results,
                              CancellationToken   cancellationToken = default)
      => throw new NotImplementedException();

    public IAsyncEnumerable<T> GetResults<T>(Expression<Func<Result, bool>> filter,
                                             Expression<Func<Result, T>>    convertor,
                                             CancellationToken              cancellationToken = default)
      => resultIds_.Select(s => convertor.Compile()
                                         .Invoke(new Result(sessionId_,
                                                            s,
                                                            "",
                                                            "",
                                                            "",
                                                            "",
                                                            ResultStatus.Completed,
                                                            new List<string>(),
                                                            DateTime.UtcNow,
                                                            DateTime.UtcNow,
                                                            100,
                                                            Encoding.UTF8.GetBytes(s),
                                                            false)))
                   .ToAsyncEnumerable();

    public Task<(IEnumerable<Result> results, int totalCount)> ListResultsAsync(Expression<Func<Result, bool>>    filter,
                                                                                Expression<Func<Result, object?>> orderField,
                                                                                bool                              ascOrder,
                                                                                int                               page,
                                                                                int                               pageSize,
                                                                                CancellationToken                 cancellationToken = default)
      => throw new NotImplementedException();

    public Task SetTaskOwnership(ICollection<(string resultId, string taskId)> requests,
                                 CancellationToken                             cancellationToken = default)
      => throw new NotImplementedException();

    public Task<Result> UpdateOneResult(string                   resultId,
                                        UpdateDefinition<Result> updates,
                                        CancellationToken        cancellationToken = default)
      => throw new NotImplementedException();

    public Task<long> UpdateManyResults(Expression<Func<Result, bool>> filter,
                                        UpdateDefinition<Result>       updates,
                                        CancellationToken              cancellationToken = default)
      => throw new NotImplementedException();
  }

  [Test]
  public async Task EmptyPayloadAndOneDependency()
  {
    const string sessionId    = "SessionId";
    const string parentTaskId = "ParentTaskId";
    const string taskId       = "TaskId";
    const string output1      = "Output1";
    const string dependency1  = "Dependency1";
    const string podId        = "PodId";
    const string podName      = "PodName";
    const string payloadId    = "PayloadId";
    const string createdBy    = "CreatedBy";

    var mockObjectStorage = new Mock<IObjectStorage>();
    mockObjectStorage.Setup(x => x.GetValuesAsync(It.IsAny<byte[]>(),
                                                  CancellationToken.None))
                     .Returns((byte[]            _,
                               CancellationToken _) => new List<byte[]>
                                                       {
                                                         Convert.FromBase64String("1111"),
                                                         Convert.FromBase64String("2222"),
                                                         Convert.FromBase64String("3333"),
                                                         Convert.FromBase64String("4444"),
                                                       }.ToAsyncEnumerable());

    var loggerFactory = new LoggerFactory();

    var dataPrefetcher = new DataPrefetcher(mockObjectStorage.Object,
                                            new CustomGetResultTable(sessionId,
                                                                     new List<string>
                                                                     {
                                                                       dependency1,
                                                                       payloadId,
                                                                     }),
                                            activitySource_,
                                            loggerFactory.CreateLogger<DataPrefetcher>());


    var sharedFolder = Path.Combine(Path.GetTempPath(),
                                    "data");
    var internalFolder = Path.Combine(Path.GetTempPath(),
                                      "internal");
    Directory.Delete(sharedFolder,
                     true);
    Directory.CreateDirectory(sharedFolder);

    await dataPrefetcher.PrefetchDataAsync(new TaskData(sessionId,
                                                        taskId,
                                                        podId,
                                                        podName,
                                                        payloadId,
                                                        createdBy,
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
                                                        new Output(OutputStatus.Success,
                                                                   "")),
                                           sharedFolder,
                                           CancellationToken.None)
                        .ConfigureAwait(false);

    Assert.IsTrue(File.Exists(Path.Combine(sharedFolder,
                                           payloadId)));
    Assert.IsTrue(File.Exists(Path.Combine(sharedFolder,
                                           dependency1)));
  }


  [Test]
  public async Task EmptyPayloadAndNoDependenciesStateMachine()
  {
    const string sessionId    = "SessionId";
    const string parentTaskId = "ParentTaskId";
    const string taskId       = "TaskId";
    const string output1      = "Output1";
    const string dependency1  = "Dependency1";
    const string dependency2  = "Dependency2";
    const string podId        = "PodId";
    const string podName      = "PodName";
    const string payloadId    = "PayloadId";
    const string createdBy    = "CreatedBy";


    var mockObjectStorage = new Mock<IObjectStorage>();
    mockObjectStorage.Setup(x => x.GetValuesAsync(It.IsAny<byte[]>(),
                                                  CancellationToken.None))
                     .Returns((byte[]            _,
                               CancellationToken _) => new List<byte[]>
                                                       {
                                                         Convert.FromBase64String("1111"),
                                                         Convert.FromBase64String("2222"),
                                                         Convert.FromBase64String("3333"),
                                                         Convert.FromBase64String("4444"),
                                                       }.ToAsyncEnumerable());

    var loggerFactory = new LoggerFactory();

    var dataPrefetcher = new DataPrefetcher(mockObjectStorage.Object,
                                            new CustomGetResultTable(sessionId,
                                                                     new List<string>
                                                                     {
                                                                       dependency1,
                                                                       dependency2,
                                                                       payloadId,
                                                                     }),
                                            activitySource_,
                                            loggerFactory.CreateLogger<DataPrefetcher>());


    var sharedFolder = Path.Combine(Path.GetTempPath(),
                                    "data");
    var internalFolder = Path.Combine(Path.GetTempPath(),
                                      "internal");
    Directory.Delete(sharedFolder,
                     true);
    Directory.CreateDirectory(sharedFolder);

    await dataPrefetcher.PrefetchDataAsync(new TaskData(sessionId,
                                                        taskId,
                                                        podId,
                                                        podName,
                                                        payloadId,
                                                        createdBy,
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
                                                        new Output(OutputStatus.Success,
                                                                   "")),
                                           sharedFolder,
                                           CancellationToken.None)
                        .ConfigureAwait(false);

    Assert.IsTrue(File.Exists(Path.Combine(sharedFolder,
                                           payloadId)));
  }

  [Test]
  public async Task InitShouldSucceed()
  {
    var mockObjectStorage = new Mock<IObjectStorage>();
    var loggerFactory     = new LoggerFactory();

    var dataPrefetcher = new DataPrefetcher(mockObjectStorage.Object,
                                            new SimpleResultTable(),
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
