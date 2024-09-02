// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Base;
using ArmoniK.Core.Common.gRPC.Convertors;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.Helpers;

using Google.Protobuf.WellKnownTypes;

using Microsoft.Extensions.DependencyInjection;

using NUnit.Framework;

using ResultStatus = ArmoniK.Core.Common.Storage.ResultStatus;
using SessionStatus = ArmoniK.Core.Common.Storage.SessionStatus;
using TaskRequest = ArmoniK.Core.Common.gRPC.Services.TaskRequest;

namespace ArmoniK.Core.Common.Tests;

[TestFixture]
public class TaskLifeCycleHelperTest
{
  private class Holder : IDisposable
  {
    private const string Partition       = "PartitionId";
    private const string ExpectedOutput1 = "ExpectedOutput1";
    private const string ExpectedOutput2 = "ExpectedOutput2";
    private const string DataDependency1 = "DataDependency1";
    private const string DataDependency2 = "DataDependency2";

    private static readonly TaskOptions Options = new()
                                                  {
                                                    ApplicationName      = "applicationName",
                                                    ApplicationNamespace = "applicationNamespace",
                                                    ApplicationVersion   = "applicationVersion",
                                                    ApplicationService   = "applicationService",
                                                    EngineType           = "engineType",
                                                    PartitionId          = Partition,
                                                    MaxDuration          = Duration.FromTimeSpan(TimeSpan.FromMinutes(10)),
                                                    MaxRetries           = 5,
                                                    Priority             = 1,
                                                    Options =
                                                    {
                                                      {
                                                        "key1", "val1"
                                                      },
                                                      {
                                                        "key2", "val2"
                                                      },
                                                    },
                                                  };

    private static readonly Injection.Options.Submitter SubmitterOptions = new()
                                                                           {
                                                                             DefaultPartition = Partition,
                                                                             MaxErrorAllowed  = -1,
                                                                           };


    public readonly  string                        Folder;
    public readonly  IObjectStorage                ObjectStorage;
    private readonly TestDatabaseProvider          prov_;
    public readonly  IPullQueueStorage             PullQueueStorage;
    public readonly  IPushQueueStorage             PushQueueStorage;
    public readonly  SimplePullQueueStorageChannel QueueStorage;
    public readonly  IResultTable                  ResultTable;
    public readonly  string                        Session;
    public readonly  ISessionTable                 SessionTable;
    public readonly  ISubmitter                    Submitter;
    public readonly  ITaskTable                    TaskTable;

    public Holder()
    {
      QueueStorage     = new SimplePullQueueStorageChannel();
      PushQueueStorage = QueueStorage;
      PullQueueStorage = QueueStorage;
      prov_ = new TestDatabaseProvider(collection => collection.AddSingleton<ISubmitter, gRPC.Services.Submitter>()
                                                               .AddSingleton(SubmitterOptions)
                                                               .AddSingleton(PushQueueStorage)
                                                               .AddSingleton(PullQueueStorage));

      ResultTable   = prov_.GetRequiredService<IResultTable>();
      TaskTable     = prov_.GetRequiredService<ITaskTable>();
      ObjectStorage = prov_.GetRequiredService<IObjectStorage>();
      SessionTable  = prov_.GetRequiredService<ISessionTable>();
      Submitter     = prov_.GetRequiredService<ISubmitter>();

      Session = SessionTable.SetSessionDataAsync(new[]
                                                 {
                                                   Partition,
                                                 },
                                                 Options.ToTaskOptions(),
                                                 CancellationToken.None)
                            .Result;

      var sessionData = SessionTable.GetSessionAsync(Session,
                                                     CancellationToken.None)
                                    .Result;

      ResultTable.Create(new[]
                         {
                           new Result(sessionData.SessionId,
                                      DataDependency1,
                                      "",
                                      "",
                                      "",
                                      ResultStatus.Completed,
                                      new List<string>(),
                                      DateTime.UtcNow,
                                      0,
                                      Array.Empty<byte>()),
                           new Result(sessionData.SessionId,
                                      DataDependency2,
                                      "",
                                      "",
                                      "",
                                      ResultStatus.Completed,
                                      new List<string>(),
                                      DateTime.UtcNow,
                                      0,
                                      Array.Empty<byte>()),
                           new Result(Session,
                                      ExpectedOutput1,
                                      "",
                                      "",
                                      "",
                                      ResultStatus.Created,
                                      new List<string>(),
                                      DateTime.UtcNow,
                                      0,
                                      Array.Empty<byte>()),
                           new Result(Session,
                                      ExpectedOutput2,
                                      "",
                                      "",
                                      "",
                                      ResultStatus.Created,
                                      new List<string>(),
                                      DateTime.UtcNow,
                                      0,
                                      Array.Empty<byte>()),
                         },
                         CancellationToken.None)
                 .Wait();

      Folder = Path.Combine(Path.GetTempPath(),
                            "data");
      Directory.CreateDirectory(Folder);

      var createdTasks = Submitter.CreateTasks(Session,
                                               Session,
                                               Options.ToTaskOptions(),
                                               new[]
                                               {
                                                 new TaskRequest(new List<string>
                                                                 {
                                                                   ExpectedOutput1,
                                                                 },
                                                                 new List<string>(),
                                                                 new List<byte[]>
                                                                   {
                                                                     Encoding.ASCII.GetBytes("Payload1"),
                                                                     Encoding.ASCII.GetBytes("Payload2"),
                                                                   }.Select(bytes => new ReadOnlyMemory<byte>(bytes))
                                                                    .ToAsyncEnumerable()),
                                               }.ToAsyncEnumerable(),
                                               CancellationToken.None)
                                  .Result;

      Submitter.FinalizeTaskCreation(createdTasks,
                                     sessionData,
                                     Session,
                                     CancellationToken.None)
               .Wait();

      var createdTasks2 = Submitter.CreateTasks(Session,
                                                Session,
                                                Options.ToTaskOptions(),
                                                new[]
                                                {
                                                  new TaskRequest(new List<string>
                                                                  {
                                                                    ExpectedOutput2,
                                                                  },
                                                                  new List<string>
                                                                  {
                                                                    ExpectedOutput1,
                                                                  },
                                                                  new List<byte[]>
                                                                    {
                                                                      Encoding.ASCII.GetBytes("Payload1"),
                                                                      Encoding.ASCII.GetBytes("Payload2"),
                                                                    }.Select(bytes => new ReadOnlyMemory<byte>(bytes))
                                                                     .ToAsyncEnumerable()),
                                                  new TaskRequest(new List<string>(),
                                                                  new List<string>
                                                                  {
                                                                    ExpectedOutput1,
                                                                  },
                                                                  new List<byte[]>
                                                                    {
                                                                      Encoding.ASCII.GetBytes("Payload1"),
                                                                      Encoding.ASCII.GetBytes("Payload2"),
                                                                    }.Select(bytes => new ReadOnlyMemory<byte>(bytes))
                                                                     .ToAsyncEnumerable()),
                                                }.ToAsyncEnumerable(),
                                                CancellationToken.None)
                                   .Result;

      Submitter.FinalizeTaskCreation(createdTasks2,
                                     sessionData,
                                     Session,
                                     CancellationToken.None)
               .Wait();
    }

    public void Dispose()
      => prov_.Dispose();
  }


  [Test]
  public async Task SubmitTasksCreateOneRequestShouldSucceed()
  {
    using var holder = new Holder();

    await holder.SessionTable.PauseSessionAsync(holder.Session)
                .ConfigureAwait(false);

    var session = await holder.SessionTable.GetSessionAsync(holder.Session)
                              .ConfigureAwait(false);

    Assert.That(session.Status,
                Is.EqualTo(SessionStatus.Paused));

    await holder.QueueStorage.EmptyAsync()
                .ConfigureAwait(false);
    Assert.That(holder.QueueStorage.Channel.Reader.Count,
                Is.EqualTo(0));

    await TaskLifeCycleHelper.ResumeAsync(holder.TaskTable,
                                          holder.SessionTable,
                                          holder.PushQueueStorage,
                                          holder.Session)
                             .ConfigureAwait(false);

    session = await holder.SessionTable.GetSessionAsync(holder.Session)
                          .ConfigureAwait(false);

    Assert.That(session.Status,
                Is.EqualTo(SessionStatus.Running));
    Assert.That(holder.QueueStorage.Channel.Reader.Count,
                Is.EqualTo(1));
  }
}
