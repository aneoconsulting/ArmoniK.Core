// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Storage;

using Google.Protobuf.WellKnownTypes;

using JetBrains.Annotations;

using NUnit.Framework;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Tests;

[TestFixture(TestOf = typeof(ITableStorage))]
public abstract class TableStorageTestBase
{
  [PublicAPI]
  protected ITableStorage TableStorage { get; set; }

  [Test]
  public async Task CreateAndDeleteSession()
  {
    const string id = $"{nameof(TableStorageTestBase)}.{nameof(CreateAndDeleteSession)}.SessionId";

    Assert.False(await TableStorage.ListSessionsAsync()
                                   .ContainsAsync(id));

    Assert.AreEqual(CreateSessionReply.ResultOneofCase.Ok,
                    (await TableStorage.CreateSessionAsync(new()
                                                          {
                                                            Root = new()
                                                                   {
                                                                     Id = id,
                                                            DefaultTaskOption = new()
                                                                                {
                                                                                  MaxDuration = Duration.FromTimeSpan(TimeSpan.FromMinutes(2)),
                                                                                  MaxRetries  = 2,
                                                                                  Priority    = 2,
                                                                                },
                                                                   },
                                                          })).ResultCase);

    Assert.True(await TableStorage.ListSessionsAsync()
                                  .ContainsAsync(id));

    var filter = new TaskFilter
                 {
                   Unknown = new()
                             {
                               SessionId = id,
                             },
                 };

    Assert.IsEmpty(await TableStorage.ListTasksAsync(filter,
                                                     CancellationToken.None)
                                     .ToListAsync(CancellationToken.None));
    Assert.IsEmpty(await TableStorage.CountTasksAsync(filter,
                                                      CancellationToken.None));

    await TableStorage.DeleteSessionAsync(id,
                                          CancellationToken.None);

    Assert.False(await TableStorage.ListSessionsAsync()
                                   .ContainsAsync(id));
  }

  [Test]
  [Category("NotYetImplemented")]
  public void WhenSessionHasBeenCreatedDefaultTaskOptionShouldBeRetrievable()
  {
    Assert.Fail("NotYetImplemented");
  }

  [Test]
  [Category("NotYetImplemented")]
  public void WhenSubSessionHasBeenCreatedDefaultTaskOptionShouldBeRetrievable()
  {
    Assert.Fail("NotYetImplemented");
  }

  [Test]
  [Category("NotYetImplemented")]
  public void WhenDispatchHasLostOwnershipTtlExtensionShouldThrow()
  {
    Assert.Fail("NotYetImplemented");
  }

  [Test]
  [Category("NotYetImplemented")]
  public void WhenDispatchHasLostOwnershipFormerDispatchTtlShouldBeInThePast()
  {
    Assert.Fail("NotYetImplemented");
  }

  [Test]
  [Category("NotYetImplemented")]
  public void WhenDispatchTtlIsInTheFutureOwnershipShouldNotChange()
  {
    Assert.Fail("NotYetImplemented");
  }

  [Test]
  public async Task FullScenario()
  {
    // create session s1
    const string sessionId1 = $"{nameof(TableStorageTestBase)}.{nameof(FullScenario)}.Session1";
    const string t1         = "taskId1";
    const string t2         = "taskId2";
    const string t3         = "taskId3";
    const string t4         = "taskId4";
    const string o1         = "output1";
    const string o2         = "output2";
    const string o3         = "output3";
    const string o4         = "output4";
    const string t1d1       = "task1DispatchId1";
    var          ttlT1D1    = DateTime.UtcNow + TimeSpan.FromMinutes(5);

    Assert.False(await TableStorage.ListSessionsAsync()
                                   .ContainsAsync(sessionId1));

    Assert.AreEqual(CreateSessionReply.ResultOneofCase.Ok,
                    (await TableStorage.CreateSessionAsync(new()
                                                          {
                                                            Root = new()
                                                                   {
                                                                     Id = sessionId1,
                                                                     DefaultTaskOption = new()
                                                                                         {
                                                                                           MaxDuration = Duration.FromTimeSpan(TimeSpan.FromMinutes(2)),
                                                                                           MaxRetries  = 2,
                                                                                           Priority    = 2,
                                                                                         },
                                                                   },
                                                          })).ResultCase);


    // check that session can be listed
    {
      Assert.True(await TableStorage.ListSessionsAsync()
                                    .ContainsAsync(sessionId1));
    }


    // Initialize creation of task t1 with 3 outputs o1, o2, o3
    {

      Assert.IsFalse(await TableStorage.ListResultsAsync(sessionId1,
                                                         CancellationToken.None).ContainsAsync(o1));
      Assert.IsFalse(await TableStorage.ListResultsAsync(sessionId1,
                                                         CancellationToken.None).ContainsAsync(o2));
      Assert.IsFalse(await TableStorage.ListResultsAsync(sessionId1,
                                                         CancellationToken.None).ContainsAsync(o3));
      Assert.IsFalse(await TableStorage.ListResultsAsync(sessionId1,
                                                         CancellationToken.None).ContainsAsync(o4));


      await TableStorage.InitializeTaskCreation(sessionId1,
                                                sessionId1,
                                                new()
                                                {
                                                  
                                                  MaxDuration = Duration.FromTimeSpan(TimeSpan.FromMinutes(3)),
                                                  MaxRetries  = 3,
                                                  Priority    = 3,
                                                },
                                                new[]
                                                {
                                                  new CreateSmallTaskRequest.Types.TaskRequest()
                                                  {
                                                    Id = t1,
                                                    ExpectedOutputKeys =
                                                    {
                                                      o1,
                                                      o2,
                                                      o3,
                                                    },
                                                  },
                                                });

      Assert.IsTrue(await TableStorage.ListResultsAsync(sessionId1,
                                                        CancellationToken.None)
                                      .ContainsAsync(o1));
      Assert.IsTrue(await TableStorage.ListResultsAsync(sessionId1,
                                                        CancellationToken.None)
                                      .ContainsAsync(o2));
      Assert.IsTrue(await TableStorage.ListResultsAsync(sessionId1,
                                                        CancellationToken.None)
                                      .ContainsAsync(o3));
      {
        var result = await TableStorage.GetResult(sessionId1,
                                                  o1);
        Assert.AreEqual(t1,
                        result.Owner);
        Assert.AreEqual(o1,
                        result.Key);
        Assert.AreEqual(false,
                        result.IsResultAvailable);
        Assert.AreEqual(sessionId1,
                        result.SessionId);
      }
      {
        var result = await TableStorage.GetResult(sessionId1,
                                                  o2);
        Assert.AreEqual(t1,
                        result.Owner);
        Assert.AreEqual(o2,
                        result.Key);
        Assert.AreEqual(false,
                        result.IsResultAvailable);
        Assert.AreEqual(sessionId1,
                        result.SessionId);
      }
      {
        var result = await TableStorage.GetResult(sessionId1,
                                                  o3);
        Assert.AreEqual(t1,
                        result.Owner);
        Assert.AreEqual(o3,
                        result.Key);
        Assert.AreEqual(false,
                        result.IsResultAvailable);
        Assert.AreEqual(sessionId1,
                        result.SessionId);
      }
    }


    // check that the task can be listed as creating
    {
      Assert.AreEqual(t1,
                      await TableStorage.ListTasksAsync(new()
                                                        {
                                                          Unknown = new()
                                                                    {
                                                                      SessionId = sessionId1,
                                                                    },
                                                        },
                                                        CancellationToken.None)
                                        .SingleAsync(CancellationToken.None));
      Assert.AreEqual(t1,
                      await TableStorage.ListTasksAsync(new()
                                                        {
                                                          Unknown = new()
                                                                    {
                                                                      SessionId = sessionId1,
                                                                    },
                                                          Included = new()
                                                                     {
                                                                       IncludedStatuses =
                                                                       {
                                                                         TaskStatus.Creating,
                                                                       },
                                                                     },
                                                        },
                                                        CancellationToken.None)
                                        .SingleAsync(CancellationToken.None));

      Assert.AreEqual(TaskStatus.Creating,
                      (await TableStorage.ReadTaskAsync(t1,
                                                        CancellationToken.None)).Status);
    }

    // check the task count
    {
      var count = (await TableStorage.CountTasksAsync(new()
                                                      {
                                                        Unknown = new()
                                                                  {
                                                                    SessionId = sessionId1,
                                                                  },
                                                      },
                                                      CancellationToken.None)).ToList();

      Assert.IsNotNull(count);

      Assert.IsNotNull(count);
      Assert.AreEqual(1,
                      count.Count);
      Assert.DoesNotThrow(() => Assert.AreEqual(1,
                                                count.Single(tuple => tuple.Status == TaskStatus.Creating).Count));
    }

    // Set the status as submitted and check.
    {
      await TableStorage.UpdateTaskStatusAsync(t1,
                                               TaskStatus.Submitted,
                                               CancellationToken.None);

      Assert.AreEqual(t1,
                      await TableStorage.ListTasksAsync(new()
                                                        {
                                                          Unknown = new()
                                                                    {
                                                                      SessionId = sessionId1,
                                                                    },
                                                          Included = new()
                                                                     {
                                                                       IncludedStatuses =
                                                                       {
                                                                         TaskStatus.Submitted,
                                                                       },
                                                                     },
                                                        },
                                                        CancellationToken.None)
                                        .SingleAsync(CancellationToken.None));

      var count = (await TableStorage.CountTasksAsync(new()
                                                      {
                                                        Unknown = new()
                                                                  {
                                                                    SessionId = sessionId1,
                                                                  },
                                                      },
                                                      CancellationToken.None)).ToList();

      Assert.IsNotNull(count);
      Assert.AreEqual(1,
                      count.Count);
      Assert.DoesNotThrow(() => Assert.AreEqual(1,
                                                count.Single(tuple => tuple.Status == TaskStatus.Submitted).Count));
    }

    // acquire a dispatch for the task
    {
      Assert.IsTrue(await TableStorage.TryAcquireDispatchAsync(t1d1,
                                                               t1,
                                                               ttlT1D1));
    }


    // check that the dispatch can be listed

    {
      var dispatch = await TableStorage.GetDispatchAsync(t1d1,
                                                         CancellationToken.None);

      Assert.NotNull(dispatch);
      Assert.AreEqual(t1d1,
                      dispatch.Id);
      Assert.AreEqual(1,
                      dispatch.Attempt);
      Assert.AreEqual(t1,
                      dispatch.TaskId);
      Assert.AreEqual(ttlT1D1,
                      dispatch.TimeToLive);

      Assert.Contains(t1d1,
                      await TableStorage.ListDispatchAsync(t1,
                                                           CancellationToken.None)
                                        .ToListAsync(CancellationToken.None));
    }

    // check that another dispatch cannot be acquired
    {
      Assert.ThrowsAsync<KeyNotFoundException>(() => TableStorage.GetDispatchAsync("fakeDispatchId",
                                                                        CancellationToken.None));
    }

    // set t1 to dispatched and check the status and list tasks and count tasks
    {
      await TableStorage.UpdateTaskStatusAsync(t1,
                                               TaskStatus.Dispatched,
                                               CancellationToken.None);

      Assert.AreEqual(t1,
                      await TableStorage.ListTasksAsync(new()
                                                        {
                                                          Unknown = new()
                                                                    {
                                                                      SessionId = sessionId1,
                                                                    },
                                                          Included = new()
                                                                     {
                                                                       IncludedStatuses =
                                                                       {
                                                                         TaskStatus.Dispatched,
                                                                       },
                                                                     },
                                                        },
                                                        CancellationToken.None)
                                        .SingleAsync(CancellationToken.None));

      var count = (await TableStorage.CountTasksAsync(new()
                                                      {
                                                        Unknown = new()
                                                                  {
                                                                    SessionId = sessionId1,
                                                                  },
                                                      },
                                                      CancellationToken.None)).ToList();

      Assert.IsNotEmpty(count);
      Assert.AreEqual(1,
                      count.Count);
      Assert.DoesNotThrow(() => Assert.AreEqual(1,
                                                count.Single(tuple => tuple.Status == TaskStatus.Dispatched).Count));
    }

    // set t1 to running and check the status and list tasks and count tasks
    {
      await TableStorage.UpdateDispatch(t1d1,
                                        TaskStatus.Processing,
                                        CancellationToken.None);


      var dispatch = await TableStorage.GetDispatchAsync(t1d1,
                                                         CancellationToken.None);

      Assert.NotNull(dispatch);
      Assert.Contains(TaskStatus.Processing,
                      dispatch.Statuses
                              .Select(pair => pair.Status)
                              .ToList());

      Assert.AreEqual(t1,
                      await TableStorage.ListTasksAsync(new()
                                                        {
                                                          Unknown = new()
                                                                    {
                                                                      SessionId = sessionId1,
                                                                    },
                                                          Included = new()
                                                                     {
                                                                       IncludedStatuses =
                                                                       {
                                                                         TaskStatus.Processing,
                                                                       },
                                                                     },
                                                        },
                                                        CancellationToken.None)
                                        .SingleAsync(CancellationToken.None));

      var count = (await TableStorage.CountTasksAsync(new()
                                                      {
                                                        Unknown = new()
                                                                  {
                                                                    SessionId = sessionId1,
                                                                  },
                                                      },
                                                      CancellationToken.None)).ToList();

      Assert.IsNotEmpty(count);
      Assert.AreEqual(1,
                      count.Count);
      Assert.DoesNotThrow(() => Assert.AreEqual(1,
                                                count.Single(tuple => tuple.Status == TaskStatus.Processing).Count));
    }

    // create a new session s2 with parentId=t1
    Assert.AreEqual(CreateSessionReply.ResultOneofCase.Ok,
                    (await TableStorage.CreateSessionAsync(new()
                                                          {
                                                            SubSession = new()
                                                                         {
                                                                           RootId       = sessionId1,
                                                                           ParentTaskId = t1,
                                                                         },
                                                          })).ResultCase);


    // create 3 tasks :
    //  t2 take ownership of o1
    //  t3 has its own output o4
    //  t4 depends on the o1, o2 and o4 ; it take ownership of o3
    {
      await TableStorage.InitializeTaskCreation(sessionId1,
                                                t1,
                                                new()
                                                {
                                                  MaxDuration = Duration.FromTimeSpan(TimeSpan.FromMinutes(3)),
                                                  MaxRetries  = 3,
                                                  Priority    = 3,
                                                },
                                                new[]
                                                {
                                                  new CreateSmallTaskRequest.Types.TaskRequest
                                                  {
                                                    Id = t2,
                                                    ExpectedOutputKeys =
                                                    {
                                                      o1,
                                                    },
                                                  },
                                                  new CreateSmallTaskRequest.Types.TaskRequest
                                                  {
                                                    Id = t3,
                                                    ExpectedOutputKeys =
                                                    {
                                                      o4,
                                                    },
                                                  },
                                                  new CreateSmallTaskRequest.Types.TaskRequest
                                                  {
                                                    Id = t4,
                                                    ExpectedOutputKeys =
                                                    {
                                                      o3,
                                                    },
                                                    DataDependencies =
                                                    {
                                                      o1,
                                                      o2,
                                                      o4,
                                                    },
                                                  },
                                                });
    }
    // check that the tasks can be listed :
    //  filter = s1 => t1, t2, t3, t4
    //  filter = t1 => t1, t2, t3, t4
    {
      var filteredS1 = await TableStorage.ListTasksAsync(new()
                                                         {
                                                           Unknown = new()
                                                                     {
                                                                       SessionId = sessionId1,
                                                                     },
                                                         },
                                                         CancellationToken.None)
                                         .ToListAsync(CancellationToken.None);
      Assert.Contains(t1,
                      filteredS1);
      Assert.Contains(t2,
                      filteredS1);
      Assert.Contains(t3,
                      filteredS1);
      Assert.Contains(t4,
                      filteredS1);

      var filteredT1 = await TableStorage.ListTasksAsync(new()
                                                         {
                                                           Known = new()
                                                                   {
                                                                     TaskIds =
                                                                     {
                                                                       t1,
                                                                     },
                                                                   },
                                                         },
                                                         CancellationToken.None)
                                         .ToListAsync(CancellationToken.None);
      Assert.Contains(t1,
                      filteredT1);
      Assert.Contains(t2,
                      filteredT1);
      Assert.Contains(t3,
                      filteredT1);
      Assert.Contains(t4,
                      filteredT1);
    }

    // check that the ownership of o1 and o3 has changed
    {
      var result = await TableStorage.GetResult(sessionId1,
                                                o1);
      Assert.AreEqual(t2,
                      result.Owner);
      Assert.AreEqual(o1,
                      result.Key);
      Assert.AreEqual(false,
                      result.IsResultAvailable);
      Assert.AreEqual(sessionId1,
                      result.SessionId);
    }
    {
      var result = await TableStorage.GetResult(sessionId1,
                                                o3);
      Assert.AreEqual(t4,
                      result.Owner);
      Assert.AreEqual(o3,
                      result.Key);
      Assert.AreEqual(false,
                      result.IsResultAvailable);
      Assert.AreEqual(sessionId1,
                      result.SessionId);
    }

    // check the tasks count (1 running, 3 submitted)
    {
      var count = (await TableStorage.CountTasksAsync(new()
                                                      {
                                                        Unknown = new()
                                                                  {
                                                                    SessionId = sessionId1,
                                                                  },
                                                      },
                                                      CancellationToken.None)).ToList();

      Assert.IsNotEmpty(count);
      Assert.AreEqual(2,
                      count.Count);

      Assert.DoesNotThrow(() => Assert.AreEqual(1,
                                                count.Single(tuple => tuple.Status == TaskStatus.Processing).Count));
      Assert.DoesNotThrow(() => Assert.AreEqual(3,
                                                count.Single(tuple => tuple.Status == TaskStatus.Creating).Count));
    }

    // complete t1 status and provide result for o2
    {
      await TableStorage.SetResult(t1,
                                   o2,
                                   Array.Empty<byte>(),
                                   CancellationToken.None);

      await TableStorage.UpdateTaskStatusAsync(t1,
                                               TaskStatus.Completed,
                                               CancellationToken.None);

      Assert.AreEqual(t1,
                      await TableStorage.ListTasksAsync(new()
                                                        {
                                                          Unknown = new()
                                                                    {
                                                                      SessionId = sessionId1,
                                                                    },
                                                          Included = new()
                                                                     {
                                                                       IncludedStatuses =
                                                                       {
                                                                         TaskStatus.Completed,
                                                                       },
                                                                     },
                                                        },
                                                        CancellationToken.None)
                                        .SingleAsync(CancellationToken.None));
    }

    // check the task count
    {
      var count = (await TableStorage.CountTasksAsync(new()
                                                      {
                                                        Unknown = new()
                                                                  {
                                                                    SessionId = sessionId1,
                                                                  },
                                                      },
                                                      CancellationToken.None)).ToList();

      Assert.IsNotEmpty(count);
      Assert.AreEqual(2,
                      count.Count);
      Assert.DoesNotThrow(() => Assert.AreEqual(1,
                                                count.Single(tuple => tuple.Status == TaskStatus.Completed).Count));
      Assert.DoesNotThrow(() => Assert.AreEqual(3,
                                                count.Single(tuple => tuple.Status == TaskStatus.Creating).Count));
    }

    // check that o2 can be retrieved
    {
      Assert.IsTrue((await TableStorage.GetResult(sessionId1,
                                                  o2)).IsResultAvailable);
    }


    // check that o1 and o3 cannot be retrieved
    {
      Assert.IsFalse((await TableStorage.GetResult(sessionId1,o1)).IsResultAvailable);
      Assert.IsFalse((await TableStorage.GetResult(sessionId1,
                                                   o3)).IsResultAvailable);
    }
  }
}
