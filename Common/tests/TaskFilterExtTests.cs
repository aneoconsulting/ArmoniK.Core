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

using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

[TestFixture(TestOf = typeof(TaskFilterExt))]
internal class TaskFilterExtTests
{
  private static readonly TaskOptions TaskOptions = new(new Dictionary<string, string>(),
                                                        TimeSpan.Zero,
                                                        0,
                                                        0,
                                                        "part1",
                                                        "ApplicationName",
                                                        "ApplicationVersion",
                                                        "applicationNamespace",
                                                        "applicationService",
                                                        "engineType");

  [Test]
  public void ShouldRecognizeSession()
  {
    var func = new TaskFilter
               {
                 Session = new TaskFilter.Types.IdsRequest
                           {
                             Ids =
                             {
                               "Session",
                             },
                           },
               }.ToFilterExpression()
                .Compile();

    var model = new TaskData("Session",
                             "TaskCompletedId",
                             "OwnerPodId",
                             "OwnerPodName",
                             "PayloadId",
                             new[]
                             {
                               "parent1",
                             },
                             new[]
                             {
                               "dependency1",
                             },
                             new[]
                             {
                               "output1",
                             },
                             Array.Empty<string>(),
                             TaskStatus.Completed,
                             TaskOptions,
                             new Output(true,
                                        ""));

    Assert.IsTrue(func(model));
  }

  [Test]
  public void ShouldRejectOtherSession()
  {
    var func = new TaskFilter
               {
                 Session = new TaskFilter.Types.IdsRequest
                           {
                             Ids =
                             {
                               "Session",
                             },
                           },
               }.ToFilterExpression()
                .Compile();

    var model = new TaskData("OtherSession",
                             "TaskCompletedId",
                             "OwnerPodId",
                             "OwnerPodName",
                             "PayloadId",
                             new[]
                             {
                               "parent1",
                             },
                             new[]
                             {
                               "dependency1",
                             },
                             new[]
                             {
                               "output1",
                             },
                             Array.Empty<string>(),
                             TaskStatus.Completed,
                             TaskOptions,
                             new Output(true,
                                        ""));

    Assert.IsFalse(func(model));
  }

  [Test]
  public void ShouldRecognizeStatus()
  {
    var func = new TaskFilter
               {
                 Included = new TaskFilter.Types.StatusesRequest
                            {
                              Statuses =
                              {
                                Api.gRPC.V1.TaskStatus.Completed,
                              },
                            },
                 Session = new TaskFilter.Types.IdsRequest
                           {
                             Ids =
                             {
                               "SessionId",
                             },
                           },
               }.ToFilterExpression()
                .Compile(true);


    var model = new TaskData("SessionId",
                             "TaskCompletedId",
                             "OwnerPodId",
                             "OwnerPodName",
                             "PayloadId",
                             new[]
                             {
                               "parent1",
                             },
                             new[]
                             {
                               "dependency1",
                             },
                             new[]
                             {
                               "output1",
                             },
                             Array.Empty<string>(),
                             TaskStatus.Completed,
                             TaskOptions,
                             new Output(true,
                                        ""));

    Assert.IsTrue(func(model));
  }

  [Test]
  public void ShouldExcludeStatus()
  {
    var func = new TaskFilter
               {
                 Excluded = new TaskFilter.Types.StatusesRequest
                            {
                              Statuses =
                              {
                                Api.gRPC.V1.TaskStatus.Completed,
                              },
                            },
                 Session = new TaskFilter.Types.IdsRequest
                           {
                             Ids =
                             {
                               "SessionId",
                             },
                           },
               }.ToFilterExpression()
                .Compile();

    var model = new TaskData("SessionId",
                             "TaskCompletedId",
                             "OwnerPodId",
                             "OwnerPodName",
                             "PayloadId",
                             new[]
                             {
                               "parent1",
                             },
                             new[]
                             {
                               "dependency1",
                             },
                             new[]
                             {
                               "output1",
                             },
                             Array.Empty<string>(),
                             TaskStatus.Completed,
                             TaskOptions,
                             new Output(true,
                                        ""));

    Assert.IsFalse(func(model));
  }

  [Test]
  public void ShouldRecognizeMultipleStatus()
  {
    var func = new TaskFilter
               {
                 Included = new TaskFilter.Types.StatusesRequest
                            {
                              Statuses =
                              {
                                Api.gRPC.V1.TaskStatus.Completed,
                                Api.gRPC.V1.TaskStatus.Cancelled,
                              },
                            },
                 Session = new TaskFilter.Types.IdsRequest
                           {
                             Ids =
                             {
                               "SessionId",
                             },
                           },
               }.ToFilterExpression()
                .Compile();

    var model = new TaskData("SessionId",
                             "TaskCompletedId",
                             "OwnerPodId",
                             "OwnerPodName",
                             "PayloadId",
                             new[]
                             {
                               "parent1",
                             },
                             new[]
                             {
                               "dependency1",
                             },
                             new[]
                             {
                               "output1",
                             },
                             Array.Empty<string>(),
                             TaskStatus.Completed,
                             TaskOptions,
                             new Output(true,
                                        ""));

    Assert.IsTrue(func(model));
  }

  [Test]
  public void ShouldExcludeMultipleStatus()
  {
    var func = new TaskFilter
               {
                 Excluded = new TaskFilter.Types.StatusesRequest
                            {
                              Statuses =
                              {
                                Api.gRPC.V1.TaskStatus.Completed,
                                Api.gRPC.V1.TaskStatus.Cancelled,
                              },
                            },
                 Session = new TaskFilter.Types.IdsRequest
                           {
                             Ids =
                             {
                               "SessionId",
                             },
                           },
               }.ToFilterExpression()
                .Compile();

    var model = new TaskData("SessionId",
                             "TaskCompletedId",
                             "OwnerPodId",
                             "OwnerPodName",
                             "PayloadId",
                             new[]
                             {
                               "parent1",
                             },
                             new[]
                             {
                               "dependency1",
                             },
                             new[]
                             {
                               "output1",
                             },
                             Array.Empty<string>(),
                             TaskStatus.Completed,
                             TaskOptions,
                             new Output(true,
                                        ""));

    Assert.IsFalse(func(model));
  }

  [Test]
  public void ShouldRejectOtherStatus()
  {
    var func = new TaskFilter
               {
                 Included = new TaskFilter.Types.StatusesRequest
                            {
                              Statuses =
                              {
                                Api.gRPC.V1.TaskStatus.Completed,
                              },
                            },
                 Session = new TaskFilter.Types.IdsRequest
                           {
                             Ids =
                             {
                               "SessionId",
                             },
                           },
               }.ToFilterExpression()
                .Compile();

    var model = new TaskData("SessionId",
                             "Task",
                             "OwnerPodId",
                             "OwnerPodName",
                             "PayloadId",
                             new[]
                             {
                               "parent1",
                             },
                             new[]
                             {
                               "dependency1",
                             },
                             new[]
                             {
                               "output1",
                             },
                             Array.Empty<string>(),
                             TaskStatus.Error,
                             TaskOptions,
                             new Output(true,
                                        ""));

    Assert.IsFalse(func(model));
  }

  [Test]
  public void ShouldIncludeOtherStatus()
  {
    var func = new TaskFilter
               {
                 Excluded = new TaskFilter.Types.StatusesRequest
                            {
                              Statuses =
                              {
                                Api.gRPC.V1.TaskStatus.Completed,
                              },
                            },
                 Session = new TaskFilter.Types.IdsRequest
                           {
                             Ids =
                             {
                               "SessionId",
                             },
                           },
               }.ToFilterExpression()
                .Compile();

    var model = new TaskData("SessionId",
                             "TaskCompletedId",
                             "OwnerPodId",
                             "OwnerPodName",
                             "PayloadId",
                             new[]
                             {
                               "parent1",
                             },
                             new[]
                             {
                               "dependency1",
                             },
                             new[]
                             {
                               "output1",
                             },
                             Array.Empty<string>(),
                             TaskStatus.Error,
                             TaskOptions,
                             new Output(true,
                                        ""));

    Assert.IsTrue(func(model));
  }

  [Test]
  public void ShouldRejectOtherMultipleStatus()
  {
    var func = new TaskFilter
               {
                 Included = new TaskFilter.Types.StatusesRequest
                            {
                              Statuses =
                              {
                                Api.gRPC.V1.TaskStatus.Completed,
                                Api.gRPC.V1.TaskStatus.Cancelling,
                              },
                            },
                 Session = new TaskFilter.Types.IdsRequest
                           {
                             Ids =
                             {
                               "SessionId",
                             },
                           },
               }.ToFilterExpression()
                .Compile();

    var model = new TaskData("SessionId",
                             "TaskCompletedId",
                             "OwnerPodId",
                             "OwnerPodName",
                             "PayloadId",
                             new[]
                             {
                               "parent1",
                             },
                             new[]
                             {
                               "dependency1",
                             },
                             new[]
                             {
                               "output1",
                             },
                             Array.Empty<string>(),
                             TaskStatus.Error,
                             TaskOptions,
                             new Output(true,
                                        ""));

    Assert.IsFalse(func(model));
  }

  [Test]
  public void ShouldIncludeOtherMultipleStatus()
  {
    var func = new TaskFilter
               {
                 Excluded = new TaskFilter.Types.StatusesRequest
                            {
                              Statuses =
                              {
                                Api.gRPC.V1.TaskStatus.Completed,
                                Api.gRPC.V1.TaskStatus.Cancelling,
                              },
                            },
                 Session = new TaskFilter.Types.IdsRequest
                           {
                             Ids =
                             {
                               "SessionId",
                             },
                           },
               }.ToFilterExpression()
                .Compile();

    var model = new TaskData("SessionId",
                             "TaskCompletedId",
                             "OwnerPodId",
                             "OwnerPodName",
                             "PayloadId",
                             new[]
                             {
                               "parent1",
                             },
                             new[]
                             {
                               "dependency1",
                             },
                             new[]
                             {
                               "output1",
                             },
                             Array.Empty<string>(),
                             TaskStatus.Error,
                             TaskOptions,
                             new Output(true,
                                        ""));

    Assert.IsTrue(func(model));
  }

  [Test]
  public void ShouldRecognizeTask()
  {
    var func = new TaskFilter
               {
                 Task = new TaskFilter.Types.IdsRequest
                        {
                          Ids =
                          {
                            "Task",
                          },
                        },
               }.ToFilterExpression()
                .Compile();


    var model = new TaskData("SessionId",
                             "Task",
                             "OwnerPodId",
                             "OwnerPodName",
                             "PayloadId",
                             new[]
                             {
                               "parent1",
                             },
                             new[]
                             {
                               "dependency1",
                             },
                             new[]
                             {
                               "output1",
                             },
                             Array.Empty<string>(),
                             TaskStatus.Completed,
                             TaskOptions,
                             new Output(true,
                                        ""));

    Assert.IsTrue(func(model));
  }

  [Test]
  public void ShouldRecognizeMultipleTask()
  {
    var func = new TaskFilter
               {
                 Task = new TaskFilter.Types.IdsRequest
                        {
                          Ids =
                          {
                            "Task",
                            "Task2",
                          },
                        },
               }.ToFilterExpression()
                .Compile();


    var model = new TaskData("SessionId",
                             "Task",
                             "OwnerPodId",
                             "OwnerPodName",
                             "PayloadId",
                             new[]
                             {
                               "parent1",
                             },
                             new[]
                             {
                               "dependency1",
                             },
                             new[]
                             {
                               "output1",
                             },
                             Array.Empty<string>(),
                             TaskStatus.Completed,
                             TaskOptions,
                             new Output(true,
                                        ""));

    Assert.IsTrue(func(model));
  }

  [Test]
  public void ShouldRejectOtherTask()
  {
    var func = new TaskFilter
               {
                 Task = new TaskFilter.Types.IdsRequest
                        {
                          Ids =
                          {
                            "Task",
                          },
                        },
               }.ToFilterExpression()
                .Compile();


    var model = new TaskData("SessionId",
                             "TaskCompletedId",
                             "OwnerPodId",
                             "OwnerPodName",
                             "PayloadId",
                             new[]
                             {
                               "parent1",
                             },
                             new[]
                             {
                               "dependency1",
                             },
                             new[]
                             {
                               "output1",
                             },
                             Array.Empty<string>(),
                             TaskStatus.Completed,
                             TaskOptions,
                             new Output(true,
                                        ""));

    Assert.IsFalse(func(model));
  }

  [Test]
  public void ShouldRejectOtherMultipleTask()
  {
    var func = new TaskFilter
               {
                 Task = new TaskFilter.Types.IdsRequest
                        {
                          Ids =
                          {
                            "Task",
                            "Task2",
                          },
                        },
               }.ToFilterExpression()
                .Compile();


    var model = new TaskData("SessionId",
                             "TaskCompletedId",
                             "OwnerPodId",
                             "OwnerPodName",
                             "PayloadId",
                             new[]
                             {
                               "parent1",
                             },
                             new[]
                             {
                               "dependency1",
                             },
                             new[]
                             {
                               "output1",
                             },
                             Array.Empty<string>(),
                             TaskStatus.Completed,
                             TaskOptions,
                             new Output(true,
                                        ""));

    Assert.IsFalse(func(model));
  }
}
