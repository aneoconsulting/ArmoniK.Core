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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Adapters.MongoDB.Table;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

using Output = ArmoniK.Core.Common.Storage.Output;
using TaskOptions = ArmoniK.Core.Common.Storage.TaskOptions;

namespace ArmoniK.Core.Adapters.MongoDB.Tests;

[TestFixture(TestOf = typeof(ExpressionsBuilders))]
internal class ExpressionsBuildersFieldFilterExpressionTests
{
  private static readonly TaskOptions _taskOptions = new(new Dictionary<string, string>(),
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
    var func = ExpressionsBuilders.FieldFilterExpression<TaskData, string>(model => model.SessionId,
                                                                           new[]
                                                                           {
                                                                             "Session",
                                                                           })
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
                             _taskOptions,
                             new Output(true,
                                        ""));

    Assert.IsTrue(func(model));
  }

  [Test]
  public void ShouldRejectOtherSession()
  {
    var func = ExpressionsBuilders.FieldFilterExpression<TaskData, string>(model => model.SessionId,
                                                                           new[]
                                                                           {
                                                                             "Session",
                                                                           })
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
                             _taskOptions,
                             new Output(true,
                                        ""));

    Assert.IsFalse(func(model));
  }

  [Test]
  public void ShouldExcludeSession()
  {
    var func = ExpressionsBuilders.FieldFilterExpression<TaskData, string>(model => model.SessionId,
                                                                           new[]
                                                                           {
                                                                             "Session",
                                                                           },
                                                                           false)
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
                             _taskOptions,
                             new Output(true,
                                        ""));

    Assert.IsFalse(func(model));
  }

  [Test]
  public void ShouldIncludeOtherSession()
  {
    var func = ExpressionsBuilders.FieldFilterExpression<TaskData, string>(model => model.SessionId,
                                                                           new[]
                                                                           {
                                                                             "Session",
                                                                           },
                                                                           false)
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
                             _taskOptions,
                             new Output(true,
                                        ""));

    Assert.IsTrue(func(model));
  }

  [Test]
  public void ShouldRecognizeStatus()
  {
    var func = ExpressionsBuilders.FieldFilterExpression<TaskData, TaskStatus>(model => model.Status,
                                                                               new[]
                                                                               {
                                                                                 TaskStatus.Completed,
                                                                               })
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
                             _taskOptions,
                             new Output(true,
                                        ""));

    Assert.IsTrue(func(model));
  }

  [Test]
  public void ShouldExcludeStatus()
  {
    var func = ExpressionsBuilders.FieldFilterExpression<TaskData, TaskStatus>(model => model.Status,
                                                                               new[]
                                                                               {
                                                                                 TaskStatus.Completed,
                                                                               },
                                                                               false)
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
                             _taskOptions,
                             new Output(true,
                                        ""));

    Assert.IsFalse(func(model));
  }

  [Test]
  public void ShouldRecognizeMultipleStatus()
  {
    var func = ExpressionsBuilders.FieldFilterExpression<TaskData, TaskStatus>(model => model.Status,
                                                                               new[]
                                                                               {
                                                                                 TaskStatus.Completed,
                                                                                 TaskStatus.Cancelled,
                                                                               })
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
                             _taskOptions,
                             new Output(true,
                                        ""));

    Assert.IsTrue(func(model));
  }

  [Test]
  public void ShouldExcludeMultipleStatus()
  {
    var func = ExpressionsBuilders.FieldFilterExpression<TaskData, TaskStatus>(model => model.Status,
                                                                               new[]
                                                                               {
                                                                                 TaskStatus.Completed,
                                                                                 TaskStatus.Cancelled,
                                                                               },
                                                                               false)
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
                             _taskOptions,
                             new Output(true,
                                        ""));

    Assert.IsFalse(func(model));
  }

  [Test]
  public void ShouldRejectOtherStatus()
  {
    var func = ExpressionsBuilders.FieldFilterExpression<TaskData, TaskStatus>(model => model.Status,
                                                                               new[]
                                                                               {
                                                                                 TaskStatus.Completed,
                                                                               })
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
                             TaskStatus.Cancelled,
                             _taskOptions,
                             new Output(true,
                                        ""));

    Assert.IsFalse(func(model));
  }

  [Test]
  public void ShouldIncludeOtherStatus()
  {
    var func = ExpressionsBuilders.FieldFilterExpression<TaskData, TaskStatus>(model => model.Status,
                                                                               new[]
                                                                               {
                                                                                 TaskStatus.Completed,
                                                                               },
                                                                               false)
                                  .Compile(true);

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
                             TaskStatus.Cancelled,
                             _taskOptions,
                             new Output(true,
                                        ""));

    Assert.IsTrue(func(model));
  }

  [Test]
  public void ShouldRejectOtherMultipleStatus()
  {
    var func = ExpressionsBuilders.FieldFilterExpression<TaskData, TaskStatus>(model => model.Status,
                                                                               new[]
                                                                               {
                                                                                 TaskStatus.Completed,
                                                                                 TaskStatus.Cancelling,
                                                                               })
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
                             TaskStatus.Cancelled,
                             _taskOptions,
                             new Output(true,
                                        ""));

    Assert.IsFalse(func(model));
  }

  [Test]
  public void ShouldIncludeOtherMultipleStatus()
  {
    var func = ExpressionsBuilders.FieldFilterExpression<TaskData, TaskStatus>(model => model.Status,
                                                                               new[]
                                                                               {
                                                                                 TaskStatus.Completed,
                                                                                 TaskStatus.Cancelling,
                                                                               },
                                                                               false)
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
                             TaskStatus.Cancelled,
                             _taskOptions,
                             new Output(true,
                                        ""));

    Assert.IsTrue(func(model));
  }

  [Test]
  public void ShouldRecognizeTask()
  {
    var func = ExpressionsBuilders.FieldFilterExpression<TaskData, string>(model => model.TaskId,
                                                                           new[]
                                                                           {
                                                                             "Task",
                                                                           })
                                  .Compile();

    var model = new TaskData("OtherSession",
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
                             TaskStatus.Cancelled,
                             _taskOptions,
                             new Output(true,
                                        ""));

    Assert.IsTrue(func(model));
  }

  [Test]
  public void ShouldExcludeTask()
  {
    var func = ExpressionsBuilders.FieldFilterExpression<TaskData, string>(model => model.TaskId,
                                                                           new[]
                                                                           {
                                                                             "Task",
                                                                           },
                                                                           false)
                                  .Compile();

    var model = new TaskData("OtherSession",
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
                             TaskStatus.Cancelled,
                             _taskOptions,
                             new Output(true,
                                        ""));

    Assert.IsFalse(func(model));
  }

  [Test]
  public void ShouldRecognizeMultipleTask()
  {
    var func = ExpressionsBuilders.FieldFilterExpression<TaskData, string>(model => model.TaskId,
                                                                           new[]
                                                                           {
                                                                             "Task",
                                                                             "Task2",
                                                                           })
                                  .Compile();

    var model = new TaskData("OtherSession",
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
                             TaskStatus.Cancelled,
                             _taskOptions,
                             new Output(true,
                                        ""));

    Assert.IsTrue(func(model));
  }

  [Test]
  public void ShouldExcludeMultipleTask()
  {
    var func = ExpressionsBuilders.FieldFilterExpression<TaskData, string>(model => model.TaskId,
                                                                           new[]
                                                                           {
                                                                             "Task",
                                                                             "Task2",
                                                                           },
                                                                           false)
                                  .Compile();

    var model = new TaskData("OtherSession",
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
                             TaskStatus.Cancelled,
                             _taskOptions,
                             new Output(true,
                                        ""));

    Assert.IsFalse(func(model));
  }

  [Test]
  public void ShouldRejectOtherTask()
  {
    var func = ExpressionsBuilders.FieldFilterExpression<TaskData, string>(model => model.TaskId,
                                                                           new[]
                                                                           {
                                                                             "Task",
                                                                           })
                                  .Compile();

    var model = new TaskData("OtherSession",
                             "OtherTask",
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
                             TaskStatus.Cancelled,
                             _taskOptions,
                             new Output(true,
                                        ""));

    Assert.IsFalse(func(model));
  }

  [Test]
  public void ShouldIncludeOtherTask()
  {
    var func = ExpressionsBuilders.FieldFilterExpression<TaskData, string>(model => model.TaskId,
                                                                           new[]
                                                                           {
                                                                             "Task",
                                                                           },
                                                                           false)
                                  .Compile(true);

    var model = new TaskData("OtherSession",
                             "OtherTask",
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
                             TaskStatus.Cancelled,
                             _taskOptions,
                             new Output(true,
                                        ""));

    Assert.IsTrue(func(model));
  }

  [Test]
  public void ShouldRejectOtherMultipleTask()
  {
    var func = ExpressionsBuilders.FieldFilterExpression<TaskData, string>(model => model.TaskId,
                                                                           new[]
                                                                           {
                                                                             "Task",
                                                                             "Task2",
                                                                           })
                                  .Compile();

    var model = new TaskData("OtherSession",
                             "OtherTask",
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
                             TaskStatus.Cancelled,
                             _taskOptions,
                             new Output(true,
                                        ""));

    Assert.IsFalse(func(model));
  }

  [Test]
  public void ShouldIncludeOtherMultipleTask()
  {
    var func = ExpressionsBuilders.FieldFilterExpression<TaskData, string>(model => model.TaskId,
                                                                           new[]
                                                                           {
                                                                             "Task",
                                                                             "Task2",
                                                                           },
                                                                           false)
                                  .Compile();

    var model = new TaskData("OtherSession",
                             "OtherTask",
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
                             TaskStatus.Cancelled,
                             _taskOptions,
                             new Output(true,
                                        ""));

    Assert.IsTrue(func(model));
  }
}
