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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Adapters.MongoDB.Table;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

using Output = ArmoniK.Core.Common.Storage.Output;

namespace ArmoniK.Core.Adapters.MongoDB.Tests;

[TestFixture(TestOf = typeof(ExpressionsBuilders))]
internal class ExpressionsBuildersFieldFilterExpressionTests
{
  [Test]
  public void ShouldRecognizeSession()
  {
    var func = ExpressionsBuilders.FieldFilterExpression(model => model.SessionId,
                                                         new[]
                                                         {
                                                           "Session",
                                                         })
                                  .Compile();

    var model = new TaskData("Session",
                             "TaskCompletedId",
                             "OwnerPodId",
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
                             TaskStatus.Failed,
                             "",
                             default,
                             DateTime.Now,
                             DateTime.Now + TimeSpan.FromSeconds(1),
                             DateTime.Now + TimeSpan.FromSeconds(10),
                             DateTime.Now + TimeSpan.FromSeconds(20),
                             DateTime.Now,
                             new Output(true,
                                        ""));

    Assert.IsTrue(func(model));
  }

  [Test]
  public void ShouldRejectOtherSession()
  {
    var func = ExpressionsBuilders.FieldFilterExpression(model => model.SessionId,
                                                         new[]
                                                         {
                                                           "Session",
                                                         })
                                  .Compile();

    var model = new TaskData("OtherSession",
                             "TaskCompletedId",
                             "OwnerPodId",
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
                             TaskStatus.Failed,
                             "",
                             default,
                             DateTime.Now,
                             DateTime.Now + TimeSpan.FromSeconds(1),
                             DateTime.Now + TimeSpan.FromSeconds(10),
                             DateTime.Now + TimeSpan.FromSeconds(20),
                             DateTime.Now,
                             new Output(true,
                                        ""));

    Assert.IsFalse(func(model));
  }

  [Test]
  public void ShouldExcludeSession()
  {
    var func = ExpressionsBuilders.FieldFilterExpression(model => model.SessionId,
                                                         new[]
                                                         {
                                                           "Session",
                                                         },
                                                         false)
                                  .Compile();

    var model = new TaskData("Session",
                             "TaskCompletedId",
                             "OwnerPodId",
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
                             TaskStatus.Failed,
                             "",
                             default,
                             DateTime.Now,
                             DateTime.Now + TimeSpan.FromSeconds(1),
                             DateTime.Now + TimeSpan.FromSeconds(10),
                             DateTime.Now + TimeSpan.FromSeconds(20),
                             DateTime.Now,
                             new Output(true,
                                        ""));

    Assert.IsFalse(func(model));
  }

  [Test]
  public void ShouldIncludeOtherSession()
  {
    var func = ExpressionsBuilders.FieldFilterExpression(model => model.SessionId,
                                                         new[]
                                                         {
                                                           "Session",
                                                         },
                                                         false)
                                  .Compile();

    var model = new TaskData("OtherSession",
                             "TaskCompletedId",
                             "OwnerPodId",
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
                             TaskStatus.Failed,
                             "",
                             default,
                             DateTime.Now,
                             DateTime.Now + TimeSpan.FromSeconds(1),
                             DateTime.Now + TimeSpan.FromSeconds(10),
                             DateTime.Now + TimeSpan.FromSeconds(20),
                             DateTime.Now,
                             new Output(true,
                                        ""));

    Assert.IsTrue(func(model));
  }

  [Test]
  public void ShouldRecognizeStatus()
  {
    var func = ExpressionsBuilders.FieldFilterExpression(model => model.Status,
                                                         new[]
                                                         {
                                                           TaskStatus.Completed,
                                                         })
                                  .Compile();


    var model = new TaskData("OtherSession",
                             "TaskCompletedId",
                             "OwnerPodId",
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
                             "",
                             default,
                             DateTime.Now,
                             DateTime.Now + TimeSpan.FromSeconds(1),
                             DateTime.Now + TimeSpan.FromSeconds(10),
                             DateTime.Now + TimeSpan.FromSeconds(20),
                             DateTime.Now,
                             new Output(true,
                                        ""));

    Assert.IsTrue(func(model));
  }

  [Test]
  public void ShouldExcludeStatus()
  {
    var func = ExpressionsBuilders.FieldFilterExpression(model => model.Status,
                                                         new[]
                                                         {
                                                           TaskStatus.Completed,
                                                         },
                                                         false)
                                  .Compile();

    var model = new TaskData("OtherSession",
                             "TaskCompletedId",
                             "OwnerPodId",
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
                             "",
                             default,
                             DateTime.Now,
                             DateTime.Now + TimeSpan.FromSeconds(1),
                             DateTime.Now + TimeSpan.FromSeconds(10),
                             DateTime.Now + TimeSpan.FromSeconds(20),
                             DateTime.Now,
                             new Output(true,
                                        ""));

    Assert.IsFalse(func(model));
  }

  [Test]
  public void ShouldRecognizeMultipleStatus()
  {
    var func = ExpressionsBuilders.FieldFilterExpression(model => model.Status,
                                                         new[]
                                                         {
                                                           TaskStatus.Completed,
                                                           TaskStatus.Canceled,
                                                         })
                                  .Compile();

    var model = new TaskData("OtherSession",
                             "TaskCompletedId",
                             "OwnerPodId",
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
                             "",
                             default,
                             DateTime.Now,
                             DateTime.Now + TimeSpan.FromSeconds(1),
                             DateTime.Now + TimeSpan.FromSeconds(10),
                             DateTime.Now + TimeSpan.FromSeconds(20),
                             DateTime.Now,
                             new Output(true,
                                        ""));

    Assert.IsTrue(func(model));
  }

  [Test]
  public void ShouldExcludeMultipleStatus()
  {
    var func = ExpressionsBuilders.FieldFilterExpression(model => model.Status,
                                                         new[]
                                                         {
                                                           TaskStatus.Completed,
                                                           TaskStatus.Canceled,
                                                         },
                                                         false)
                                  .Compile();

    var model = new TaskData("OtherSession",
                             "TaskCompletedId",
                             "OwnerPodId",
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
                             "",
                             default,
                             DateTime.Now,
                             DateTime.Now + TimeSpan.FromSeconds(1),
                             DateTime.Now + TimeSpan.FromSeconds(10),
                             DateTime.Now + TimeSpan.FromSeconds(20),
                             DateTime.Now,
                             new Output(true,
                                        ""));

    Assert.IsFalse(func(model));
  }

  [Test]
  public void ShouldRejectOtherStatus()
  {
    var func = ExpressionsBuilders.FieldFilterExpression(model => model.Status,
                                                         new[]
                                                         {
                                                           TaskStatus.Completed,
                                                         })
                                  .Compile();

    var model = new TaskData("OtherSession",
                             "TaskCompletedId",
                             "OwnerPodId",
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
                             TaskStatus.Canceled,
                             "",
                             default,
                             DateTime.Now,
                             DateTime.Now + TimeSpan.FromSeconds(1),
                             DateTime.Now + TimeSpan.FromSeconds(10),
                             DateTime.Now + TimeSpan.FromSeconds(20),
                             DateTime.Now,
                             new Output(true,
                                        ""));

    Assert.IsFalse(func(model));
  }

  [Test]
  public void ShouldIncludeOtherStatus()
  {
    var func = ExpressionsBuilders.FieldFilterExpression(model => model.Status,
                                                         new[]
                                                         {
                                                           TaskStatus.Completed,
                                                         },
                                                         false)
                                  .Compile(true);

    var model = new TaskData("OtherSession",
                             "TaskCompletedId",
                             "OwnerPodId",
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
                             TaskStatus.Canceled,
                             "",
                             default,
                             DateTime.Now,
                             DateTime.Now + TimeSpan.FromSeconds(1),
                             DateTime.Now + TimeSpan.FromSeconds(10),
                             DateTime.Now + TimeSpan.FromSeconds(20),
                             DateTime.Now,
                             new Output(true,
                                        ""));

    Assert.IsTrue(func(model));
  }

  [Test]
  public void ShouldRejectOtherMultipleStatus()
  {
    var func = ExpressionsBuilders.FieldFilterExpression(model => model.Status,
                                                         new[]
                                                         {
                                                           TaskStatus.Completed,
                                                           TaskStatus.Canceling,
                                                         })
                                  .Compile();

    var model = new TaskData("OtherSession",
                             "TaskCompletedId",
                             "OwnerPodId",
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
                             TaskStatus.Canceled,
                             "",
                             default,
                             DateTime.Now,
                             DateTime.Now + TimeSpan.FromSeconds(1),
                             DateTime.Now + TimeSpan.FromSeconds(10),
                             DateTime.Now + TimeSpan.FromSeconds(20),
                             DateTime.Now,
                             new Output(true,
                                        ""));

    Assert.IsFalse(func(model));
  }

  [Test]
  public void ShouldIncludeOtherMultipleStatus()
  {
    var func = ExpressionsBuilders.FieldFilterExpression(model => model.Status,
                                                         new[]
                                                         {
                                                           TaskStatus.Completed,
                                                           TaskStatus.Canceling,
                                                         },
                                                         false)
                                  .Compile();

    var model = new TaskData("OtherSession",
                             "TaskCompletedId",
                             "OwnerPodId",
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
                             TaskStatus.Canceled,
                             "",
                             default,
                             DateTime.Now,
                             DateTime.Now + TimeSpan.FromSeconds(1),
                             DateTime.Now + TimeSpan.FromSeconds(10),
                             DateTime.Now + TimeSpan.FromSeconds(20),
                             DateTime.Now,
                             new Output(true,
                                        ""));

    Assert.IsTrue(func(model));
  }

  [Test]
  public void ShouldRecognizeTask()
  {
    var func = ExpressionsBuilders.FieldFilterExpression(model => model.TaskId,
                                                         new[]
                                                         {
                                                           "Task",
                                                         })
                                  .Compile();

    var model = new TaskData("OtherSession",
                             "Task",
                             "OwnerPodId",
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
                             TaskStatus.Canceled,
                             "",
                             default,
                             DateTime.Now,
                             DateTime.Now + TimeSpan.FromSeconds(1),
                             DateTime.Now + TimeSpan.FromSeconds(10),
                             DateTime.Now + TimeSpan.FromSeconds(20),
                             DateTime.Now,
                             new Output(true,
                                        ""));

    Assert.IsTrue(func(model));
  }

  [Test]
  public void ShouldExcludeTask()
  {
    var func = ExpressionsBuilders.FieldFilterExpression(model => model.TaskId,
                                                         new[]
                                                         {
                                                           "Task",
                                                         },
                                                         false)
                                  .Compile();

    var model = new TaskData("OtherSession",
                             "Task",
                             "OwnerPodId",
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
                             TaskStatus.Canceled,
                             "",
                             default,
                             DateTime.Now,
                             DateTime.Now + TimeSpan.FromSeconds(1),
                             DateTime.Now + TimeSpan.FromSeconds(10),
                             DateTime.Now + TimeSpan.FromSeconds(20),
                             DateTime.Now,
                             new Output(true,
                                        ""));

    Assert.IsFalse(func(model));
  }

  [Test]
  public void ShouldRecognizeMultipleTask()
  {
    var func = ExpressionsBuilders.FieldFilterExpression(model => model.TaskId,
                                                         new[]
                                                         {
                                                           "Task",
                                                           "Task2",
                                                         })
                                  .Compile();

    var model = new TaskData("OtherSession",
                             "Task",
                             "OwnerPodId",
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
                             TaskStatus.Canceled,
                             "",
                             default,
                             DateTime.Now,
                             DateTime.Now + TimeSpan.FromSeconds(1),
                             DateTime.Now + TimeSpan.FromSeconds(10),
                             DateTime.Now + TimeSpan.FromSeconds(20),
                             DateTime.Now,
                             new Output(true,
                                        ""));

    Assert.IsTrue(func(model));
  }

  [Test]
  public void ShouldExcludeMultipleTask()
  {
    var func = ExpressionsBuilders.FieldFilterExpression(model => model.TaskId,
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
                             TaskStatus.Canceled,
                             "",
                             default,
                             DateTime.Now,
                             DateTime.Now + TimeSpan.FromSeconds(1),
                             DateTime.Now + TimeSpan.FromSeconds(10),
                             DateTime.Now + TimeSpan.FromSeconds(20),
                             DateTime.Now,
                             new Output(true,
                                        ""));

    Assert.IsFalse(func(model));
  }

  [Test]
  public void ShouldRejectOtherTask()
  {
    var func = ExpressionsBuilders.FieldFilterExpression(model => model.TaskId,
                                                         new[]
                                                         {
                                                           "Task",
                                                         })
                                  .Compile();

    var model = new TaskData("OtherSession",
                             "OtherTask",
                             "OwnerPodId",
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
                             TaskStatus.Canceled,
                             "",
                             default,
                             DateTime.Now,
                             DateTime.Now + TimeSpan.FromSeconds(1),
                             DateTime.Now + TimeSpan.FromSeconds(10),
                             DateTime.Now + TimeSpan.FromSeconds(20),
                             DateTime.Now,
                             new Output(true,
                                        ""));

    Assert.IsFalse(func(model));
  }

  [Test]
  public void ShouldIncludeOtherTask()
  {
    var func = ExpressionsBuilders.FieldFilterExpression(model => model.TaskId,
                                                         new[]
                                                         {
                                                           "Task",
                                                         },
                                                         false)
                                  .Compile(true);

    var model = new TaskData("OtherSession",
                             "OtherTask",
                             "OwnerPodId",
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
                             TaskStatus.Canceled,
                             "",
                             default,
                             DateTime.Now,
                             DateTime.Now + TimeSpan.FromSeconds(1),
                             DateTime.Now + TimeSpan.FromSeconds(10),
                             DateTime.Now + TimeSpan.FromSeconds(20),
                             DateTime.Now,
                             new Output(true,
                                        ""));

    Assert.IsTrue(func(model));
  }

  [Test]
  public void ShouldRejectOtherMultipleTask()
  {
    var func = ExpressionsBuilders.FieldFilterExpression(model => model.TaskId,
                                                         new[]
                                                         {
                                                           "Task",
                                                           "Task2",
                                                         })
                                  .Compile();

    var model = new TaskData("OtherSession",
                             "OtherTask",
                             "OwnerPodId",
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
                             TaskStatus.Canceled,
                             "",
                             default,
                             DateTime.Now,
                             DateTime.Now + TimeSpan.FromSeconds(1),
                             DateTime.Now + TimeSpan.FromSeconds(10),
                             DateTime.Now + TimeSpan.FromSeconds(20),
                             DateTime.Now,
                             new Output(true,
                                        ""));

    Assert.IsFalse(func(model));
  }

  [Test]
  public void ShouldIncludeOtherMultipleTask()
  {
    var func = ExpressionsBuilders.FieldFilterExpression(model => model.TaskId,
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
                             TaskStatus.Canceled,
                             "",
                             default,
                             DateTime.Now,
                             DateTime.Now + TimeSpan.FromSeconds(1),
                             DateTime.Now + TimeSpan.FromSeconds(10),
                             DateTime.Now + TimeSpan.FromSeconds(20),
                             DateTime.Now,
                             new Output(true,
                                        ""));

    Assert.IsTrue(func(model));
  }
}
