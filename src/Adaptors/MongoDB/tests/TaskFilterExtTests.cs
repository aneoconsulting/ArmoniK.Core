// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
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

using System.Linq;

using ArmoniK.Core.gRPC.V1;

using NUnit.Framework;

namespace ArmoniK.Adapters.MongoDB.Tests
{
  [TestFixture(TestOf = typeof(TaskFilterExt))]
  internal class TaskFilterExtTests
  {
    [Test]
    public void ShouldRecognizeSession()
    {
      var func = new TaskFilter
                 {
                   SessionId = "Session",
                 }
                .ToFilterExpression()
                .Compile();

      var model = new TaskDataModel
                  {
                    SessionId = "Session",
                  };

      Assert.IsTrue(func(model));
    }

    [Test]
    public void ShouldRejectOtherSession()
    {
      var func = new TaskFilter
                 {
                   SessionId = "Session",
                 }
                .ToFilterExpression()
                .Compile();

      var model = new TaskDataModel
                  {
                    SessionId = "OtherSession",
                  };

      Assert.IsFalse(func(model));
    }

    [Test]
    public void ShouldRecognizeSubSession()
    {
      var func = new TaskFilter
                 {
                   SubSessionId = "SubSession",
                 }
                .ToFilterExpression()
                .Compile();

      var model = new TaskDataModel
                  {
                    SubSessionId = "SubSession",
                  };

      Assert.IsTrue(func(model));
    }

    [Test]
    public void ShouldRejectOtherSubSession()
    {
      var func = new TaskFilter
                 {
                   SubSessionId = "SubSession",
                 }
                .ToFilterExpression()
                .Compile();

      var model = new TaskDataModel
                  {
                    SubSessionId = "OtherSubSession",
                  };

      Assert.IsFalse(func(model));
    }

    [Test]
    public void ShouldRecognizeStatus()
    {
      var func = new TaskFilter
                 {
                   IncludedStatuses = { TaskStatus.Completed },
                 }
                .ToFilterExpression()
                .Compile(true);

      var model = new TaskDataModel
                  {
                    Status = TaskStatus.Completed,
                  };

      Assert.IsTrue(func(model));
    }

    [Test]
    public void ShouldExcludeStatus()
    {
      var func = new TaskFilter
                 {
                   ExcludedStatuses = { TaskStatus.Completed },
                 }
                .ToFilterExpression()
                .Compile();

      var model = new TaskDataModel
                  {
                    Status = TaskStatus.Completed,
                  };

      Assert.IsFalse(func(model));
    }

    [Test]
    public void ShouldRecognizeMultipleStatus()
    {
      var func = new TaskFilter
                 {
                   IncludedStatuses =
                   {
                     TaskStatus.Completed,
                     TaskStatus.Canceled,
                   },
                 }
                .ToFilterExpression()
                .Compile();

      var model = new TaskDataModel
                  {
                    Status = TaskStatus.Completed,
                  };

      Assert.IsTrue(func(model));
    }

    [Test]
    public void ShouldExcludeMultipleStatus()
    {
      var func = new TaskFilter
                 {
                   ExcludedStatuses =
                   {
                     TaskStatus.Completed,
                     TaskStatus.Canceled,
                   },
                 }
                .ToFilterExpression()
                .Compile();

      var model = new TaskDataModel
                  {
                    Status = TaskStatus.Completed,
                  };

      Assert.IsFalse(func(model));
    }

    [Test]
    public void ShouldRejectOtherStatus()
    {
      var func = new TaskFilter
                 {
                   IncludedStatuses = { TaskStatus.Completed },
                 }
                .ToFilterExpression()
                .Compile();

      var model = new TaskDataModel
                  {
                    Status = TaskStatus.Canceled,
                  };

      Assert.IsFalse(func(model));
    }

    [Test]
    public void ShouldIncludeOtherStatus()
    {
      var func = new TaskFilter
                 {
                   ExcludedStatuses = { TaskStatus.Completed },
                 }
                .ToFilterExpression()
                .Compile();

      var model = new TaskDataModel
                  {
                    Status = TaskStatus.Canceled,
                  };

      Assert.IsTrue(func(model));
    }

    [Test]
    public void ShouldRejectOtherMultipleStatus()
    {
      var func = new TaskFilter
                 {
                   IncludedStatuses =
                   {
                     TaskStatus.Completed,
                     TaskStatus.Canceling,
                   },
                 }
                .ToFilterExpression()
                .Compile();

      var model = new TaskDataModel
                  {
                    Status = TaskStatus.Canceled,
                  };

      Assert.IsFalse(func(model));
    }

    [Test]
    public void ShouldIncludeOtherMultipleStatus()
    {
      var func = new TaskFilter
                 {
                   ExcludedStatuses =
                   {
                     TaskStatus.Completed,
                     TaskStatus.Canceling,
                   },
                 }
                .ToFilterExpression()
                .Compile();

      var model = new TaskDataModel
                  {
                    Status = TaskStatus.Canceled,
                  };

      Assert.IsTrue(func(model));
    }

    [Test]
    public void ShouldRecognizeTask()
    {
      var func = new TaskFilter
                 {
                   IncludedTaskIds =
                   {
                     "Task",
                   },
                 }
                .ToFilterExpression()
                .Compile();

      var model = new TaskDataModel
                  {
                    TaskId = "Task",
                  };

      Assert.IsTrue(func(model));
    }

    [Test]
    public void ShouldExcludeTask()
    {
      var func = new TaskFilter
                 {
                   ExcludedTaskIds =
                   {
                     "Task",
                   },
                 }
                .ToFilterExpression()
                .Compile();

      var model = new TaskDataModel
                  {
                    TaskId = "Task",
                  };

      Assert.IsFalse(func(model));
    }

    [Test]
    public void ShouldRecognizeMultipleTask()
    {
      var func = new TaskFilter
                 {
                   IncludedTaskIds =
                   {
                     "Task",
                     "Task2",
                   },
                 }
                .ToFilterExpression()
                .Compile();

      var model = new TaskDataModel
                  {
                    TaskId = "Task",
                  };

      Assert.IsTrue(func(model));
    }

    [Test]
    public void ShouldExcludeMultipleTask()
    {
      var func = new TaskFilter
                 {
                   ExcludedTaskIds =
                   {
                     "Task",
                     "Task2",
                   },
                 }
                .ToFilterExpression()
                .Compile();

      var model = new TaskDataModel
                  {
                    TaskId = "Task",
                  };

      Assert.IsFalse(func(model));
    }

    [Test]
    public void ShouldRejectOtherTask()
    {
      var func = new TaskFilter
                 {
                   IncludedTaskIds =
                   {
                     "Task",
                   },
                 }
                .ToFilterExpression()
                .Compile();

      var model = new TaskDataModel
                  {
                    TaskId = "OtherTask",
                  };

      Assert.IsFalse(func(model));
    }

    [Test]
    public void ShouldIncludeOtherTask()
    {
      var func = new TaskFilter
                 {
                   ExcludedTaskIds =
                   {
                     "Task",
                   },
                 }
                .ToFilterExpression()
                .Compile();

      var model = new TaskDataModel
                  {
                    TaskId = "OtherTask",
                  };

      Assert.IsTrue(func(model));
    }

    [Test]
    public void ShouldRejectOtherMultipleTask()
    {
      var func = new TaskFilter
                 {
                   IncludedTaskIds =
                   {
                     "Task",
                     "Task2",
                   },
                 }
                .ToFilterExpression()
                .Compile();

      var model = new TaskDataModel
                  {
                    TaskId = "OtherTask",
                  };

      Assert.IsFalse(func(model));
    }

    [Test]
    public void ShouldIncludeOtherMultipleTask()
    {
      var func = new TaskFilter
                 {
                   ExcludedTaskIds =
                   {
                     "Task",
                     "Task2",
                   },
                 }
                .ToFilterExpression()
                .Compile();

      var model = new TaskDataModel
                  {
                    TaskId = "OtherTask",
                  };

      Assert.IsTrue(func(model));
    }

    [Test]
    public void AllNullShouldPass()
    {
      var func = new TaskFilter
                 {
                   SessionId = null,
                   SubSessionId = null,
                 }
                .ToFilterExpression()
                .Compile();

      var model = new TaskDataModel
                  {
                    SessionId = null,
                    SubSessionId = null,
                    Dependencies = null,
                    Options = null,
                    ParentsSubSessions = null,
                    Payload = null,
                    TaskId = null,
                  };

      Assert.IsTrue(func(model));
    }

  }
}
