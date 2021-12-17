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

using ArmoniK.Core.gRPC.V1;

using NUnit.Framework;

namespace ArmoniK.Adapters.MongoDB.Tests
{
  [TestFixture]
  internal class FieldFilterTests
  {
    [Test]
    public void ShouldRecognizeSession()
    {
      var func = MongoCollectionExt.FieldFilterExpression(model => model.SessionId,
                                                                new[] { "Session" }).Compile();

      var model = new TaskDataModel
                  { SessionId = "Session" };

      Assert.IsTrue(func(model));
    }

    [Test]
    public void ShouldRejectOtherSession()
    {
      var func = MongoCollectionExt.FieldFilterExpression(model => model.SessionId,
                                                                new[] { "Session" })
                                   .Compile();

      var model = new TaskDataModel
                  { SessionId = "OtherSession" };

      Assert.IsFalse(func(model));
    }

    [Test]
    public void ShouldExcludeSession()
    {
      var func = MongoCollectionExt.FieldFilterExpression(model => model.SessionId,
                                                                new[] { "Session" },
                                                          false)
                                   .Compile();

      var model = new TaskDataModel
                  { SessionId = "Session" };

      Assert.IsFalse(func(model));
    }

    [Test]
    public void ShouldIncludeOtherSession()
    {
      var func = MongoCollectionExt.FieldFilterExpression(model => model.SessionId,
                                                                new[] { "Session" },
                                                          false)
                                   .Compile();

      var model = new TaskDataModel
                  { SessionId = "OtherSession" };

      Assert.IsTrue(func(model));
    }

    [Test]
    public void ShouldRecognizeSubSession()
    {
      var func = MongoCollectionExt.FieldFilterExpression(model => model.SubSessionId,
                                                                new[] { "SubSession" })
                                   .Compile();

      var model = new TaskDataModel
                  { SubSessionId = "SubSession" };

      Assert.IsTrue(func(model));
    }

    [Test]
    public void ShouldExcludeSubSession()
    {
      var func = MongoCollectionExt.FieldFilterExpression(model => model.SubSessionId,
                                                                new[] { "SubSession" }, 
                                                          false)
                                   .Compile();

      var model = new TaskDataModel
                  { SubSessionId = "SubSession" };

      Assert.IsFalse(func(model));
    }

    [Test]
    public void ShouldRecognizeMultipleSubSession()
    {
      var func = MongoCollectionExt.FieldFilterExpression(model => model.SubSessionId,
                                                                new[] { "SubSession", "SubSession2" })
                                   .Compile();

      var model = new TaskDataModel
                  { SubSessionId = "SubSession" };

      Assert.IsTrue(func(model));
    }

    [Test]
    public void ShouldExcludeMultipleSubSession()
    {
      var func = MongoCollectionExt.FieldFilterExpression(model => model.SubSessionId,
                                                                new[] { "SubSession", "SubSession2" },
                                                          false)
                                   .Compile();

      var model = new TaskDataModel
                  { SubSessionId = "SubSession" };

      Assert.IsFalse(func(model));
    }

    [Test]
    public void ShouldRejectOtherSubSession()
    {
      var func = MongoCollectionExt.FieldFilterExpression(model => model.SubSessionId,
                                                                new[] { "SubSession" })
                                   .Compile();

      var model = new TaskDataModel
                  { SubSessionId = "OtherSubSession" };

      Assert.IsFalse(func(model));
    }

    [Test]
    public void ShouldIncludeOtherSubSession()
    {
      var func = MongoCollectionExt.FieldFilterExpression(model => model.SubSessionId,
                                                                new[] { "SubSession" },
                                                          false)
                                   .Compile(true);

      var model = new TaskDataModel
                  { SubSessionId = "OtherSubSession" };

      Assert.IsTrue(func(model));
    }

    [Test]
    public void ShouldRejectOtherMultipleSubSession()
    {
      var func = MongoCollectionExt.FieldFilterExpression(model => model.SubSessionId,
                                                                new[] { "SubSession", "SubSession2" })
                                   .Compile();

      var model = new TaskDataModel
                  { SubSessionId = "OtherSubSession" };

      Assert.IsFalse(func(model));
    }

    [Test]
    public void ShouldIncludeOtherMultipleSubSession()
    {
      var func = MongoCollectionExt.FieldFilterExpression(model => model.SubSessionId,
                                                                new[] { "SubSession", "SubSession2" },
                                                          false)
                                   .Compile();

      var model = new TaskDataModel
                  { SubSessionId = "OtherSubSession" };

      Assert.IsTrue(func(model));
    }

    [Test]
    public void ShouldIncludeStatus()
    {
      var func = MongoCollectionExt.FieldFilterExpression(model => model.Status,
                                                                new[] { TaskStatus.Completed })
                                   .Compile();

      var model = new TaskDataModel
                  { Status = TaskStatus.Completed };

      Assert.IsTrue(func(model));
    }


  }
}
