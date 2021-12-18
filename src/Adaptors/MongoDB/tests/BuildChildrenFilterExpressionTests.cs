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

using System;

using NUnit.Framework;

namespace ArmoniK.Adapters.MongoDB.Tests
{
  [TestFixture]
  internal class BuildChildrenFilterExpressionTests
  {

    [Test]
    public void ParentsSubSessionsFieldIsNull()
    {
      var parentsIds = new[]{ "parent1" };

      var filter = TableStorage.BuildChildrenFilterExpression(parentsIds)
                               .Compile();

      var tdm = new TaskDataModel
                {
                  TaskId = "taskId1",
                };

      Assert.IsFalse(filter(tdm));
    }

    [Test]
    public void ParentsSubSessionsFieldIsEmpty()
    {
      var parentsIds = new[]{ "parent1" };

      var filter = TableStorage.BuildChildrenFilterExpression(parentsIds)
                               .Compile();

      var tdm = new TaskDataModel
                {
                  TaskId             = "taskId1",
                  ParentsSubSessions = Array.Empty<string>(),
                };

      Assert.IsFalse(filter(tdm));
    }

    [Test]
    public void ParentsSubSessionsFieldHasWrongData()
    {
      var parentsIds = new[]{ "parent1" };

      var filter = TableStorage.BuildChildrenFilterExpression(parentsIds)
                               .Compile();

      var tdm = new TaskDataModel
                {
                  TaskId             = "taskId1",
                  ParentsSubSessions = new []{"parent0"},
                };

      Assert.IsFalse(filter(tdm));
    }

    [Test]
    public void SingleChildren()
    {
      var parentsIds = new[]{ "parent1" };

      var filter = TableStorage.BuildChildrenFilterExpression(parentsIds)
                               .Compile();

      var tdm = new TaskDataModel
                {
                  TaskId             = "taskId1",
                  ParentsSubSessions = new []{"parent1"},
                };

      Assert.IsTrue(filter(tdm));
    }

    [Test]
    public void EmptyParentList()
    {
      var parentsIds = Array.Empty<string>();

      var filter = TableStorage.BuildChildrenFilterExpression(parentsIds)
                               .Compile();

      var tdm = new TaskDataModel
                {
                  TaskId             = "taskId1",
                  ParentsSubSessions = new []{"parent0"},
                };

      Assert.IsFalse(filter(tdm));
    }
  }
}
