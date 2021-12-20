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
using System.Linq;

using ArmoniK.Core.gRPC.V1;

using MongoDB.Bson.Serialization;
using MongoDB.Bson;
using MongoDB.Driver;

using NUnit.Framework;

namespace ArmoniK.Adapters.MongoDB.Tests
{
  [TestFixture]
  internal class BuildChildrenFilterExpressionTests
  {

    [Test]
    public void ParentsSubSessionsFieldIsUndefined()
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

    [Test]
    public void NullParentList()
    {
      var filter = TableStorage.BuildChildrenFilterExpression(null)
                               .Compile();

      var tdm = new TaskDataModel
                {
                  TaskId             = "taskId1",
                  ParentsSubSessions = new []{"parent0"},
                };

      Assert.IsFalse(filter(tdm));
    }

    [Test]
    public void CompoundDataFromRealCaseTest()
    {
      var taskDataModels = new TaskDataModel[]
                           {
                             new()
                             {
                               TaskId             = "49da1bd5-578d-4abf-83a0-ae030165d365", SessionId = "8aec804b-834c-44cd-900a-ae030165cae1",
                               SubSessionId       = "908c6ead-2dba-465d-88ca-ae030165cb71", Status    = TaskStatus.Completed,
                               ParentsSubSessions = new[] { "8aec804b-834c-44cd-900a-ae030165cae1" },
                             },
                             new()
                             {
                               TaskId             = "0634a7e5-450d-46eb-83bc-ae030165d36d", SessionId = "8aec804b-834c-44cd-900a-ae030165cae1",
                               SubSessionId       = "908c6ead-2dba-465d-88ca-ae030165cb71", Status    = TaskStatus.Failed,
                               ParentsSubSessions = new[] { "8aec804b-834c-44cd-900a-ae030165cae1" },
                             },
                             new()
                             {
                               TaskId             = "97a3e77e-ad37-4754-b40a-ae030165d4c9", SessionId = "8aec804b-834c-44cd-900a-ae030165cae1",
                               SubSessionId       = "49da1bd5-578d-4abf-83a0-ae030165d365", Status    = TaskStatus.Completed,
                               ParentsSubSessions = new[] { "8aec804b-834c-44cd-900a-ae030165cae1", "908c6ead-2dba-465d-88ca-ae030165cb71" },
                             },
                             new()
                             {
                               TaskId             = "c5beb772-7274-4c62-8ca0-ae030165d4c9", SessionId = "8aec804b-834c-44cd-900a-ae030165cae1",
                               SubSessionId       = "49da1bd5-578d-4abf-83a0-ae030165d365", Status    = TaskStatus.Completed,
                               ParentsSubSessions = new[] { "8aec804b-834c-44cd-900a-ae030165cae1", "908c6ead-2dba-465d-88ca-ae030165cb71" },
                             },
                             new()
                             {
                               TaskId             = "6c3c4553-1dff-4170-ab5a-ae030165d4cf", SessionId = "8aec804b-834c-44cd-900a-ae030165cae1",
                               SubSessionId       = "49da1bd5-578d-4abf-83a0-ae030165d365", Status    = TaskStatus.Failed,
                               ParentsSubSessions = new[] { "8aec804b-834c-44cd-900a-ae030165cae1", "908c6ead-2dba-465d-88ca-ae030165cb71" },
                             },
                             new()
                             {
                               TaskId       = "8e7fe719-2abd-4a2e-bc8c-ae030165d70a", SessionId = "8aec804b-834c-44cd-900a-ae030165cae1",
                               SubSessionId = "c5beb772-7274-4c62-8ca0-ae030165d4c9", Status    = TaskStatus.Completed,
                               ParentsSubSessions = new[]
                                                    {
                                                      "8aec804b-834c-44cd-900a-ae030165cae1", "908c6ead-2dba-465d-88ca-ae030165cb71",
                                                      "49da1bd5-578d-4abf-83a0-ae030165d365",
                                                    },
                             },
                             new()
                             {
                               TaskId       = "b73901e3-eda4-42f1-bd4f-ae030165d70a", SessionId = "8aec804b-834c-44cd-900a-ae030165cae1",
                               SubSessionId = "c5beb772-7274-4c62-8ca0-ae030165d4c9", Status    = TaskStatus.Completed,
                               ParentsSubSessions = new[]
                                                    {
                                                      "8aec804b-834c-44cd-900a-ae030165cae1", "908c6ead-2dba-465d-88ca-ae030165cb71",
                                                      "49da1bd5-578d-4abf-83a0-ae030165d365",
                                                    },
                             },
                             new()
                             {
                               TaskId       = "c220a89a-d62b-424c-87dc-ae030165d70a", SessionId = "8aec804b-834c-44cd-900a-ae030165cae1",
                               SubSessionId = "c5beb772-7274-4c62-8ca0-ae030165d4c9", Status    = TaskStatus.Completed,
                               ParentsSubSessions = new[]
                                                    {
                                                      "8aec804b-834c-44cd-900a-ae030165cae1", "908c6ead-2dba-465d-88ca-ae030165cb71",
                                                      "49da1bd5-578d-4abf-83a0-ae030165d365",
                                                    },
                             },
                             new()
                             {
                               TaskId       = "c156295e-6922-49dc-a59c-ae030165d70a", SessionId = "8aec804b-834c-44cd-900a-ae030165cae1",
                               SubSessionId = "c5beb772-7274-4c62-8ca0-ae030165d4c9", Status    = TaskStatus.Completed,
                               ParentsSubSessions = new[]
                                                    {
                                                      "8aec804b-834c-44cd-900a-ae030165cae1", "908c6ead-2dba-465d-88ca-ae030165cb71",
                                                      "49da1bd5-578d-4abf-83a0-ae030165d365",
                                                    },
                             },
                             new()
                             {
                               TaskId       = "7fcb43d7-6905-44f7-b78f-ae030165d70a", SessionId = "8aec804b-834c-44cd-900a-ae030165cae1",
                               SubSessionId = "c5beb772-7274-4c62-8ca0-ae030165d4c9", Status    = TaskStatus.Completed,
                               ParentsSubSessions = new[]
                                                    {
                                                      "8aec804b-834c-44cd-900a-ae030165cae1", "908c6ead-2dba-465d-88ca-ae030165cb71",
                                                      "49da1bd5-578d-4abf-83a0-ae030165d365",
                                                    },
                             },
                             new()
                             {
                               TaskId       = "7e52b1e5-f7cc-4b1a-9553-ae030165d70f", SessionId = "8aec804b-834c-44cd-900a-ae030165cae1",
                               SubSessionId = "c5beb772-7274-4c62-8ca0-ae030165d4c9", Status    = TaskStatus.Completed,
                               ParentsSubSessions = new[]
                                                    {
                                                      "8aec804b-834c-44cd-900a-ae030165cae1", "908c6ead-2dba-465d-88ca-ae030165cb71",
                                                      "49da1bd5-578d-4abf-83a0-ae030165d365",
                                                    },
                             },
                             new()
                             {
                               TaskId       = "fca15643-0630-4864-975d-ae030165dbbd", SessionId = "8aec804b-834c-44cd-900a-ae030165cae1",
                               SubSessionId = "7fcb43d7-6905-44f7-b78f-ae030165d70a", Status    = TaskStatus.Completed,
                               ParentsSubSessions = new[]
                                                    {
                                                      "8aec804b-834c-44cd-900a-ae030165cae1", "908c6ead-2dba-465d-88ca-ae030165cb71",
                                                      "49da1bd5-578d-4abf-83a0-ae030165d365", "c5beb772-7274-4c62-8ca0-ae030165d4c9",
                                                    },
                             },
                             new()
                             {
                               TaskId       = "8891bf28-0552-4a84-8e4a-ae030165dbbd", SessionId = "8aec804b-834c-44cd-900a-ae030165cae1",
                               SubSessionId = "7fcb43d7-6905-44f7-b78f-ae030165d70a", Status    = TaskStatus.Completed,
                               ParentsSubSessions = new[]
                                                    {
                                                      "8aec804b-834c-44cd-900a-ae030165cae1", "908c6ead-2dba-465d-88ca-ae030165cb71",
                                                      "49da1bd5-578d-4abf-83a0-ae030165d365", "c5beb772-7274-4c62-8ca0-ae030165d4c9",
                                                    },
                             },
                             new()
                             {
                               TaskId       = "fb844cfa-a08c-40dd-abfa-ae030165dbbd", SessionId = "8aec804b-834c-44cd-900a-ae030165cae1",
                               SubSessionId = "7fcb43d7-6905-44f7-b78f-ae030165d70a", Status    = TaskStatus.Completed,
                               ParentsSubSessions = new[]
                                                    {
                                                      "8aec804b-834c-44cd-900a-ae030165cae1", "908c6ead-2dba-465d-88ca-ae030165cb71",
                                                      "49da1bd5-578d-4abf-83a0-ae030165d365", "c5beb772-7274-4c62-8ca0-ae030165d4c9",
                                                    },
                             },
                             new()
                             {
                               TaskId       = "3738d535-0766-4c6a-a2a4-ae030165dbbd", SessionId = "8aec804b-834c-44cd-900a-ae030165cae1",
                               SubSessionId = "7fcb43d7-6905-44f7-b78f-ae030165d70a", Status    = TaskStatus.Completed,
                               ParentsSubSessions = new[]
                                                    {
                                                      "8aec804b-834c-44cd-900a-ae030165cae1", "908c6ead-2dba-465d-88ca-ae030165cb71",
                                                      "49da1bd5-578d-4abf-83a0-ae030165d365", "c5beb772-7274-4c62-8ca0-ae030165d4c9",
                                                    },
                             },
                             new()
                             {
                               TaskId       = "c304f8d0-496f-47f2-ba20-ae030165dbbd", SessionId = "8aec804b-834c-44cd-900a-ae030165cae1",
                               SubSessionId = "7fcb43d7-6905-44f7-b78f-ae030165d70a", Status    = TaskStatus.Completed,
                               ParentsSubSessions = new[]
                                                    {
                                                      "8aec804b-834c-44cd-900a-ae030165cae1", "908c6ead-2dba-465d-88ca-ae030165cb71",
                                                      "49da1bd5-578d-4abf-83a0-ae030165d365", "c5beb772-7274-4c62-8ca0-ae030165d4c9",
                                                    },
                             },
                             new()
                             {
                               TaskId       = "5d09f129-f541-4858-98e3-ae030165dbbd", SessionId = "8aec804b-834c-44cd-900a-ae030165cae1",
                               SubSessionId = "7fcb43d7-6905-44f7-b78f-ae030165d70a", Status    = TaskStatus.Completed,
                               ParentsSubSessions = new[]
                                                    {
                                                      "8aec804b-834c-44cd-900a-ae030165cae1", "908c6ead-2dba-465d-88ca-ae030165cb71",
                                                      "49da1bd5-578d-4abf-83a0-ae030165d365", "c5beb772-7274-4c62-8ca0-ae030165d4c9",
                                                    },
                             },
                             new()
                             {
                               TaskId       = "67978406-2285-4aa6-89d6-ae030165dbbd", SessionId = "8aec804b-834c-44cd-900a-ae030165cae1",
                               SubSessionId = "7fcb43d7-6905-44f7-b78f-ae030165d70a", Status    = TaskStatus.Completed,
                               ParentsSubSessions = new[]
                                                    {
                                                      "8aec804b-834c-44cd-900a-ae030165cae1", "908c6ead-2dba-465d-88ca-ae030165cb71",
                                                      "49da1bd5-578d-4abf-83a0-ae030165d365", "c5beb772-7274-4c62-8ca0-ae030165d4c9",
                                                    },
                             },
                           };
      Console.WriteLine($"taskDataModels: {string.Join(", ", taskDataModels.Select(model => model.TaskId).OrderBy(s => s))}");

      var parentsIds = new[] { "49da1bd5-578d-4abf-83a0-ae030165d365", "49da1bd5-578d-4abf-83a0-ae030165d65" };


      var filterExpression = TableStorage.BuildChildrenFilterExpression(parentsIds);

      var definition = Builders<TaskDataModel>.Filter.Where(filterExpression);

      var documentSerializer = BsonSerializer.SerializerRegistry.GetSerializer<TaskDataModel>();

      var renderedFilter = definition.Render(documentSerializer,
                                             BsonSerializer.SerializerRegistry);

      Console.WriteLine(renderedFilter.ToString());

      var filteredTasks = taskDataModels.AsQueryable().Where(filterExpression);

      Console.WriteLine($"filteredTasks: {string.Join(", ", filteredTasks.Select(model => model.TaskId).OrderBy(s => s))}" );

      var filter = new TaskFilter
                   {
                     SessionId       = "8aec804b-834c-44cd-900a-ae030165cae1",
                     SubSessionId    = "908c6ead-2dba-465d-88ca-ae030165cb71",
                     IncludedTaskIds = {parentsIds},
                     ExcludedStatuses =
                     {
                       TaskStatus.Completed,
                     },
                   };

      var childrenTaskFilter = new TaskFilter(filter)
                               {
                                 SubSessionId = string.Empty,
                               };
      childrenTaskFilter.IncludedTaskIds.Clear();
      childrenTaskFilter.ExcludedTaskIds.Clear();

      Console.WriteLine(childrenTaskFilter.ToString());


      var remainingTasks = taskDataModels.AsQueryable().FilterQuery(childrenTaskFilter).ToList();

      Console.WriteLine($"remainingTasks: {string.Join(", ", remainingTasks.Select(model => model.TaskId).OrderBy(s => s))}");

      Assert.Greater(remainingTasks.Count, 0);
    }
  }
}
